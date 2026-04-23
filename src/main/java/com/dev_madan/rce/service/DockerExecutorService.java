package com.dev_madan.rce.service;

import com.dev_madan.rce.dto.ExecutionHandle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class DockerExecutorService {
    @Value("${app.host-temp-path:}")
    private String hostTempPath;

    private final SimpMessagingTemplate messagingTemplate;

    public DockerExecutorService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    private ExecutionHandle executePython(String code, String sessionId) {
        String jobId = UUID.randomUUID().toString();
        String filename = jobId + ".py";
        Path containerTempDir = Path.of(hostTempPath.isBlank() ? "/app/temp" : hostTempPath);
        Path filepath = containerTempDir.resolve(filename);
        String hostTempDir = hostTempPath.isBlank() ? containerTempDir.toString().replace("\\", "/") : hostTempPath.replace("\\", "/");

        try {
            Files.createDirectories(containerTempDir);
            Files.writeString(filepath, code);
        } catch (IOException e) {
            sendError(sessionId, "Failed to write code on file: " + e.getMessage());
            return null;
        }

        List<String> cmd = List.of(
                "docker", "run", "--rm", "-i", "--memory=256m",
                "--cpus=0.5", "--network", "none",
                "-v", String.format("%s:/code", hostTempDir),
                "python-runner", "python", "-u", String.format("/code/%s", filename)
        );

        return startProcess(cmd, sessionId, () -> {
            try {
                Files.deleteIfExists(filepath);
            } catch (IOException ignored) {
            }
        });
    }

    public ExecutionHandle execute(String lang, String code, String sessionId) {
        return switch (lang) {
            case "python" -> executePython(code, sessionId);
//            case "java" -> executeJava(code, sessionId);
//            case "cpp" -> executeCpp(code, sessionId);
            default -> {
                sendError(sessionId, "Unsupported language: " + lang);
                yield null;
            }
        };
    }

    private ExecutionHandle startProcess(List<String> cmd, String sessionId, Runnable cleanup) {
        Process child;
        try {
            child = new ProcessBuilder(cmd)
                    .redirectErrorStream(false) // redirectErrorStream(false) — keeps stdout and stderr as separate streams. Same as Node where child.stdout and child.stderr were separate
                    .start();
        } catch (IOException e) {
            // equivalant of child.on('error') in Node
            sendError(sessionId, "Failed to start Docker: " + e.getMessage());
            cleanup.run();
            return null;
        }

        // ── Timer system ──────────────────────────────────────────────────────
        // In Node you used setTimeout/setInterval in a closure.
        // In Java, we use ScheduledExecutorService — same concept, different API.
        // Think of it as:
        //   setTimeout  → scheduler.schedule(task, delay, TimeUnit)
        //   setInterval → scheduler.scheduleAtFixedRate(task, 0, interval, TimeUnit)
        //   clearTimeout/clearInterval → future.cancel(true)

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        AtomicLong cpuTime = new AtomicLong(0);
        AtomicLong cpuStart = new AtomicLong(System.currentTimeMillis());
        AtomicBoolean waitingForInput = new AtomicBoolean(false);

        AtomicReference<ScheduledFuture<?>> inputTimeRef = new AtomicReference<>();

//        AtomicLong uses CPU-level instructions that make read-modify-write happen as one uninterruptible operation. No thread can read in the middle of another thread's update:
//        cpuTime = 100
//
//        Thread 1 calls addAndGet(50) → CPU locks, reads 100, adds 50, writes 150, unlocks
//        Thread 2 calls addAndGet(30) → CPU locks, reads 150, adds 30, writes 180, unlocks
//        correct answer 180 ✓

        Runnable resetInputTimer = () -> {
            ScheduledFuture<?> existing = inputTimeRef.get();
            if (existing != null) existing.cancel(false);

            ScheduledFuture<?> newTimer = scheduler.schedule(() -> {
                child.destroyForcibly();
                sendError(sessionId, "Input timeout, no input was received for 30s");
            }, 30, TimeUnit.SECONDS);

            inputTimeRef.set(newTimer); // setting a timeout for user input of limit 30s
        };

        ScheduledFuture<?> checkCpuTime = scheduler.scheduleAtFixedRate(() ->{
            System.out.println("cpuTime: " + cpuTime);
            if(!waitingForInput.get()){
                cpuTime.addAndGet(System.currentTimeMillis() - cpuStart.get());
                cpuStart.set(System.currentTimeMillis());
            }
            if(cpuTime.get() >= 10000){
                child.destroyForcibly();
                sendError(sessionId, "CPU Time Limit Exceeded");
            }
        }, 500, 500, TimeUnit.MILLISECONDS);

        resetInputTimer.run();

        Thread stdoutThread = new Thread(() -> {
            try(InputStream stdout = child.getInputStream()){
                byte[] buf = new byte[1024]; // it can store the initial 1024 bytes from the input stream (os)
                int n;
                // if the stdout might be having 2000 bytes waiting to read
                // stdout.read(buf) reads bytes from the stream, write those bytes in buf and return how many bytes were written

                // for ex let's say stream passed - HelloWorld   ASCII bytes: [72 101 108 108 111 87 111 114 108 100] written in buf
                // buf = [72 101 108 108 111 87 111 114 108 100 ...] and return how many bytes were written

                while((n = stdout.read(buf)) != -1){ // also if the stream was 2000 bytes and in buf only 1024 bytes can be written it would first pass 1024 bytes and then the remaining, we read till the stream is empty
                    String chunk = new String(buf, 0, n);
                    // later we use the n to covert the buf = [...] into string, because buf only has 10 bytes that were written except that is garbage.

                    waitingForInput.set(false);
                    cpuTime.addAndGet(System.currentTimeMillis() - cpuStart.get());
                    cpuStart.set(System.currentTimeMillis());
                    resetInputTimer.run();

                    send(sessionId, "stdout", chunk);
                }
            }catch (IOException ignored){}
        });

        Thread stderrThread = new Thread(() -> {
            try(InputStream stderr = child.getErrorStream()){
                byte[] buf = new byte[1024];
                int n;

                while((n = stderr.read(buf)) != -1){
                    String chunk = new String(buf, 0, n);
                    send(sessionId, "stderr", chunk);
                }
            }catch (IOException ignored){}
        });

        stdoutThread.start();
        stderrThread.start();

        Thread waitThread = new Thread(() ->{
            try{
                int exitCode = child.waitFor(); // blocks until exit

                stdoutThread.join();
                stderrThread.join();

                ScheduledFuture<?> inputTimer = inputTimeRef.get();
                if(inputTimer != null) inputTimer.cancel(false);
                checkCpuTime.cancel(false);
                scheduler.shutdown();

                Map<String, Object> exitPayload = Map.of(
                        "type" , "exit",
                        "code" , exitCode,
                        "cpuTime", cpuTime.get()
                );

                messagingTemplate.convertAndSend("/topic/output/" + sessionId, (Object) exitPayload);

                cleanup.run();

            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        });

        waitThread.start();
        return new ExecutionHandle(child, waitingForInput, resetInputTimer);
    }
    private void send(String sessionId, String type, String data){
        System.out.println("Sending message to: /topic/output/" + sessionId);
        messagingTemplate.convertAndSend("/topic/output/" + sessionId, (Object) Map.of("type", type, "data", data));
    }

    private void sendError(String sessionId, String message){
        send(sessionId, "stderr", message);
    }
}
