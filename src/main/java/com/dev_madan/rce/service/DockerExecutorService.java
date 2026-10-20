package com.dev_madan.rce.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
// import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.dev_madan.rce.controller.ExecutionController;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class DockerExecutorService {
    @Value("${app.host-temp-path:}")
    private String hostTempPath;

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final SessionRegistry sessionReg;

    private final KafkaTemplate<String, String> kafkaTemplate;
    // private final Semaphore containerSlots;

    public DockerExecutorService(RedisTemplate<String, String> redisTemplate,
            ObjectMapper objectMapper, SessionRegistry sessionReg, ExecutionController exeController,
            KafkaTemplate<String, String> kafkaTemplate) {

        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.sessionReg = sessionReg;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Process execute(String lang, String code, String inputString, String sessionId, String runId) {
        return switch (lang) {
            case "python" -> executePython(code, inputString, sessionId, runId);
            case "java" -> executeJava(code, inputString, sessionId, runId);
            // case "cpp" -> executeCpp(code, sessionId);
            default -> {
                sendError(sessionId, runId, "Unsupported language: " + lang);
                yield null;
            }
        };
    }

    private Process executePython(String code, String inputString, String sessionId, String runId) {
        String jobId = UUID.randomUUID().toString();
        String filename = jobId + ".py";
        Path containerTempDir = Path.of(hostTempPath.isBlank() ? "/app/temp" : hostTempPath);
        Path filepath = containerTempDir.resolve(filename);
        String hostTempDir = hostTempPath.isBlank() ? containerTempDir.toString().replace("\\", "/")
                : hostTempPath.replace("\\", "/");

        try {
            Files.createDirectories(containerTempDir);
            Files.writeString(filepath, code);
        } catch (IOException e) {
            sendError(
                    sessionId,
                    runId,
                    "Failed to write code on file: " + filepath + " - " + e.getMessage());
            return null;
        }

        // we never close the input stream
        List<String> cmd = List.of("docker", "run", "--rm",
                "-i", "--memory=128m", "--cpus=0.25", "--network", "none",
                "-v", String.format("%s:/code", hostTempDir),
                "python-runner", "python", "-u", String.format("/code/%s", filename));

        return startProcess(cmd, inputString, sessionId, () -> {
            try {
                Files.deleteIfExists(filepath);
            } catch (IOException ignored) {
            }
        }, runId);
    }

    private Process executeJava(String code, String inputString, String sessionId, String runId) {
        String jobId = UUID.randomUUID().toString();
        String filename = "Main.java";
        Path containerTempDir = Path.of(hostTempPath.isBlank() ? "/app/temp" : hostTempPath, jobId);
        Path filePath = containerTempDir.resolve(filename);

        System.out.println("containerTempDir: " + containerTempDir);
        System.out.println("filePath: " + filePath);

        try {
            Files.createDirectories(containerTempDir);
            Files.writeString(filePath, code);
        } catch (IOException e) {
            sendError(
                    sessionId,
                    runId,
                    "Failed to write code on file: " + filePath + " - " + e.getMessage());
            return null;
        }

        List<String> cmd = List.of("docker", "run", "--rm",
                "-i", "--memory=128m", "--cpus=0.25", "--network", "none",
                "-v", String.format("%s:/code", containerTempDir.toString()), "java-runner", "sh", "-c",
                String.format("javac -d /tmp/java-out /code/%s && java -cp /tmp/java-out Main", filename));

        return startProcess(cmd, inputString, sessionId, () -> {
            deleteDir(containerTempDir.toString());
        }, runId);
    }

    private Process startProcess(List<String> cmd, String inputString, String sessionId, Runnable cleanup,
            String runId) {

        Process child;
        try {
            child = new ProcessBuilder(cmd).redirectErrorStream(false).start();

            // register immediately after spawning — before threads start
            sessionReg.register(sessionId, child);
            send(sessionId, runId, "status", "now running...");
            // This way the process is registered the moment Docker spawns — not after
            // execution finishes.
            // If disconnect fires during execution, killIfExists finds the process and
            // destroys it.

            if (inputString != null && !inputString.isBlank()) {
                String normalized = inputString.endsWith("\n") ? inputString : inputString + "\n";
                child.getOutputStream().write(normalized.getBytes());
                child.getOutputStream().flush();
                child.getOutputStream().close();
            }
        } catch (IOException e) {
            sendError(sessionId, runId, "Failed to start Docker: " + e.getMessage());
            cleanup.run();
            return null;
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        AtomicLong cpuTime = new AtomicLong(0);
        AtomicLong cpuStart = new AtomicLong(System.currentTimeMillis());

        ScheduledFuture<?> checkCpuTime = scheduler.scheduleAtFixedRate(() -> {
            System.out.println("cpuTime: " + cpuTime);
            cpuTime.addAndGet(System.currentTimeMillis() - cpuStart.get());
            cpuStart.set(System.currentTimeMillis());
            if (cpuTime.get() >= 10000) {
                child.destroyForcibly();
                sendError(sessionId, runId, "CPU Time Limit Exceeded");
            }
        }, 500, 500, TimeUnit.MILLISECONDS);

        Thread stdoutThread = new Thread(() -> {
            try (InputStream stdout = child.getInputStream()) {
                byte[] buf = new byte[1024];
                int n;

                while ((n = stdout.read(buf)) != -1) {
                    String chunk = new String(buf, 0, n);
                    send(sessionId, runId, "stdout", chunk);
                }
            } catch (IOException ignored) {
            }
        });

        Thread stderrThread = new Thread(() -> {
            try (InputStream stderr = child.getErrorStream()) {
                byte[] buf = new byte[1024];
                int n;

                while ((n = stderr.read(buf)) != -1) {
                    String chunk = new String(buf, 0, n);
                    send(sessionId, runId, "stderr", chunk);
                }
            } catch (IOException ignored) {
            }
        });

        stdoutThread.start();
        stderrThread.start();

        try {
            int exitCode = child.waitFor(); // blocks until exit

            stdoutThread.join();
            stderrThread.join();

            Map<String, Object> exitPayload = Map.of("type", "exit", "code", exitCode, "cpuTime", cpuTime.get());

            String exitJson = objectMapper.writeValueAsString(exitPayload); // serialize to JSON string

            // redisTemplate.convertAndSend("output:" + sessionId, exitJson);
            ProducerRecord<String, String> record = new ProducerRecord<String,String>("execution-output", sessionId, exitJson);
            record.headers().add("runId", runId.getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("Error while completion of Docker container: " + e.getMessage());
        } finally {
            checkCpuTime.cancel(false);
            scheduler.shutdownNow();
            cleanup.run();
            sessionReg.removeProcess(sessionId);
        }
        return child;
    }

    private void send(String sessionId, String runId, String type, String data) {
        try {
            String payload = objectMapper.writeValueAsString(Map.of("type", type, "data", data));

            ProducerRecord<String, String> record = new ProducerRecord<>("execution-output", sessionId, payload);
            record.headers().add("runId", runId.getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(record);

        } catch (Exception e) {
            System.err.println("Failed to publish output: " + e.getMessage());
        }
    }

    private void sendError(String sessionId, String runId, String message) {
        send(sessionId, runId, "stderr", message);
    }

    private void deleteDir(String dirPath) {
        Path dir = Path.of(dirPath);
        try {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder()) // delete children first
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            System.out.println("Failed to delete: " + path + ": " + e.getMessage());
                        }
                    });
        } catch (IOException ignored) {
        }
    }
}
