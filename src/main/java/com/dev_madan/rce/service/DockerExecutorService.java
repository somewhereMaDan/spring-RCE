package com.dev_madan.rce.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Sinks.One;

@Service
public class DockerExecutorService {
    @Value("${app.host-temp-path:}")
    private String hostTempPath;

    private final StringRedisTemplate redis;

    private static final int MAX_OUTPUT = 64 * 1024;
    private static final long TIME_LIMIT_MS = 10_000;

    public DockerExecutorService(StringRedisTemplate redisTemplate) {
        this.redis = redisTemplate;
    }

    public void execute(String lang, String code, String inputString, String jobId) {
        switch (lang) {
            case "python" -> executePython(code, inputString, jobId);
            case "java" -> executeJava(code, inputString, jobId);
            default -> throw new IllegalArgumentException(
                    "Unsupported language: " + lang);
        }
    }

    private void executePython(String code, String inputString, String jobId) {
        Path dir = Path.of(hostTempPath.isBlank() ? "/app/temp" : hostTempPath, jobId);
        try {
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("main.py"), code);
        } catch (IOException e) {
            finish(jobId, "ERROR", "INTERNAL", "Failed to write code: " + e.getMessage());
            return;
        }

        List<String> cmd = List.of("docker", "run", "--rm", "--name", "rce-" + jobId,
                "-i", "--memory=128m", "--cpus=0.25", "--network", "none",
                "-v", dir.toString().replace("\\", "/") + ":/code:ro",
                "python-runner", "python", "-u", "/code/main.py");

        startProcess(cmd, inputString, jobId, () -> deleteDir(dir.toString()));
    }

    private void executeJava(String code, String inputString, String jobId) {
        String runId_for_file = UUID.randomUUID().toString();
        String filename = "Main.java";
        Path containerTempDir = Path.of(hostTempPath.isBlank() ? "/app/temp" : hostTempPath, runId_for_file);
        Path filePath = containerTempDir.resolve(filename);

        System.out.println("containerTempDir: " + containerTempDir);
        System.out.println("filePath: " + filePath);

        try {
            Files.createDirectories(containerTempDir);
            Files.writeString(filePath, code);
        } catch (IOException e) {
            // sendError(
            // jobId,
            // "Failed to write code on file: " + filePath + " - " + e.getMessage());
            // return null;
        }

        List<String> cmd = List.of("docker", "run", "--rm",
                "-i", "--memory=128m", "--cpus=0.25", "--network", "none",
                "-v", String.format("%s:/code", containerTempDir.toString()), "java-runner", "sh", "-c",
                String.format("javac -d /tmp/java-out /code/%s && java -cp /tmp/java-out Main", filename));

        startProcess(cmd, inputString, jobId, () -> {
            deleteDir(containerTempDir.toString());
        });
    }

    private void startProcess(List<String> cmd, String inputString, String jobId, Runnable cleanup) {
        String key = "run:" + jobId;
        long start = System.currentTimeMillis();

        AtomicBoolean timedOut = new AtomicBoolean(false);
        AtomicBoolean cancelled = new AtomicBoolean(false);

        ScheduledExecutorService schedular = Executors.newSingleThreadScheduledExecutor(); // schedular is started in separate thread and all stdout, stderr threads use 
        // this schedular shared thread, this stdout, stderr does not have to maintain seprate thread everytime. 

        // One subtle trade-off A single scheduler thread executes watchdog checks sequentially. 
        // If one check blocks—for example, killContainer(jobId) takes a long time—other jobs' checks may be delayed.

        Process child = null;

        try {
            child = new ProcessBuilder(cmd).start();

            final Process p = child;
            AtomicLong killedAt = new AtomicLong(0);

            redis.opsForHash().putAll(key, Map.of("status", "RUNNING", "startedAt", String.valueOf(start)));

            schedular.scheduleAtFixedRate(() -> {
                try {
                    if (timedOut.get() || cancelled.get()) {
                        // if the docker run CLI process is still alive after killing container, we'll destroyForcibly()
                        if (p.isAlive() && System.currentTimeMillis() - killedAt.get() > 2000) {
                            p.destroyForcibly(); // last resort: kills the CLI, unblocks waitFor
                            return;
                        }
                    }
                    if (System.currentTimeMillis() - start >= TIME_LIMIT_MS) {
                        timedOut.set(true);
                        killedAt.set(System.currentTimeMillis());
                        killContainer(jobId);
                    } else if (redis.opsForHash().hasKey(key, "cancelRequested")) {
                        cancelled.set(true);
                        killedAt.set(System.currentTimeMillis());
                        killContainer(jobId);
                    }
                } catch (Exception ignored) {
                    // TODO: handle exception
                }
            }, 500, 500, TimeUnit.MILLISECONDS);

            // we will discard the bytes after the limit, that covers the edge case ->
            // output consuming too much memory (miilion lines)
            CappedBuffer out = new CappedBuffer(MAX_OUTPUT);
            CappedBuffer err = new CappedBuffer(MAX_OUTPUT);

            Thread tOut = drain(child.getInputStream(), out);
            Thread tErr = drain(child.getErrorStream(), err);

            writeInput(child, inputString);

            int exitCode = child.waitFor();
            tOut.join();
            tErr.join();

            Map<String, String> res = new HashMap<>();
            res.put("stdout", out.text());
            res.put("stderr", err.text());
            res.put("truncated", String.valueOf(out.truncated() || err.truncated()));
            res.put("exitCode", String.valueOf(exitCode));
            res.put("wallMs", String.valueOf(System.currentTimeMillis() - start));
            res.put("finishedAt", String.valueOf(System.currentTimeMillis()));

            if (cancelled.get()) {
                res.put("status", "CANCELLED");
            } else {
                res.put("status", "DONE");
                res.put("verdict", timedOut.get() ? "TLE" : exitCode == 0 ? "OK" : exitCode == 137 ? "MLE" : "RE");
            }
            redis.opsForHash().putAll(key, res);
            redis.expire(key, Duration.ofHours(1));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finish(jobId, "ERROR", "INTERNAL", "Interrupted");
        } catch (Exception e) {
            finish(jobId, "ERROR", "INTERNAL", String.valueOf(e.getMessage()));
        } finally {
            schedular.shutdownNow();
            if (child != null && child.isAlive())
                killContainer(jobId);
            cleanup.run();
        }
    }

    private void finish(String jobId, String status, String verdict, String stderr) {
        redis.opsForHash().putAll("run:" + jobId, Map.of(
                "status", status, "verdict", verdict, "stderr", stderr,
                "finishedAt", String.valueOf(System.currentTimeMillis())));
    }

    private void killContainer(String jobId) {
        try {
            new ProcessBuilder("docker", "rm", "-f", "rce-" + jobId)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectErrorStream(true).start().waitFor(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
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

    // writing the input bytes to the outputStream of the process
    private void writeInput(Process p, String input) {
        new Thread(() -> {
            try (OutputStream os = p.getOutputStream()) {
                if (input != null && !input.isBlank()) {
                    String s = input.endsWith("\n") ? input : input + "\n";
                    os.write(s.getBytes(StandardCharsets.UTF_8));
                }
            } catch (IOException ignored) { // the program may exit without reading all input
            }
        }).start();
    }

    // for reading the stream and storing withing the capped buffer limit
    private Thread drain(InputStream in, CappedBuffer buf) {
        Thread t = new Thread(() -> {
            try (in) {
                byte[] b = new byte[4096];
                int n;
                while ((n = in.read(b)) != -1) // read available bytes into the array 'b', until the stream reaches it's
                                               // end
                    buf.write(b, n);
            } catch (IOException ignored) {
            }
        });
        t.start();
        return t;
    }

    // enforcing limit
    static class CappedBuffer {
        private final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        private final int max;
        private boolean truncated;

        CappedBuffer(int max) {
            this.max = max;
        }

        synchronized void write(byte[] b, int n) {
            int room = max - bos.size();
            if (room > 0)
                bos.write(b, 0, Math.min(room, n));
            if (n > room)
                truncated = true;
        }

        synchronized String text() {
            return bos.toString(StandardCharsets.UTF_8);
        }

        synchronized boolean truncated() {
            return truncated;
        }
    }
}
