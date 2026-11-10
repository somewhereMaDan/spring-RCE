package com.dev_madan.rce.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.dev_madan.rce.pool.WarmPool;

@Service
public class DockerExecutorService {
    @Value("${app.host-temp-path:}")
    private String hostTempPath;

    private final StringRedisTemplate redis;

    private static final int MAX_OUTPUT = 64 * 1024;
    private static final long TIME_LIMIT_MS = 10_000;

    private final WarmPool warmPool; // null outside the worker profile

    @Value("${app.warm-pool:true}")
    private boolean poolEnabled;

    public DockerExecutorService(StringRedisTemplate redisTemplate, ObjectProvider<WarmPool> pool) {
        this.redis = redisTemplate;
        this.warmPool = pool.getIfAvailable();
    }

    public void execute(String lang, String code, String inputString, String jobId, int slot) {
        switch (lang) {
            case "python" -> executePython(code, inputString, jobId);
            case "java" -> executeJava(code, inputString, jobId);
            case "cpp" -> {
                if (warmPool != null && poolEnabled) {
                    System.out.println("new warm containerssss");
                    executeCppWarm(code, inputString, jobId, slot);
                } else {
                    System.out.println("olderrr");
                    executeCpp(code, inputString, jobId);
                }
            }

            default -> throw new IllegalArgumentException(
                    "Unsupported language: " + lang);
        }
    }

    private static final List<String> SANDBOX = List.of(
            "--memory=256m", "--memory-swap=256m", "--cpus=0.5", "--network", "none",
            "--pids-limit=128",
            "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=32m",
            "--tmpfs", "/run-code:rw,exec,nosuid,nodev,size=32m,mode=700,uid=1000,gid=1000",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges");

    private List<String> dockerCmd(String jobId, Path dir, List<String> tail) {
        List<String> cmd = new ArrayList<>(
                List.of("docker", "run", "--rm", "--name", "rce-" + jobId, "--stop-timeout", "13", "-i"));
        cmd.addAll(SANDBOX);
        cmd.addAll(List.of("-v", dir.toString().replace("\\", "/") + ":/code:ro"));
        cmd.addAll(tail);
        return cmd;
    }

    private void executeCppWarm(String code, String inputString, String jobId, int slot) {
        Path src = warmPool.workerDir(slot).resolve("main.cpp");
        try {
            Files.writeString(src, code);
        } catch (IOException e) {
            finish(jobId, "ERROR", "INTERNAL", "Failed to write code: " + e.getMessage());
            return;
        }
        String container = warmPool.containerName(slot);
        List<String> cmd = List.of("docker", "exec", "-i", container, "sh", "-c",
                "find /run-code /tmp -mindepth 1 -delete 2>/dev/null; "
                        + "g++ -std=c++17 -O2 -pipe /code/main.cpp -o /run-code/main "
                        + "&& exec timeout -s KILL 12s /run-code/main");

        // Delete only the file, never the directory: the directory is bind-mounted into
        // the
        // container, and recreating it would leave the container pointing at a deleted
        // inode.
        startProcess(cmd, inputString, jobId, container, slot, () -> {
            try {
                Files.deleteIfExists(src);
            } catch (IOException ignored) {
            }
        });
    }

    private void executePython(String code, String inputString, String jobId) {

        // Path visible to the worker container
        Path workerDir = Path.of("/app/temp", jobId);

        // Path visible to the host Docker daemon
        Path hostDir = Path.of(hostTempPath, jobId);

        try {
            Files.createDirectories(workerDir);
            Files.writeString(workerDir.resolve("main.py"), code);
        } catch (IOException e) {
            finish(jobId, "ERROR", "INTERNAL", "Failed to write code: " + e.getMessage());
            return;
        }

        // List<String> cmd = dockerCmd(jobId, hostDir, List.of("python-runner",
        // "python", "-u", "/code/main.py"));

        // Docker starts sh as PID 1, the python worker gets SIGKILL unexpectedly (maybe
        // restart of instance)
        // container keeps running independently, after 12s timeout sends SIGKILL to py
        // program
        // timeout exits then shell exits
        // Docker detects PID 1 has exited and stops/removes the container (--rm).

        List<String> cmd = dockerCmd(jobId, hostDir, List.of(
                "python-runner", "sh", "-c",
                "exec timeout -s KILL 12s python -u /code/main.py"));

        startProcess(cmd, inputString, jobId, "rce-" + jobId, -1, () -> deleteDir(workerDir.toString()));
    }

    private void executeCpp(String code, String inputString, String jobId) {
        // Path visible to the worker container
        Path workerDir = Path.of("/app/temp", jobId);

        // Path visible to the host Docker daemon
        Path hostDir = Path.of(hostTempPath, jobId);

        try {
            Files.createDirectories(workerDir);
            Files.writeString(workerDir.resolve("main.cpp"), code);

        } catch (IOException e) {
            finish(jobId, "ERROR", "INTERNAL", "Failed to write code: " + e.getMessage());
            return;
        }

        List<String> cmd = dockerCmd(jobId, hostDir, List.of("cpp-runner", "sh", "-c",
                "g++ -std=c++17 -O2 -pipe /code/main.cpp -o /run-code/main && exec timeout -s KILL 12s /run-code/main"));

        startProcess(cmd, inputString, jobId, "rce-" + jobId, -1, () -> deleteDir(workerDir.toString()));
    }

    // Suppose the job ID is 123.
    // 1. Java creates /app/temp/123/main.cpp.
    // 2. Because of the Compose volume mount, the same file exists on your host at
    // /home/madan/Desktop/work/spring-RCE/temp/123/main.cpp.
    // 3. Java asks Docker to mount /home/madan/Desktop/work/spring-RCE/temp/123
    // into /code.
    // 4. The runner sees /code/main.cpp.
    // 5. Compilation and execution happen inside the isolated runner.
    // 6. The worker collects stdout, stderr and exit code.
    // 7. The worker deletes /app/temp/123, which also removes the host-side files.

    private void executeJava(String code, String inputString, String jobId) {

        // Path visible to the worker container
        Path workerDir = Path.of("/app/temp", jobId);

        // Path visible to the host Docker daemon
        Path hostDir = Path.of(hostTempPath, jobId);

        try {
            Files.createDirectories(workerDir);
            Files.writeString(workerDir.resolve("Main.java"), code);

        } catch (IOException e) {
            finish(jobId, "ERROR", "INTERNAL", "Failed to write code: " + e.getMessage());
            return;
        }

        // List<String> cmd = dockerCmd(jobId, hostDir, List.of("java-runner", "sh",
        // "-c",
        // "javac -d /tmp/java-out /code/Main.java && java -cp /tmp/java-out Main"));

        List<String> cmd = dockerCmd(jobId, hostDir, List.of(
                "java-runner", "sh", "-c",
                "javac -d /tmp/java-out /code/Main.java && exec timeout -s KILL 12s java -cp /tmp/java-out Main"));

        startProcess(cmd, inputString, jobId, "rce-" + jobId, -1, () -> {
            deleteDir(workerDir.toString());
        });
    }

    private void startProcess(List<String> cmd, String inputString, String jobId,
            String killTarget, int slot, Runnable cleanup) {
        String key = "run:" + jobId;
        long start = System.currentTimeMillis();

        AtomicBoolean timedOut = new AtomicBoolean(false);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        boolean clean = false;

        ScheduledExecutorService schedular = Executors.newSingleThreadScheduledExecutor();
        // schedular is started in separate thread and all stdout, stderr threads use
        // this schedular shared thread, this stdout, stderr does not have to maintain
        // seprate thread everytime.

        // One subtle trade-off A single scheduler thread executes watchdog checks
        // sequentially.
        // If one check blocks—for example, killContainer(jobId) takes a long time—other
        // jobs' checks may be delayed.

        Process child = null;

        try {
            child = new ProcessBuilder(cmd).start(); // spawns docker

            final Process p = child;
            AtomicLong killedAt = new AtomicLong(0);

            redis.opsForHash().putAll(key, Map.of("status", "RUNNING", "startedAt", String.valueOf(start)));

            schedular.scheduleAtFixedRate(() -> {
                try {
                    if (timedOut.get() || cancelled.get()) {
                        // if the docker run CLI process is still alive after killing container, we'll
                        // destroyForcibly()
                        if (p.isAlive() && System.currentTimeMillis() - killedAt.get() > 2000) {
                            p.destroyForcibly(); // last resort: kills the CLI, unblocks waitFor
                        }
                        return;
                    }
                    if (System.currentTimeMillis() - start >= TIME_LIMIT_MS) {
                        System.out.println("[WD] job=" + jobId + " limit hit at " + (System.currentTimeMillis() - start)
                                + "ms, killing " + killTarget);
                        timedOut.set(true);
                        timedOut.set(true);
                        killedAt.set(System.currentTimeMillis());
                        killContainer(killTarget);
                    } else if (redis.opsForHash().hasKey(key, "cancelRequested")) {
                        cancelled.set(true);
                        killedAt.set(System.currentTimeMillis());
                        killContainer(killTarget);
                    }
                } catch (Exception ignored) {
                    // TODO: handle exception
                }
            }, 500, 500, TimeUnit.MILLISECONDS);

            // we will discard the bytes after the limit, that covers the edge case ->
            // output consuming too much memory (ex = milion lines output)
            CappedBuffer out = new CappedBuffer(MAX_OUTPUT);
            CappedBuffer err = new CappedBuffer(MAX_OUTPUT);

            Thread tOut = drain(child.getInputStream(), out);
            Thread tErr = drain(child.getErrorStream(), err);

            writeInput(child, inputString);

            int exitCode = child.waitFor(); // it waits until the docker CLI process executes
            tOut.join();
            tErr.join(); // will wait until the thread is done executing

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
                res.put("verdict", timedOut.get() || exitCode == 124 ? "TLE"
                        : exitCode == 0 ? "OK" : exitCode == 137 ? "MLE" : "RE");
                // == 137 ? "MLE" : "RE");
            }
            redis.opsForHash().putAll(key, res);
            redis.expire(key, Duration.ofHours(1));
            clean = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finish(jobId, "ERROR", "INTERNAL", "Interrupted");
        } catch (Exception e) {
            finish(jobId, "ERROR", "INTERNAL", String.valueOf(e.getMessage()));
        } finally {
            schedular.shutdownNow();
            if (child != null && child.isAlive())
                killContainer(killTarget);
            cleanup.run();
            if (slot >= 0 && warmPool != null && (!clean || timedOut.get() || cancelled.get())) {
                warmPool.recycle(slot);
            }
        }
    }

    private void finish(String jobId, String status, String verdict, String stderr) {
        redis.opsForHash().putAll("run:" + jobId, Map.of(
                "status", status, "verdict", verdict, "stderr", stderr,
                "finishedAt", String.valueOf(System.currentTimeMillis())));
    }

    private void killContainer(String name) {
        try {
            long t0 = System.currentTimeMillis();
            Process rm = new ProcessBuilder("docker", "rm", "-f", name)
                    .redirectErrorStream(true).start();
            boolean done = rm.waitFor(5, TimeUnit.SECONDS);
            String out = new String(rm.getInputStream().readAllBytes()).trim();
            System.out.println("[WD] rm -f " + name + " done=" + done
                    + (done ? " exit=" + rm.exitValue() : "")
                    + " in " + (System.currentTimeMillis() - t0) + "ms out='" + out + "'");
        } catch (InterruptedException e) {
            // job already ended, the watchdog was shut down mid-wait; the rm was already
            // issued
        } catch (Exception e) {
            System.out.println("[WD] rm -f failed: " + e);
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
