package com.dev_madan.rce.pool;
// TODO: add your package line here, plus the imports below if your IDE doesn't.

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Warm pool, piece 1: one long-lived sandbox container per worker thread.
 * Containers are created at worker start and removed at shutdown.
 * Nothing executes through them yet (that is piece 2: docker exec).
 *
 * Only cpp for now: it is the language measured in the spike. Python and Java
 * keep using docker run until the cpp numbers are re-measured.
 */
@Profile("worker")
@Service
public class WarmPool {

    private static final String IMAGE = "cpp-runner";

    // Per-slot CPU quota. Override with POOL_CPUS (e.g. 1) to test how much the cap limits throughput.
    private static final String POOL_CPUS = System.getenv().getOrDefault("POOL_CPUS", "0.25");

    // Same limits as DockerExecutorService.SANDBOX. Duplicated on purpose for now;
    // make that constant package-private and share it once the pool works.
    private static final List<String> SANDBOX = List.of(
            "--memory=256m", "--memory-swap=256m", "--network", "none",
            "--pids-limit=128",
            "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=32m",
            "--tmpfs", "/run-code:rw,exec,nosuid,nodev,size=32m,mode=700,uid=1000,gid=1000",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges");

    // Path of the shared bind mount as seen inside the worker container.
    private static final Path WORKER_SLOTS_ROOT = Path.of("/app/temp/slots");

    @Value("${app.host-temp-path:}")
    private String hostTempPath;

    // Container hostname: unique per worker container, so scaled workers never collide.
    private final String owner = System.getenv().getOrDefault("HOSTNAME", "local");

    private int slotCount;

    public void start(int slotCount) {
        if (hostTempPath == null || hostTempPath.isBlank()) {
            throw new IllegalStateException("app.host-temp-path must be set for the warm pool");
        }
        this.slotCount = slotCount;

        // Leftovers from a previous run of this same worker container (e.g. after SIGKILL + restart).
        removeOwnContainers();

        for (int i = 0; i < slotCount; i++) {
            createSlot(i);
        }
        System.out.println("[POOL] " + slotCount + " warm containers ready for owner " + owner);
    }

    public void shutdown() {
        removeOwnContainers();
        for (int i = 0; i < slotCount; i++) {
            deleteTree(workerDir(i));
        }
    }

    /** Replace a slot's container with a fresh one (after timeout, cancel or failure). */
    public void recycle(int slot) {
        run(List.of("docker", "rm", "-f", containerName(slot)), 15);
        // The watchdog's own rm -f may still be removing this container, so the name can be
        // briefly in use. Retry creation a few times before giving up.
        for (int attempt = 1; attempt <= 5; attempt++) {
            try {
                createSlot(slot);
                return;
            } catch (RuntimeException e) {
                if (attempt == 5) {
                    System.err.println("[POOL] recycle failed for slot " + slot + ": " + e.getMessage());
                    return;
                }
                try {
                    Thread.sleep(300L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public String containerName(int slot) {
        return "rce-slot-" + owner + "-" + slot;
    }

    /** Where the worker writes the job file (path inside the worker container). */
    public Path workerDir(int slot) {
        return WORKER_SLOTS_ROOT.resolve(owner + "-" + slot);
    }

    /** Same directory as the Docker daemon on the host sees it. */
    private Path hostDir(int slot) {
        return Path.of(hostTempPath, "slots", owner + "-" + slot);
    }

    private void createSlot(int slot) {
        try {
            Files.createDirectories(workerDir(slot));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create slot dir " + workerDir(slot), e);
        }

        List<String> cmd = new ArrayList<>(List.of(
                "docker", "run", "-d", "--init",
                "--name", containerName(slot),
                "--label", "rce.pool=1",
                "--label", "rce.owner=" + owner,
                "--label", "rce.slot=" + slot));
        cmd.addAll(SANDBOX);
        cmd.add("--cpus=" + POOL_CPUS);
        cmd.addAll(List.of("-v", hostDir(slot).toString().replace("\\", "/") + ":/code:ro"));
        cmd.addAll(List.of(IMAGE, "sleep", "infinity"));

        Result r = run(cmd, 30);
        if (r.exit != 0) {
            throw new IllegalStateException(
                    "Failed to create warm container " + containerName(slot) + ": " + r.output);
        }
    }

    private void removeOwnContainers() {
        Result ids = run(List.of("docker", "ps", "-aq", "--filter", "label=rce.owner=" + owner), 15);
        if (ids.exit != 0 || ids.output.isBlank()) {
            return;
        }
        List<String> cmd = new ArrayList<>(List.of("docker", "rm", "-f"));
        for (String id : ids.output.trim().split("\\s+")) {
            cmd.add(id);
        }
        run(cmd, 30);
    }

    private record Result(int exit, String output) {
    }

    private Result run(List<String> cmd, int timeoutSec) {
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            // Output here is tiny (ids or one error line), so reading after the process is safe.
            if (!p.waitFor(timeoutSec, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return new Result(-1, "timed out: " + String.join(" ", cmd));
            }
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            return new Result(p.exitValue(), out);
        } catch (IOException e) {
            return new Result(-1, String.valueOf(e.getMessage()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Result(-1, "interrupted");
        }
    }

    private void deleteTree(Path dir) {
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }
}