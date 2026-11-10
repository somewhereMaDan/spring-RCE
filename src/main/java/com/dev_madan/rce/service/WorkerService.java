package com.dev_madan.rce.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.data.redis.connection.stream.*;

import com.dev_madan.rce.dto.RunRequest;
import com.dev_madan.rce.pool.WarmPool;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.domain.*;

import lombok.RequiredArgsConstructor;

@Profile("worker")
@RequiredArgsConstructor
@Service
public class WorkerService implements SmartLifecycle {
  private final DockerExecutorService dockerExecutorService;
  private final ObjectMapper objMap;
  private final StringRedisTemplate redis;
  private final WarmPool warmPool;

  // private static final String JOB_QUEUE = "rce:jobs";
  // private static final int WORKER_COUNT = 2;

  private static final Duration RECLAIM_IDLE = Duration.ofSeconds(30);

  private static final String STREAM = "rce:stream";
  private static final String GROUP = "workers";
  private static final int WORKER_COUNT = 6;

  private static final int MAX_DELIVERIES = 3;
  private static final String DEAD_STREAM = "rce:dead";

  private final String consumerPrefix = System.getenv().getOrDefault("HOSTNAME", "local");

  // Gracefull Shutdown edge-case
  // Executions are killed after 10s, so a running job always ends within ~10s.
  // 3s buffer for publishing final output/exit event and listener cleanup.
  private static final Duration MAX_EXECUTION = Duration.ofSeconds(10);
  private static final Duration SHUTDOWN_WAIT = MAX_EXECUTION.plusSeconds(3);

  private final List<Thread> workers = new ArrayList<>();
  private volatile boolean running = true;
  private volatile boolean shuttingDown = false;
  private volatile boolean active = false; // SmartLifecycle state

  @Override
  public void start() {
    warmPool.start(WORKER_COUNT);
    createGroupIfMissing();
    for (int i = 0; i < WORKER_COUNT; i++) {
      int workerId = i;

      Thread thread = new Thread(() -> workerLoop(workerId));
      thread.setName("worker-" + workerId);

      workers.add(thread);
      thread.start();
    }
    active = true;
  }

  private void createGroupIfMissing() {
    try {
      redis.opsForStream().createGroup(STREAM, ReadOffset.from("0"), GROUP);
      // On the stream rce:stream, create a consumer group called workers. This group
      // should start reading messages from the beginning of the stream (ID 0).
    } catch (RuntimeException e) {
      Throwable root = e;
      while (root.getCause() != null)
        root = root.getCause();
      // The first time it creates the group.
      // Every later time (restart, or a second worker container) Redis replies
      // BUSYGROUP Consumer Group name already exists
      if (!String.valueOf(root.getMessage()).contains("BUSYGROUP"))
        throw e;
    }
  }

  private void workerLoop(int workerId) {
    String consumer = consumerPrefix + "-" + workerId;
    while (running && !shuttingDown) {
      try {
        System.out.println("consumer: " + consumer);
        List<MapRecord<String, Object, Object>> records = claimStale(consumer);

        if (records.isEmpty()) {
          records = redis.opsForStream().read(
              Consumer.from(GROUP, consumer),
              StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
              StreamOffset.create(STREAM, ReadOffset.lastConsumed()));
        }

        if (records == null || records.isEmpty()) {
          continue;
        }

        // "Hey Redis! I'm employee container123-0, working in the workers department.
        // Please give me one job from the rce:stream inbox that hasn't already been
        // delivered to someone in my department. If there are no jobs, I'll wait for up
        // to 2 seconds."

        // Remember these four things:
        // - Consumer.from() → Who am I?
        // - GROUP → Which department do I belong to?
        // - StreamOffset.create() → Which inbox should I read from?
        // - lastConsumed() → Give me a job not yet delivered to my department.

        MapRecord<String, Object, Object> record = records.get(0);
        RecordId recordId = record.getId();
        String job = (String) record.getValue().get("payload");

        // Shutdown may have started while read was waiting.
        // Don't start the job. It stays pending, and another worker reclaims it.
        if (shuttingDown) {
          break;
        }

        try {
          RunRequest jobData = objMap.readValue(job, RunRequest.class);

          System.out.println(
              "[WORKER-" + workerId + "] Picked job: " + jobData.getJobId());
          String key = "run:" + jobData.getJobId();

          if (redis.opsForHash().hasKey(key, "cancelRequested")) {
            redis.opsForHash().putAll(key, Map.of(
                "status", "CANCELLED",
                "finishedAt", String.valueOf(System.currentTimeMillis())));
            continue;
          }

          System.out.printf("[%s] Worker-%d START job=%s%n", java.time.LocalTime.now(), workerId,
              jobData.getJobId());

          // Blocks until execution completes (or is killed at the 10s limit).
          if (!"cpp".equals(jobData.getLang())) {
            try {
              new ProcessBuilder("docker", "rm", "-f", "rce-" + jobData.getJobId())
                  .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                  .redirectErrorStream(true).start().waitFor(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
          }
          try {
            dockerExecutorService.execute(
                jobData.getLang(),
                jobData.getCode(),
                jobData.getInputString(),
                jobData.getJobId(),
                workerId);
          } catch (Exception e) {
            redis.opsForHash().putAll(key, Map.of(
                "status", "ERROR", "verdict", "INTERNAL",
                "stderr", String.valueOf(e.getMessage()),
                "finishedAt", String.valueOf(System.currentTimeMillis())));
          }

          System.out.printf(
              "[%s] Worker-%d FINISHED job=%s%n",
              java.time.LocalTime.now(),
              workerId,
              jobData.getJobId());
        } finally {
          redis.opsForStream().acknowledge(STREAM, GROUP, recordId); // acknowledge is XACK. It removes the job from the
                                                                     // pending list: “done, never redeliver”.
          redis.opsForStream().delete(STREAM, recordId); // delete is XDEL. It removes the entry from the stream itself.
                                                         // Without it the stream grows forever.
        }

      } catch (Exception e) {
        if (shuttingDown || !running) {
          break;
        }
        System.err.printf("Worker-%d error: %s%n", workerId, e.getMessage());

        // f Redis goes down, leftPop throws instantly and every worker spins at 100%
        // CPU printing errors
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    System.out.println("Worker-" + workerId + " stopped.");
  }

  private List<MapRecord<String, Object, Object>> claimStale(String consumer) {
    PendingMessages pending = redis.opsForStream()
        .pending(STREAM, GROUP, Range.unbounded(), 10L);

    if (pending == null || pending.isEmpty()) {
      return List.of();
    }

    PendingMessage stale = pending.stream()
        .filter(m -> m.getElapsedTimeSinceLastDelivery().compareTo(RECLAIM_IDLE) >= 0)
        .findFirst()
        .orElse(null);

    if (stale == null) {
      return List.of();
    }

    List<MapRecord<String, Object, Object>> claimed = redis.opsForStream().claim(STREAM, GROUP, consumer, RECLAIM_IDLE,
        stale.getId());
    // XCLAIM rce:stream workers worker-2 30000 1002-0/recordId
    // Transfer ownership of message 1002-0 from its previous consumer to worker-2,
    // provided it has been idle for at least 30 seconds.

    // dummy ->
    // claimed = [
    // MapRecord {
    // stream: "rce:stream",

    // id: RecordId("1730891234567-0"),

    // value: {
    // "payload": """
    // {
    // "jobId": "job-123",
    // "lang": "cpp",
    // "code": "int main() { while(1); }",
    // "inputString": ""
    // }
    // """
    // }
    // }
    // ];
    if (claimed.isEmpty()) {
      return claimed;
    }

    if (stale.getTotalDeliveryCount() >= MAX_DELIVERIES) {
      buryPoison(claimed.get(0));
      return List.of();
    }
    return claimed;
  }

  private void buryPoison(MapRecord<String, Object, Object> record) {
    try {
      RunRequest r = objMap.readValue((String) record.getValue().get("payload"), RunRequest.class);
      redis.opsForHash().putAll("run:" + r.getJobId(), Map.of(
          "status", "ERROR", "verdict", "INTERNAL",
          "stderr", "Job failed after " + MAX_DELIVERIES + " attempts",
          "finishedAt", String.valueOf(System.currentTimeMillis())));
    } catch (Exception e) {
      // TODO: handle exception
    }
    redis.opsForStream().add(DEAD_STREAM, record.getValue());
    redis.opsForStream().acknowledge(STREAM, GROUP, record.getId());
    redis.opsForStream().delete(STREAM, record.getId());
  }

  // Shutdown sequence:
  // 1. Stop accepting new jobs (shuttingDown = true).
  // 2. Let active Docker executions finish (max 10s, they are killed after that).
  // 3. Workers publish final output and exit events to Redis.
  // 4. Wait for all worker threads to terminate (shared deadline).
  // 5. Spring then stops LettuceConnectionFactory (lower phase).
  @Override
  public void stop() {
    System.out.println("Shutdown requested. Stopping job pickup...");

    shuttingDown = true;

    // Total wait for all workers combined is capped at 13s from now.
    long deadline = System.nanoTime() + SHUTDOWN_WAIT.toNanos();

    for (Thread worker : workers) {
      // then calculating how much it has considering the deadline
      long remainingMs = Duration.ofNanos(deadline - System.nanoTime()).toMillis();
      if (remainingMs <= 0) {
        break;
      }
      try {
        worker.join(remainingMs); // it'll wait for seconds till it reaches the remaningMs with the deadline
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println(
            "Interrupted while waiting for workers to stop.");
        break;
      }
    }

    running = false;
    active = false;
    warmPool.shutdown();

    boolean allStopped = workers.stream().noneMatch(Thread::isAlive);
    if (allStopped) {
      System.out.println(
          "All workers stopped. Safe to continue application shutdown.");
    } else {
      System.err.println(
          "Some workers did not stop within " + SHUTDOWN_WAIT.toSeconds()
              + "s. Continuing shutdown.");
    }
  }

  @Override
  public boolean isRunning() {
    return active;
  }

  @Override
  public int getPhase() {
    // Stops before the Lettuce connection factory (phase 0).
    return Integer.MAX_VALUE;
  }
}