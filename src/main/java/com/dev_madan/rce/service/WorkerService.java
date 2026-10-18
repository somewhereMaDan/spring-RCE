package com.dev_madan.rce.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Service;

import com.dev_madan.rce.dto.RunRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Service
public class WorkerService implements SmartLifecycle {
  private final DockerExecutorService dockerExecutorService;

  private final RedisMessageListenerContainer redisMessageListenerContainer;

  // private final DockerExecutorService dockerExecService;
  // private final CodeExecutor dockerExecService; used for k6 load testing
  private final RedisTemplate<String, String> redisT;
  private final ObjectMapper objMap;
  private final SessionRegistry sessionReg;

  private static final String JOB_QUEUE = "rce:jobs";
  private static final int WORKER_COUNT = 2;

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
    for (int i = 0; i < WORKER_COUNT; i++) {
      int workerId = i;

      Thread thread = new Thread(() -> workerLoop(workerId));
      thread.setName("worker-" + workerId);

      workers.add(thread);
      thread.start();
    }
    active = true;
  }

  private void workerLoop(int workerId) {
    while (running && !shuttingDown) {
      try {
        String job = redisT.opsForList()
            .leftPop(JOB_QUEUE, Duration.ofSeconds(2));

        if (job == null) {
          continue;
        }

        // Shutdown may have started while leftPop was waiting.
        // Put the fetched job back and exit instead of starting it.
        if (shuttingDown) {
          redisT.opsForList().leftPush(JOB_QUEUE, job);
          break;
        }

        RunRequest jobData = objMap.readValue(job, RunRequest.class);

        sessionReg.killIfExists(jobData.getSessionId());

        if (sessionReg.isCancelled(jobData.getSessionId())) {
          continue;
        }

        Runnable listenerCleanup = () -> {
          MessageListener listener = sessionReg.removeListener(jobData.getSessionId());

          if (listener != null) {
            redisMessageListenerContainer
                .removeMessageListener(listener);
          }
        };

        System.out.printf(
            "[%s] Worker-%d START session=%s%n",
            java.time.LocalTime.now(),
            workerId,
            jobData.getSessionId());

        // Blocks until execution completes (or is killed at the 10s limit).
        dockerExecutorService.execute(
            jobData.getLang(),
            jobData.getCode(),
            jobData.getInputString(),
            jobData.getSessionId(),
            listenerCleanup);

        System.out.printf(
            "[%s] Worker-%d FINISHED session=%s%n",
            java.time.LocalTime.now(),
            workerId,
            jobData.getSessionId());

      } catch (Exception e) {
        if (shuttingDown || !running) {
          break;
        }

        System.err.printf(
            "Worker-%d error: %s%n",
            workerId,
            e.getMessage());
      }
    }
    System.out.println("Worker-" + workerId + " stopped.");
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