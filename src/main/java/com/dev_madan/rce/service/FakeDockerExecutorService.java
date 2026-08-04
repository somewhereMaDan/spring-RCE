package com.dev_madan.rce.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

@Service
@Profile("loadtest")
public class FakeDockerExecutorService implements CodeExecutor {

  private final SessionRegistry sessionReg;
  private final RedisTemplate<String, String> redisTemplate;
  private final ObjectMapper objectMapper;
  private final Semaphore containerSlots;

  public FakeDockerExecutorService(SessionRegistry sessionReg,
                                   RedisTemplate<String, String> redisTemplate,
                                   ObjectMapper objectMapper,
                                   @Value("${rce.docker.max-concurrent:6}") int maxConcurrent) {
    this.sessionReg = sessionReg;
    this.redisTemplate = redisTemplate;
    this.objectMapper = objectMapper;
    this.containerSlots = new Semaphore(maxConcurrent);
  }

  private void send(String sessionId, String type, String data) {
    try {
      String payload = objectMapper.writeValueAsString(Map.of("type", type, "data", data));
      redisTemplate.convertAndSend("output:" + sessionId, payload);
    } catch (Exception ignored) {
    }
  }

  @Override
  public Process execute(String lang, String code, String inputString, String sessionId, Runnable listenerCleanup) {
    try {
      containerSlots.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      send(sessionId, "stderr", "Interrupted while waiting for an execution slot");
      listenerCleanup.run();
      return null;
    }

    send(sessionId, "status", "now running...");

    // no real registration in SessionRegistry.register(sessionId, child) since
    // there's no real Process to store — cancellation isn't simulated in this fake.
    Thread fakeExecThread = new Thread(() -> {
      try {
        int fakeDelayMs = ThreadLocalRandom.current().nextInt(300, 1200);
        Thread.sleep(fakeDelayMs);

        send(sessionId, "stdout", "hello world (fake output for load test)\n");

        Map<String, Object> exitPayload = Map.of("type", "exit", "code", 0, "cpuTime", (long) fakeDelayMs);
        String exitJson = objectMapper.writeValueAsString(exitPayload);
        redisTemplate.convertAndSend("output:" + sessionId, exitJson);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        System.err.println("Error in fake execution: " + e.getMessage());
      } finally {
        sessionReg.removeProcess(sessionId);
        listenerCleanup.run();
        containerSlots.release();
      }
    });

    fakeExecThread.start();
    return null; // WorkerService never touches the return value — confirmed fire-and-forget
  }
}