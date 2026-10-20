package com.dev_madan.rce.controller;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Controller;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import com.dev_madan.rce.dto.RunRequest;
import com.dev_madan.rce.service.SessionRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor 
@Controller
public class ExecutionController {
  private final SimpMessagingTemplate simpMessagingTemplate;
  private final SessionRegistry sessionReg;
  private final ObjectMapper objMap;
  private final RedisTemplate<String, String> redisT;
  private static final String JOB_QUEUE = "rce:jobs";
  private static final int HARD_SAFETY_LIMIT = 10000;

  private final Map<String, String> springToClientSession = new ConcurrentHashMap<>();

  @EventListener
  public void handleConnect(SessionConnectEvent event) {
    StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());

    String springSessionId = accessor.getSessionId();
    String clientSessionId = accessor.getFirstNativeHeader("sessionId");

    if (clientSessionId != null) {
      springToClientSession.put(springSessionId, clientSessionId);
    }
  }

  @MessageMapping("/run")
  public void handleRun(RunRequest request) {
    String sessionId = request.getSessionId();
    String runId = UUID.randomUUID().toString();
    request.setRunId(runId);
    sessionReg.setCurrentRun(sessionId, runId);

    sessionReg.clearCancelled(sessionId);
    // Kill existing process if user clicks on run again
    sessionReg.killIfExists(sessionId);

    Long jobsAhead = redisT.opsForList().size(JOB_QUEUE);

    if (jobsAhead != null && jobsAhead > HARD_SAFETY_LIMIT) {
      sendStatus(sessionId, "error", "System is overloaded. Please try again shortly.");
      return;
    }

    try {
      String json = objMap.writeValueAsString(request);
      redisT.opsForList().rightPush(JOB_QUEUE, json);

      long position = (jobsAhead == null ? 0 : jobsAhead) + 1;
      sendStatus(sessionId, "queued", "Queued — position: " + position);
    } catch (Exception e) {
      System.err.println("Error while pushing the job into the queue: " + e.getMessage());
    }
  }

  private void sendStatus(String sessionId, String type, String message) {
    simpMessagingTemplate.convertAndSend("/topic/output/" + sessionId, (Object) Map.of("type", type, "data", message));
  }

  @EventListener
  public void handleDisconnect(SessionDisconnectEvent event) {
    String springSessionId = event.getSessionId();
    String clientSessionId = springToClientSession.remove(springSessionId);

    if (clientSessionId != null) {
      sessionReg.markCancelled(clientSessionId);
      sessionReg.killIfExists(clientSessionId);
    }
  }
}
