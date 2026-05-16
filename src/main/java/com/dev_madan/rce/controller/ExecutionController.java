package com.dev_madan.rce.controller;

import com.dev_madan.rce.dto.RunRequest;
import com.dev_madan.rce.service.SessionRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@AllArgsConstructor
@Controller
public class ExecutionController {
  private final SimpMessagingTemplate simpMessagingTemplate;
  private final RedisMessageListenerContainer redisMessageListenerContainer;
  private final SessionRegistry sessionReg;
  private final ObjectMapper objMap;
  private final RedisTemplate<String, String> redisT;
  private static final String JOB_QUEUE = "rce:jobs";

  private final Map<String, MessageListener> listenerMap = new ConcurrentHashMap<>();

  @MessageMapping("/run")
  public void handleRun(RunRequest request) {
    String sessionId = request.getSessionId();

    MessageListener oldListener = listenerMap.get(sessionId);
    if(oldListener != null){
      redisMessageListenerContainer.removeMessageListener(oldListener);
    }
    // Kill existing process if user clicks on run again
    sessionReg.killIfExists(sessionId);

    MessageListener listener = ((message, pattern) -> {
      String payload = new String(message.getBody());
      simpMessagingTemplate.convertAndSend(
          "/topic/output/" + sessionId,
          (Object) payload
      );
    });

    // I want to receive any message published to this channel. You become a subscriber.
    // "Hey Redis, I want to receive messages on channel output:abc123"
    redisMessageListenerContainer.addMessageListener(
        listener,
        new ChannelTopic("output:" + sessionId)
    );
    listenerMap.put(sessionId, listener);
    System.out.println("listenerMap: " + listenerMap);

    // push job to queue after subscribing so we don't lose any chunks
    try {
      String json = objMap.writeValueAsString(request);
      redisT.opsForList().rightPush(JOB_QUEUE, json);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
