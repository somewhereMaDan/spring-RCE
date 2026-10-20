package com.dev_madan.rce.kafka;

import com.dev_madan.rce.service.SessionRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
  private final SessionRegistry sessionRegistry;
  private final SimpMessagingTemplate simpMessagingTemplate;

  public KafkaConsumer(SimpMessagingTemplate simpMessagingTemplate, SessionRegistry sessionRegistry) {
    this.simpMessagingTemplate = simpMessagingTemplate;
    this.sessionRegistry = sessionRegistry;
  }

  @KafkaListener(topics = "execution-output", groupId = "ws-gateway")
  public void consumeEvent(ConsumerRecord<String, String> record) {
    Header h = record.headers().lastHeader("runId");
    String runId = h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    if (!Objects.equals(runId, sessionRegistry.getCurrentRun(record.key())))
      return; // stale
    System.out.println("runId: " + runId + " Value: " + record.value().toString());
    simpMessagingTemplate.convertAndSend("/topic/output/" + record.key(), (Object) record.value());
  }
}
