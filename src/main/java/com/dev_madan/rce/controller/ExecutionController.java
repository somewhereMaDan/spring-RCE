package com.dev_madan.rce.controller;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dev_madan.rce.dto.RunRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/runs")
@RequiredArgsConstructor
public class ExecutionController {
    private final StringRedisTemplate redis;
    private final ObjectMapper om;

    @PostMapping
    public ResponseEntity<Map<String, String>> submit(
            @RequestBody RunRequest req) throws Exception {

        String jobId = UUID.randomUUID().toString();
        req.setJobId(jobId);

        redis.opsForHash().putAll(
                "run:" + jobId,
                Map.of(
                        "status", "QUEUED",
                        "createdAt",
                        String.valueOf(System.currentTimeMillis())));

        redis.expire("run:" + jobId, Duration.ofHours(1));

        redis.opsForList().rightPush(
                "rce:jobs",
                om.writeValueAsString(req));

        return ResponseEntity.accepted().body(Map.of("jobId", jobId));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<Map<Object, Object>> get(
            @PathVariable String jobId) {

        Map<Object, Object> result = redis.opsForHash().entries("run:" + jobId);

        return result.isEmpty()
                ? ResponseEntity.notFound().build()
                : ResponseEntity.ok(result);
    }

    @DeleteMapping("/{jobId}")
    public ResponseEntity<Void> cancel(@PathVariable String jobId) {
        String key = "run:" + jobId;
        if (Boolean.TRUE.equals(redis.hasKey(key))) {
            redis.opsForHash().put(key, "cancelRequested", "true");
        }
        return ResponseEntity.accepted().build();
    }
}
