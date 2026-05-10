package com.dev_madan.rce.controller;

import com.dev_madan.rce.dto.RunRequest;
import com.dev_madan.rce.service.DockerExecutorService;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Controller
public class ExecutionController {
    private final DockerExecutorService executorService;

    private final Map<String, Process> sessionProcessMap = new ConcurrentHashMap<>();

    public ExecutionController(DockerExecutorService executorService) {
        this.executorService = executorService;
    }

    @MessageMapping("/run")
    public void handleRun(RunRequest request) {

        String sessionId = request.getSessionId();

        Process existing = sessionProcessMap.get(sessionId);
        if (existing != null && existing.isAlive()) {
            existing.destroyForcibly();
        }

        Process handle = executorService.execute(
                request.getLang(),
                request.getCode(),
                request.getInputString(),
                sessionId
        );

        if (handle != null) {
            sessionProcessMap.put(sessionId, handle);
        }
    }
}
