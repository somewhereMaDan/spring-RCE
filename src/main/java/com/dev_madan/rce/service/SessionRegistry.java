package com.dev_madan.rce.service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

@Service
public class SessionRegistry {
  private final Map<String, Process> sessionProcessMap = new ConcurrentHashMap<>();
  private final Map<String, String> currentRunMap = new ConcurrentHashMap<>();
  private final Set<String> cancelledSessions = ConcurrentHashMap.newKeySet();

  public void register(String sessionId, Process process) {
    sessionProcessMap.put(sessionId, process);
  }

  public void killIfExists(String sessionId) {
    Process exists = sessionProcessMap.get(sessionId);
    if (exists != null && exists.isAlive()) {
      exists.destroyForcibly();
    }
  }

  public void removeProcess(String sessionId) {
    sessionProcessMap.remove(sessionId);
  }

  public void markCancelled(String sessionId) {
    cancelledSessions.add(sessionId);
  }

  public boolean isCancelled(String sessionId) {
    return cancelledSessions.contains(sessionId);
  }

  public void clearCancelled(String sessionId) {
    cancelledSessions.remove(sessionId);
  }

  public void setCurrentRun(String sessionId, String runId) {
    currentRunMap.put(sessionId, runId);
  }

  public String getCurrentRun(String sessionId) {
    return currentRunMap.get(sessionId);
  }

  public void clearCurrentRun(String sessionId) {
    currentRunMap.remove(sessionId);
  }
}
