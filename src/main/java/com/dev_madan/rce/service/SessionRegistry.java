package com.dev_madan.rce.service;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Service;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SessionRegistry {
  private final Map<String, Process> sessionProcessMap = new ConcurrentHashMap<>();
  private final Map<String, MessageListener> listenerMap = new ConcurrentHashMap<>();

  public void register(String sessionId, Process process){
    sessionProcessMap.put(sessionId, process);
  }

  public void killIfExists(String sessionId){
    Process exists = sessionProcessMap.get(sessionId);
    if(exists != null && exists.isAlive()){
      exists.destroyForcibly();
    }
  }

  public void removeProcess(String sessionId){
    sessionProcessMap.remove(sessionId);
  }

  public void registerListener(String sessionId, MessageListener listener) {
    listenerMap.put(sessionId, listener);
  }

  public MessageListener removeListener(String sessionId) {
    return listenerMap.remove(sessionId);
  }
}
