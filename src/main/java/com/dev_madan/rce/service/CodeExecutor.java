package com.dev_madan.rce.service;

public interface CodeExecutor {
  Process execute(String lang, String code, String inputString, String sessionId, Runnable listenerCleanup);
}