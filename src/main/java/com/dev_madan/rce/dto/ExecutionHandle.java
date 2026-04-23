package com.dev_madan.rce.dto;

import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.Process;

public record ExecutionHandle(Process process, AtomicBoolean waitingForInput, Runnable resetTimer) {

}
