package com.dev_madan.rce.controller;

import com.dev_madan.rce.dto.ExecutionHandle;
import com.dev_madan.rce.dto.RunRequest;
import com.dev_madan.rce.dto.StdinRequest;
import com.dev_madan.rce.service.DockerExecutorService;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.stereotype.Controller;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Controller
public class ExecutionController {
    private final DockerExecutorService executorService;

    private final Map<String, ExecutionHandle> sessionProcessMap = new ConcurrentHashMap<>();
    // ConcurrentHashMap instead of plain HashMap because multiple WebSocket threads can hit this map simultaneously.
    // In Node the event loop made this safe automatically.

    // sessionId — Spring gives every WebSocket connection a unique session ID automatically,
    // accessible via SimpMessageHeaderAccessor. Think of it as the server-side equivalent of the ws object identity.


    // Spring injects DockerExecutorService here automatically (you know this from @Service/@Autowired)
    public ExecutionController(DockerExecutorService executorService) {
        this.executorService = executorService;
    }

    // ── /app/run ──────────────────────────────────────────────────────────────
    // equivalent of: if (type === 'run') { ... }
    // @MessageMapping replaces the if/else entirely. Each method is a route: @MessageMapping("/run")
    // client sends to /app/run

    @MessageMapping("/run")
    public void handleRun(RunRequest request) {

        String sessionId = request.getSessionId();

        // kill existing process for this session — same as:
        // if (currentProcess) currentProcess.kill()
        ExecutionHandle existing = sessionProcessMap.get(sessionId);
        if (existing != null && existing.process().isAlive()) {
            existing.process().destroyForcibly();  // child.kill()
        }

        // start new process — same as:
        // currentProcess = executePython(code, ws)  (or java/cpp)
        ExecutionHandle handle = executorService.execute(
                request.getLang(),
                request.getCode(),
                sessionId
        );

        if (handle != null) {
            sessionProcessMap.put(sessionId, handle);
        }
    }

    // ── /app/stdin ────────────────────────────────────────────────────────────
    // equivalent of: if (type === 'stdin') { ... }

    @MessageMapping("/stdin")
    public void handleStdin(StdinRequest request) {
        String sessionId = request.getSessionId();

        ExecutionHandle handle = sessionProcessMap.get(sessionId);

        // same as: if (currentProcess) { currentProcess.stdin.write(data) ... }
        if (handle != null && handle.process().isAlive()) {
            try {
                // process.getOutputStream() = currentProcess.stdin in Node
                // writing bytes to the running Docker process
                handle.process().getOutputStream().write(request.getData().getBytes());
                handle.process().getOutputStream().flush(); // Node did this implicitly

                handle.waitingForInput().set(true);

                // same as: currentProcess.controls?.resetInputTimer()
                handle.resetTimer().run();

            } catch (IOException e) {
                // process already died between our isAlive() check and the write
                // safe to ignore — streams are already closed
            }
        }
    }
}

//    Node                                   Spring
//    ────────────────────────────────────────────────────────────────
//            wss.on('connection', ws => {        →  @Controller (singleton)
//    let currentProcess = null           →  Map<String, ExecutionHandle> sessions
//    })                                     keyed by sessionId
//
//    ws.on('message', msg => {           →  @MessageMapping("/run")
//        if (type === 'run') {...}            @MessageMapping("/stdin")
//        if (type === 'stdin') {...}          (Spring routes by destination, not type field)
//    })
//
//            currentProcess.kill()               →  handle.process().destroyForcibly()
//    currentProcess.stdin.write(data)    →  handle.process().getOutputStream().write(...)
//    controls.setWaitingForInput(false)  →  handle.waitingForInput().set(false)
//    controls.resetInputTimer()          →  handle.resetTimer().run()
//
//    ws.on('close', () => {...})         →  @EventListener SessionDisconnectEvent
