package com.dev_madan.rce.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


@Service
public class DockerExecutorService {
  @Value("${app.host-temp-path:}")
  private String hostTempPath;

  private final RedisTemplate<String, String> redisTemplate;
  private final ObjectMapper objectMapper;
  private final SessionRegistry sessionReg;

  public DockerExecutorService(RedisTemplate<String, String> redisTemplate,
                               ObjectMapper objectMapper, SessionRegistry sessionReg) {
    this.redisTemplate = redisTemplate;
    this.objectMapper = objectMapper;
    this.sessionReg = sessionReg;
  }

  private Process executePython(String code, String inputString, String sessionId) {
    String jobId = UUID.randomUUID().toString();
    String filename = jobId + ".py";
    Path containerTempDir = Path.of(hostTempPath.isBlank() ? "/app/temp" : hostTempPath);
    Path filepath = containerTempDir.resolve(filename);
    String hostTempDir = hostTempPath.isBlank() ? containerTempDir.toString().replace("\\", "/") : hostTempPath.replace("\\", "/");

    try {
      Files.createDirectories(containerTempDir);
      Files.writeString(filepath, code);
    } catch (IOException e) {
      sendError(sessionId, "Failed to write code on file: " + e.getMessage());
      return null;
    }

    // we never close the input stream

    List<String> cmd = List.of("docker", "run", "--rm",
        "-i", "--memory=256m", "--cpus=0.5", "--network", "none",
        "-v", String.format("%s:/code", hostTempDir),
        "python-runner", "python", "-u", String.format("/code/%s", filename));

    return startProcess(cmd, inputString, sessionId, () -> {
      try {
        Files.deleteIfExists(filepath);
      } catch (IOException ignored) {
      }
    });
  }

  private Process executeJava(String code, String inputString, String sessionId) {
    String jobId = UUID.randomUUID().toString();
    String filename = "Main.java";
    Path containerTempDir = Path.of(hostTempPath.isBlank() ? "/app/temp" : hostTempPath, jobId);
    Path filePath = containerTempDir.resolve(filename);

    System.out.println("containerTempDir: " + containerTempDir);
    System.out.println("filePath: " + filePath);

    try {
      Files.createDirectories(containerTempDir);
      Files.writeString(filePath, code);
    } catch (IOException e) {
      sendError(sessionId, "Failed to write code on file: " + e.getMessage());
      return null;
    }

    List<String> cmd = List.of("docker", "run", "--rm",
        "-i", "--memory=256m", "--cpus=0.5", "--network", "none",
        "-v", String.format("%s:/code", containerTempDir.toString()), "java-runner", "sh", "-c",
        String.format("javac /code/%s && java -cp /code Main", filename));

    return startProcess(cmd, inputString, sessionId, () -> {
      deleteDir(containerTempDir.toString());
    });
  }


  public Process execute(String lang, String code, String inputString, String sessionId) {
    return switch (lang) {
      case "python" -> executePython(code, inputString, sessionId);
      case "java" -> executeJava(code, inputString, sessionId);
//            case "cpp" -> executeCpp(code, sessionId);
      default -> {
        sendError(sessionId, "Unsupported language: " + lang);
        yield null;
      }
    };
  }

  private Process startProcess(List<String> cmd, String inputString, String sessionId, Runnable cleanup) {
    Process child;
    try {
      child = new ProcessBuilder(cmd).redirectErrorStream(false).start();

      // register immediately after spawning — before threads start
      sessionReg.register(sessionId, child);
      // This way the process is registered the moment Docker spawns — not after execution finishes.
      // If disconnect fires during execution, killIfExists finds the process and destroys it.

      if (inputString != null && !inputString.isBlank()) {
        String normalized = inputString.endsWith("\n") ? inputString : inputString + "\n";
        child.getOutputStream().write(normalized.getBytes());
        child.getOutputStream().flush();
        child.getOutputStream().close();
      }
    } catch (IOException e) {
      sendError(sessionId, "Failed to start Docker: " + e.getMessage());
      cleanup.run();
      return null;
    }

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    AtomicLong cpuTime = new AtomicLong(0);
    AtomicLong cpuStart = new AtomicLong(System.currentTimeMillis());

    ScheduledFuture<?> checkCpuTime = scheduler.scheduleAtFixedRate(() -> {
      System.out.println("cpuTime: " + cpuTime);
      cpuTime.addAndGet(System.currentTimeMillis() - cpuStart.get());
      cpuStart.set(System.currentTimeMillis());
      if (cpuTime.get() >= 10000) {
        child.destroyForcibly();
        sendError(sessionId, "CPU Time Limit Exceeded");
      }
    }, 500, 500, TimeUnit.MILLISECONDS);


    Thread stdoutThread = new Thread(() -> {
      try (InputStream stdout = child.getInputStream()) {
        byte[] buf = new byte[1024];
        int n;

        while ((n = stdout.read(buf)) != -1) {
          String chunk = new String(buf, 0, n);
          send(sessionId, "stdout", chunk);
        }
      } catch (IOException ignored) {
      }
    });

    Thread stderrThread = new Thread(() -> {
      try (InputStream stderr = child.getErrorStream()) {
        byte[] buf = new byte[1024];
        int n;

        while ((n = stderr.read(buf)) != -1) {
          String chunk = new String(buf, 0, n);
          send(sessionId, "stderr", chunk);
        }
      } catch (IOException ignored) {
      }
    });

    stdoutThread.start();
    stderrThread.start();

    Thread waitThread = new Thread(() -> {
      try {
        int exitCode = child.waitFor(); // blocks until exit

        stdoutThread.join();
        stderrThread.join();

        checkCpuTime.cancel(false);
        scheduler.shutdown();

        Map<String, Object> exitPayload = Map.of("type", "exit", "code", exitCode, "cpuTime", cpuTime.get());

        String exitJson = objectMapper.writeValueAsString(exitPayload); // serialize to JSON string

        redisTemplate.convertAndSend("output:" + sessionId, exitJson);

        cleanup.run();

        // Every execution adds an entry, nothing ever removes it.
        // After thousands of executions the map holds thousands of dead Process references.
        // that's why
        sessionReg.removeProcess(sessionId);


      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    waitThread.start();
    return child;
  }

  private void send(String sessionId, String type, String data) {
    System.out.println("Sending message to output:" + sessionId);
    try {
      String payload = objectMapper.writeValueAsString(Map.of("type", type, "data", data));
      redisTemplate.convertAndSend("output:" + sessionId, payload);
      // stores message in channel "output:abc123"
      // looks up all the subscribers of "output:abc123"
      // delivers message to each subscriber (it delivers to spring listener)
      // when Redis delivers a message, it calls your registered listener (redisMessageListenerContainer with channel name -> "output:" + sessionId)
    } catch (Exception ignored) {
    }
  }

  private void sendError(String sessionId, String message) {
    send(sessionId, "stderr", message);
  }

  private void deleteDir(String dirPath) {
    Path dir = Path.of(dirPath);
    try {
      Files.walk(dir)
          .sorted(Comparator.reverseOrder()) // delete children first
          .forEach(path -> {
            try {
              Files.delete(path);
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    } catch (IOException ignored) {
    }
  }
}
