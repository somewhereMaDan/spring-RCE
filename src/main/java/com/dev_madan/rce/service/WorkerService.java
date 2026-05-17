package com.dev_madan.rce.service;

import com.dev_madan.rce.dto.RunRequest;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;

@RequiredArgsConstructor
@Service

public class WorkerService {
  private final DockerExecutorService dockerExecService;
  private final RedisTemplate<String, String> redisT;
  private final ObjectMapper objMap;
  private final SessionRegistry sessionReg;

  private static final String JOB_QUEUE = "rce:jobs";
  private static final int WORKER_COUNT = 8;
  private volatile boolean running = true;

  // "run this method automatically after Spring has finished creating and injecting all dependencies into this bean"
  @PostConstruct
  public void startWorkers(){
    for(int i = 0; i < WORKER_COUNT; i++){
      int workerId = i;

      Thread thread = new Thread(() -> workerLoop(workerId));

      thread.setName("worker-" + workerId);

      thread.start();
    }
  }

  private void workerLoop(int workerId){
    while (running){
      try{
        String job = redisT.opsForList().leftPop(JOB_QUEUE, Duration.ofSeconds(2));
        if(job == null) continue;
        // JSON string → RunRequest object
        RunRequest jobData = objMap.readValue(job, RunRequest.class);

        // if users clicks run again
        sessionReg.killIfExists(jobData.getSessionId());

        Process handle = dockerExecService.execute(
            jobData.getLang(),
            jobData.getCode(),
            jobData.getInputString(),
            jobData.getSessionId()
        );

        if(handle != null){
          sessionReg.register(jobData.getSessionId(), handle);
        }
      }catch (Exception e){
        if (!running) break;
//        e.printStackTrace();
      }
    }
  }

  @PreDestroy
  public void shutdown() {
    running = false;  // signals all worker threads to stop
  }
}
