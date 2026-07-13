package com.dev_madan.rce.config;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {
  // to push or pop from the queue
  @Primary
  @Bean
  public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory factory) {
    RedisTemplate<String, String> template = new RedisTemplate<>();
    template.setConnectionFactory(factory);
    template.setKeySerializer(new StringRedisSerializer());
    template.setValueSerializer(new StringRedisSerializer());
    return template;
  }

  // To listen
  @Bean
  public RedisMessageListenerContainer redisMsgListenerContainer(RedisConnectionFactory factory) {
    RedisMessageListenerContainer msgContainer = new RedisMessageListenerContainer();
    msgContainer.setConnectionFactory(factory);
    return msgContainer;
  }

  @Bean
  public ObjectMapper objectMapper(){
    return new ObjectMapper();
  }
}
