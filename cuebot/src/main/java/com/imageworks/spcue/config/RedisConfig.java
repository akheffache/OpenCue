
/*
 * Copyright Contributors to the OpenCue Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.imageworks.spcue.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scripting.support.ResourceScriptSource;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Redis configuration for scheduling cache.
 * Only loaded when redis.scheduling.enabled=true
 *
 * Enables @Async for non-blocking Redis event handling.
 */
@Configuration
@EnableAsync
@ConditionalOnProperty(name = "redis.scheduling.enabled", havingValue = "true")
public class RedisConfig {

    private static final Logger logger = LogManager.getLogger(RedisConfig.class);

    @Value("${redis.scheduling.enabled:false}")
    private boolean redisEnabled;

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        logger.info("Initializing Redis template for scheduling cache");

        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // Use String serializer for keys
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        // Use JSON serializer for values
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());

        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public RedisTemplate<String, String> stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    /**
     * Lua script for finding next dispatch frames.
     * Atomically finds waiting frames that match host resources.
     */
    @Bean
    public RedisScript<List> findDispatchFramesScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("lua/find_dispatch_frames.lua")));
        script.setResultType(List.class);
        return script;
    }

    /**
     * Lua script for finding dispatch frames by layer.
     * Simpler version that checks a single layer instead of all job layers.
     */
    @Bean
    public RedisScript<List> findDispatchFramesByLayerScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(
                new ClassPathResource("lua/find_dispatch_frames_by_layer.lua")));
        script.setResultType(List.class);
        return script;
    }

    /**
     * Single-threaded executor for async Redis event handling.
     *
     * WHY SINGLE-THREADED: Events for the same frame must be processed in order.
     * With multiple threads, a frame retry sequence (WAITING→RUNNING→WAITING→RUNNING)
     * could be processed out of order, leaving Redis with stale state. A single thread
     * guarantees FIFO ordering. Redis writes are ~10x faster than SQL, so this is not
     * a bottleneck.
     *
     * FUTURE: If throughput becomes an issue, use a keyed executor that routes events
     * by frameId hash (frameId.hashCode() % numThreads) to guarantee per-frame ordering
     * while allowing parallelism across different frames.
     */
    @Bean(name = "redisAsyncExecutor")
    public Executor redisAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // Single thread to guarantee event ordering (see comment above)
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1000);
        executor.setThreadNamePrefix("redis-sync-");
        // CallerRunsPolicy ensures no events are dropped - if queue is full,
        // the task runs in the caller's thread, maintaining SQL/Redis sync
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        logger.info("Redis async executor initialized (single-threaded for event ordering)");
        return executor;
    }
}
