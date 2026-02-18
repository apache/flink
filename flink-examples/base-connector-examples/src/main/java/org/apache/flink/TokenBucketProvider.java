package com.example.flink;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TokenBucketProvider {
    private static final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    public static Bucket getInstance(String name, long tokensPerSecond, long tokensPerMinute) {
        return buckets.computeIfAbsent(name, k -> {
            Bandwidth limit = Bandwidth.classic(tokensPerMinute, Refill.intervally(tokensPerMinute, Duration.ofMinutes(1)));
            return Bucket.builder().addLimit(limit).build();
        });
    }
}
