/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.annotation.Internal;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Centralized metrics manager for S3 operations. This provides comprehensive monitoring of S3
 * client performance, operation success/failure rates, and resource utilization.
 */
@Internal
public class S3MetricsManager {

    private static final S3MetricsManager INSTANCE = new S3MetricsManager();

    // Operation counters using LongAdder for better performance under contention
    private final LongAdder totalOperations = new LongAdder();
    private final LongAdder totalErrors = new LongAdder();
    private final LongAdder totalBytesTransferred = new LongAdder();

    // Operation-specific counters
    private final Map<String, LongAdder> operationCounts = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> operationErrors = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> operationLatencies = new ConcurrentHashMap<>();

    // Client management metrics
    private final AtomicLong clientsCreated = new AtomicLong(0);
    private final AtomicLong clientsClosed = new AtomicLong(0);
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    private final AtomicLong activeHelpers = new AtomicLong(0);

    // Error tracking
    private final Map<String, LongAdder> errorTypes = new ConcurrentHashMap<>();

    private S3MetricsManager() {}

    public static S3MetricsManager getInstance() {
        return INSTANCE;
    }

    /** Records a successful operation with timing information. */
    public void recordOperationSuccess(
            String operationType, Duration latency, long bytesTransferred) {
        totalOperations.increment();
        operationCounts.computeIfAbsent(operationType, k -> new LongAdder()).increment();
        operationLatencies
                .computeIfAbsent(operationType, k -> new LongAdder())
                .add(latency.toMillis());

        if (bytesTransferred > 0) {
            totalBytesTransferred.add(bytesTransferred);
        }
    }

    /** Records a failed operation with error details. */
    public void recordOperationError(String operationType, Exception error) {
        totalErrors.increment();
        operationErrors.computeIfAbsent(operationType, k -> new LongAdder()).increment();

        // Track error types for analysis
        String errorType = error.getClass().getSimpleName();
        errorTypes.computeIfAbsent(errorType, k -> new LongAdder()).increment();
    }

    /** Records client lifecycle events. */
    public void recordClientCreated() {
        clientsCreated.incrementAndGet();
    }

    public void recordClientClosed() {
        clientsClosed.incrementAndGet();
    }

    public void recordCacheHit() {
        cacheHits.incrementAndGet();
    }

    public void recordCacheMiss() {
        cacheMisses.incrementAndGet();
    }

    public void recordHelperCreated() {
        activeHelpers.incrementAndGet();
    }

    public void recordHelperClosed() {
        activeHelpers.decrementAndGet();
    }

    /** Returns comprehensive metrics for monitoring and alerting. */
    public S3Metrics getMetrics() {
        return new S3Metrics(
                totalOperations.sum(),
                totalErrors.sum(),
                totalBytesTransferred.sum(),
                clientsCreated.get(),
                clientsClosed.get(),
                cacheHits.get(),
                cacheMisses.get(),
                activeHelpers.get(),
                copyMap(operationCounts),
                copyMap(operationErrors),
                copyMap(operationLatencies),
                copyMap(errorTypes));
    }

    private Map<String, Long> copyMap(Map<String, LongAdder> source) {
        Map<String, Long> result = new ConcurrentHashMap<>();
        source.forEach((key, adder) -> result.put(key, adder.sum()));
        return result;
    }

    /** Immutable snapshot of S3 metrics at a point in time. */
    public static class S3Metrics {
        private final long totalOperations;
        private final long totalErrors;
        private final long totalBytesTransferred;
        private final long clientsCreated;
        private final long clientsClosed;
        private final long cacheHits;
        private final long cacheMisses;
        private final long activeHelpers;
        private final Map<String, Long> operationCounts;
        private final Map<String, Long> operationErrors;
        private final Map<String, Long> operationLatencies;
        private final Map<String, Long> errorTypes;
        private final Instant timestamp;

        S3Metrics(
                long totalOperations,
                long totalErrors,
                long totalBytesTransferred,
                long clientsCreated,
                long clientsClosed,
                long cacheHits,
                long cacheMisses,
                long activeHelpers,
                Map<String, Long> operationCounts,
                Map<String, Long> operationErrors,
                Map<String, Long> operationLatencies,
                Map<String, Long> errorTypes) {
            this.totalOperations = totalOperations;
            this.totalErrors = totalErrors;
            this.totalBytesTransferred = totalBytesTransferred;
            this.clientsCreated = clientsCreated;
            this.clientsClosed = clientsClosed;
            this.cacheHits = cacheHits;
            this.cacheMisses = cacheMisses;
            this.activeHelpers = activeHelpers;
            this.operationCounts = Map.copyOf(operationCounts);
            this.operationErrors = Map.copyOf(operationErrors);
            this.operationLatencies = Map.copyOf(operationLatencies);
            this.errorTypes = Map.copyOf(errorTypes);
            this.timestamp = Instant.now();
        }

        // Getters
        public long getTotalOperations() {
            return totalOperations;
        }

        public long getTotalErrors() {
            return totalErrors;
        }

        public long getTotalBytesTransferred() {
            return totalBytesTransferred;
        }

        public long getClientsCreated() {
            return clientsCreated;
        }

        public long getClientsClosed() {
            return clientsClosed;
        }

        public long getCacheHits() {
            return cacheHits;
        }

        public long getCacheMisses() {
            return cacheMisses;
        }

        public long getActiveHelpers() {
            return activeHelpers;
        }

        public Map<String, Long> getOperationCounts() {
            return operationCounts;
        }

        public Map<String, Long> getOperationErrors() {
            return operationErrors;
        }

        public Map<String, Long> getOperationLatencies() {
            return operationLatencies;
        }

        public Map<String, Long> getErrorTypes() {
            return errorTypes;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        /** Calculates error rate as a percentage. */
        public double getErrorRate() {
            if (totalOperations == 0) {
                return 0.0;
            }
            return (double) totalErrors / totalOperations * 100.0;
        }

        /** Calculates cache hit rate as a percentage. */
        public double getCacheHitRate() {
            long totalCacheAccess = cacheHits + cacheMisses;
            if (totalCacheAccess == 0) {
                return 0.0;
            }
            return (double) cacheHits / totalCacheAccess * 100.0;
        }

        /** Gets average latency for a specific operation type. */
        public double getAverageLatency(String operationType) {
            Long count = operationCounts.get(operationType);
            Long totalLatency = operationLatencies.get(operationType);
            if (count == null || totalLatency == null || count == 0) {
                return 0.0;
            }
            return (double) totalLatency / count;
        }

        @Override
        public String toString() {
            return String.format(
                    "S3Metrics{operations=%d, errors=%d (%.2f%%), bytes=%d, "
                            + "clients=%d/%d, cache=%.2f%%, helpers=%d, timestamp=%s}",
                    totalOperations,
                    totalErrors,
                    getErrorRate(),
                    totalBytesTransferred,
                    clientsCreated,
                    clientsClosed,
                    getCacheHitRate(),
                    activeHelpers,
                    timestamp);
        }
    }
}
