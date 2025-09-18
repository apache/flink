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

import org.apache.hadoop.fs.s3a.S3AUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Advanced error handling framework for S3 operations with circuit breaker pattern, intelligent
 * retry logic, and comprehensive error classification.
 */
@Internal
public class S3ErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(S3ErrorHandler.class);

    // Circuit breaker configuration
    private static final int FAILURE_THRESHOLD = 10;
    private static final Duration CIRCUIT_OPEN_DURATION = Duration.ofMinutes(1);
    private static final int HALF_OPEN_MAX_CALLS = 3;

    // Retry configuration
    private static final int MAX_RETRIES = 3;
    private static final Duration BASE_RETRY_DELAY = Duration.ofMillis(500);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(30);

    // Circuit breakers per operation type
    private final ConcurrentHashMap<String, CircuitBreaker> circuitBreakers =
            new ConcurrentHashMap<>();

    // Error classification
    private final ErrorClassifier errorClassifier = new ErrorClassifier();

    /** Executes an operation with comprehensive error handling, retries, and circuit breaking. */
    public <T> T executeWithErrorHandling(
            String operationType, String context, Callable<T> operation) throws IOException {
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker(operationType);

        // Check circuit breaker state
        if (!circuitBreaker.allowCall()) {
            throw new IOException("Circuit breaker is OPEN for operation: " + operationType);
        }

        Exception lastException = null;
        long startTime = System.nanoTime();

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                T result = operation.call();

                // Record success
                circuitBreaker.recordSuccess();
                S3MetricsManager.getInstance()
                        .recordOperationSuccess(
                                operationType,
                                Duration.ofNanos(System.nanoTime() - startTime),
                                0 // bytes unknown here
                                );

                return result;

            } catch (Exception e) {
                lastException = e;

                ErrorClassification classification = errorClassifier.classify(e);

                // Record error in circuit breaker
                circuitBreaker.recordFailure();

                // Record metrics
                S3MetricsManager.getInstance().recordOperationError(operationType, e);

                // Check if we should retry
                if (attempt == MAX_RETRIES - 1 || !classification.isRetryable()) {
                    break;
                }

                // Calculate retry delay with exponential backoff and jitter
                Duration retryDelay = calculateRetryDelay(attempt, classification);

                LOG.debug(
                        "Operation {} failed on attempt {} ({}), retrying in {}ms. Error: {}",
                        operationType,
                        attempt + 1,
                        context,
                        retryDelay.toMillis(),
                        e.getClass().getSimpleName());

                try {
                    Thread.sleep(retryDelay.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Retry interrupted", ie);
                }
            }
        }

        // All retries exhausted, translate and throw the final exception
        throw translateException(operationType, context, lastException);
    }

    private CircuitBreaker getOrCreateCircuitBreaker(String operationType) {
        return circuitBreakers.computeIfAbsent(
                operationType,
                k ->
                        new CircuitBreaker(
                                FAILURE_THRESHOLD, CIRCUIT_OPEN_DURATION, HALF_OPEN_MAX_CALLS));
    }

    private Duration calculateRetryDelay(int attempt, ErrorClassification classification) {
        // Base exponential backoff
        long baseDelayMs = BASE_RETRY_DELAY.toMillis() * (1L << attempt);

        // Apply jitter (±25%)
        double jitterFactor = 0.75 + (Math.random() * 0.5);
        long delayWithJitter = (long) (baseDelayMs * jitterFactor);

        // Apply error-specific multiplier
        delayWithJitter = (long) (delayWithJitter * classification.getRetryDelayMultiplier());

        // Cap at maximum
        return Duration.ofMillis(Math.min(delayWithJitter, MAX_RETRY_DELAY.toMillis()));
    }

    private IOException translateException(
            String operationType, String context, Exception exception) {
        try {
            if (exception instanceof SdkException) {
                return S3AUtils.translateException(
                        operationType, context, (SdkException) exception);
            } else if (exception instanceof IOException) {
                return (IOException) exception;
            } else {
                return new IOException(
                        "S3 operation failed: " + operationType + " (" + context + ")", exception);
            }
        } catch (Exception e) {
            // Fallback if S3AUtils translation fails
            return new IOException(
                    "S3 operation failed: "
                            + operationType
                            + " ("
                            + context
                            + "): "
                            + exception.getMessage(),
                    exception);
        }
    }

    /** Circuit breaker implementation for preventing cascading failures. */
    private static class CircuitBreaker {
        private enum State {
            CLOSED,
            OPEN,
            HALF_OPEN
        }

        private final int failureThreshold;
        private final Duration openDuration;
        private final int halfOpenMaxCalls;

        private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private final AtomicInteger halfOpenCallCount = new AtomicInteger(0);
        private volatile Instant lastFailureTime = Instant.MIN;

        CircuitBreaker(int failureThreshold, Duration openDuration, int halfOpenMaxCalls) {
            this.failureThreshold = failureThreshold;
            this.openDuration = openDuration;
            this.halfOpenMaxCalls = halfOpenMaxCalls;
        }

        boolean allowCall() {
            State currentState = state.get();

            switch (currentState) {
                case CLOSED:
                    return true;

                case OPEN:
                    if (Instant.now().isAfter(lastFailureTime.plus(openDuration))) {
                        // Transition to half-open
                        if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                            halfOpenCallCount.set(0);
                            LOG.info("Circuit breaker transitioning from OPEN to HALF_OPEN");
                        }
                        return halfOpenCallCount.incrementAndGet() <= halfOpenMaxCalls;
                    }
                    return false;

                case HALF_OPEN:
                    return halfOpenCallCount.incrementAndGet() <= halfOpenMaxCalls;

                default:
                    return false;
            }
        }

        void recordSuccess() {
            State currentState = state.get();

            if (currentState == State.HALF_OPEN) {
                // Transition back to closed
                if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                    failureCount.set(0);
                    LOG.info("Circuit breaker transitioning from HALF_OPEN to CLOSED");
                }
            } else if (currentState == State.CLOSED) {
                // Reset failure count on success
                failureCount.set(0);
            }
        }

        void recordFailure() {
            lastFailureTime = Instant.now();

            State currentState = state.get();

            if (currentState == State.HALF_OPEN) {
                // Go back to open
                if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                    LOG.warn("Circuit breaker transitioning from HALF_OPEN to OPEN due to failure");
                }
            } else if (currentState == State.CLOSED) {
                int failures = failureCount.incrementAndGet();
                if (failures >= failureThreshold) {
                    if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                        LOG.warn(
                                "Circuit breaker transitioning from CLOSED to OPEN due to {} failures",
                                failures);
                    }
                }
            }
        }
    }

    /** Classifies errors for appropriate handling strategies. */
    private static class ErrorClassifier {

        ErrorClassification classify(Exception exception) {
            if (exception instanceof NoSuchUploadException) {
                return new ErrorClassification(false, 1.0, "Upload ID invalid or expired");
            }

            if (exception instanceof S3Exception) {
                S3Exception s3Exception = (S3Exception) exception;
                int statusCode = s3Exception.statusCode();

                if (statusCode >= 500 && statusCode < 600) {
                    // Server errors - retryable with longer delay
                    return new ErrorClassification(true, 2.0, "Server error");
                } else if (statusCode == 429) {
                    // Rate limiting - retryable with much longer delay
                    return new ErrorClassification(true, 5.0, "Rate limited");
                } else if (statusCode >= 400 && statusCode < 500) {
                    // Client errors - generally not retryable
                    return new ErrorClassification(false, 1.0, "Client error");
                }
            }

            if (exception instanceof SdkException) {
                String message = exception.getMessage().toLowerCase();
                if (message.contains("timeout") || message.contains("connection")) {
                    return new ErrorClassification(true, 1.5, "Network/timeout error");
                }
            }

            // Default: not retryable
            return new ErrorClassification(false, 1.0, "Unknown error");
        }
    }

    /** Classification result for an error. */
    private static class ErrorClassification {
        private final boolean retryable;
        private final double retryDelayMultiplier;
        private final String category;

        ErrorClassification(boolean retryable, double retryDelayMultiplier, String category) {
            this.retryable = retryable;
            this.retryDelayMultiplier = retryDelayMultiplier;
            this.category = category;
        }

        boolean isRetryable() {
            return retryable;
        }

        double getRetryDelayMultiplier() {
            return retryDelayMultiplier;
        }

        String getCategory() {
            return category;
        }
    }
}
