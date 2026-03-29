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

package org.apache.flink.model.triton;

import org.apache.flink.model.triton.exception.TritonCircuitBreakerOpenException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Circuit breaker implementation for Triton Inference Server health management.
 *
 * <p>This circuit breaker follows the classic three-state model to protect the system from
 * cascading failures when the Triton server becomes unhealthy:
 *
 * <ul>
 *   <li><b>CLOSED</b>: Normal operation. Requests are allowed. Tracks failure rate.
 *   <li><b>OPEN</b>: Triton is unhealthy. All requests fail fast without hitting the server.
 *   <li><b>HALF_OPEN</b>: Testing recovery. Limited requests allowed to probe server health.
 * </ul>
 *
 * <p><b>State Transitions:</b>
 *
 * <pre>
 *  CLOSED ──[failure rate > threshold]──> OPEN
 *            ↑                              │
 *            │                              │ [after timeout]
 *            │                              ↓
 *            └──[success count met]── HALF_OPEN
 * </pre>
 *
 * <p><b>Benefits:</b>
 *
 * <ul>
 *   <li>Fail fast when server is down, avoiding wasted retries
 *   <li>Automatic recovery detection
 *   <li>Reduced load on failing servers (prevents cascading failure)
 *   <li>Improved system resilience
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe and designed for concurrent access from
 * multiple Flink task threads.
 *
 * @see TritonHealthChecker
 */
public class TritonCircuitBreaker {
    private static final Logger LOG = LoggerFactory.getLogger(TritonCircuitBreaker.class);

    /** Current state of the circuit breaker. */
    public enum State {
        /** Normal operation, requests allowed, tracking failures. */
        CLOSED,
        /** Server unhealthy, failing fast without hitting server. */
        OPEN,
        /** Testing recovery with limited requests. */
        HALF_OPEN
    }

    private final String endpoint;
    private final double failureThreshold;
    private final Duration openStateDuration;
    private final int halfOpenMaxRequests;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicLong lastStateTransitionTime = new AtomicLong(System.currentTimeMillis());

    // Metrics for CLOSED state
    private final AtomicInteger totalRequests = new AtomicInteger(0);
    private final AtomicInteger failedRequests = new AtomicInteger(0);

    // Metrics for HALF_OPEN state
    private final AtomicInteger halfOpenSuccesses = new AtomicInteger(0);
    private final AtomicInteger halfOpenFailures = new AtomicInteger(0);
    private final AtomicInteger halfOpenRequests = new AtomicInteger(0);

    /**
     * Minimum number of requests before evaluating failure rate. This prevents opening the circuit
     * based on too few samples.
     */
    private static final int MIN_REQUESTS_THRESHOLD = 10;

    /**
     * Maximum number of requests to track in CLOSED state before resetting counters. This prevents
     * historical successes from diluting current failure rate.
     */
    private static final int MAX_CLOSED_STATE_REQUESTS = 10000;

    /**
     * Creates a new circuit breaker for a Triton endpoint.
     *
     * @param endpoint The Triton server endpoint URL
     * @param failureThreshold Failure rate (0.0-1.0) that triggers circuit opening
     * @param openStateDuration How long to stay OPEN before transitioning to HALF_OPEN
     * @param halfOpenMaxRequests Number of successful test requests needed in HALF_OPEN to close
     */
    public TritonCircuitBreaker(
            String endpoint,
            double failureThreshold,
            Duration openStateDuration,
            int halfOpenMaxRequests) {
        this.endpoint = endpoint;
        if (failureThreshold <= 0.0 || failureThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "failureThreshold must be in range (0.0, 1.0], got: " + failureThreshold);
        }
        this.failureThreshold = failureThreshold;
        this.openStateDuration = openStateDuration;
        if (halfOpenMaxRequests <= 0) {
            throw new IllegalArgumentException(
                    "halfOpenMaxRequests must be positive, got: " + halfOpenMaxRequests);
        }
        this.halfOpenMaxRequests = halfOpenMaxRequests;

        LOG.info(
                "Circuit breaker created for endpoint {} with threshold={}, openDuration={}, halfOpenRequests={}",
                endpoint,
                failureThreshold,
                openStateDuration,
                halfOpenMaxRequests);
    }

    /**
     * Checks if a request is allowed through the circuit breaker.
     *
     * @return true if request should proceed, false if should fail fast
     * @throws TritonCircuitBreakerOpenException if circuit is OPEN
     */
    public boolean isRequestAllowed() throws TritonCircuitBreakerOpenException {
        State currentState = state.get();

        switch (currentState) {
            case CLOSED:
                return true;

            case OPEN:
                // Check if it's time to transition to HALF_OPEN
                if (shouldTransitionToHalfOpen()) {
                    LOG.info(
                            "Circuit breaker transitioning from OPEN to HALF_OPEN for {}",
                            endpoint);
                    if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        lastStateTransitionTime.set(System.currentTimeMillis());
                        resetHalfOpenMetrics();
                        // Count this first request as a half-open probe request
                        halfOpenRequests.incrementAndGet();
                        return true;
                    }
                    // CAS failed, another thread transitioned state
                }
                // fall through
                throw new TritonCircuitBreakerOpenException(
                        String.format(
                                "Circuit breaker is OPEN for endpoint %s. "
                                        + "Server is considered unhealthy. Will retry in %d seconds.",
                                endpoint, getRemainingOpenTimeSeconds()));

            case HALF_OPEN:
                // Allow limited number of requests in HALF_OPEN state using CAS loop
                while (true) {
                    int current = halfOpenRequests.get();
                    if (current >= halfOpenMaxRequests) {
                        throw new TritonCircuitBreakerOpenException(
                                String.format(
                                        "Circuit breaker is HALF_OPEN for endpoint %s. "
                                                + "Maximum test requests (%d) reached. Please retry later.",
                                        endpoint, halfOpenMaxRequests));
                    }
                    if (halfOpenRequests.compareAndSet(current, current + 1)) {
                        LOG.debug(
                                "Allowing request {}/{} in HALF_OPEN state for {}",
                                current + 1,
                                halfOpenMaxRequests,
                                endpoint);
                        return true;
                    }
                }
            // fall through

            default:
                return true;
        }
    }

    /**
     * Records a successful request.
     *
     * <p>In CLOSED state, this updates success metrics. In HALF_OPEN state, this may trigger
     * transition back to CLOSED if enough successful probes complete.
     */
    public void recordSuccess() {
        State currentState = state.get();

        switch (currentState) {
            case CLOSED:
                int total = totalRequests.incrementAndGet();

                // Reset counters periodically to prevent historical successes from diluting
                // current failure rate
                if (total >= MAX_CLOSED_STATE_REQUESTS) {
                    resetClosedMetrics();
                }

                // Also check if we should open the circuit after recording a success
                // This handles the case where the minimum threshold is reached on a success
                if (shouldOpenCircuit()) {
                    if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                        LOG.warn(
                                "Circuit breaker opening for {} due to high failure rate: {}/{}",
                                endpoint,
                                failedRequests.get(),
                                totalRequests.get());
                        lastStateTransitionTime.set(System.currentTimeMillis());
                    }
                }
                break;

            case HALF_OPEN:
                int successes = halfOpenSuccesses.incrementAndGet();
                LOG.debug(
                        "Circuit breaker recorded success {}/{} in HALF_OPEN for {}",
                        successes,
                        halfOpenMaxRequests,
                        endpoint);

                if (successes >= halfOpenMaxRequests) {
                    // Enough successful probes, close the circuit
                    if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                        LOG.info(
                                "Circuit breaker transitioning from HALF_OPEN to CLOSED for {} "
                                        + "after {} successful probes",
                                endpoint,
                                successes);
                        lastStateTransitionTime.set(System.currentTimeMillis());
                        resetClosedMetrics();
                    }
                }
                break;

            case OPEN:
                // Shouldn't happen, but log if it does
                LOG.warn("Recorded success while circuit breaker is OPEN for {}", endpoint);
                break;
        }
    }

    /**
     * Records a failed request.
     *
     * <p>In CLOSED state, this may trigger transition to OPEN if failure rate exceeds threshold. In
     * HALF_OPEN state, any failure immediately reopens the circuit.
     */
    public void recordFailure() {
        State currentState = state.get();

        switch (currentState) {
            case CLOSED:
                int total = totalRequests.incrementAndGet();
                failedRequests.incrementAndGet();

                // Reset counters periodically to prevent historical successes from diluting
                // current failure rate
                if (total >= MAX_CLOSED_STATE_REQUESTS) {
                    resetClosedMetrics();
                }

                // Check if we should open the circuit
                if (shouldOpenCircuit()) {
                    if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                        LOG.warn(
                                "Circuit breaker opening for {} due to high failure rate: {}/{}",
                                endpoint,
                                failedRequests.get(),
                                totalRequests.get());
                        lastStateTransitionTime.set(System.currentTimeMillis());
                    }
                }
                break;

            case HALF_OPEN:
                halfOpenFailures.incrementAndGet();
                // Any failure in HALF_OPEN immediately reopens the circuit
                if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                    LOG.warn(
                            "Circuit breaker reopening for {} due to failure in HALF_OPEN state",
                            endpoint);
                    lastStateTransitionTime.set(System.currentTimeMillis());
                }
                break;

            case OPEN:
                // Already open, nothing to do
                break;
        }
    }

    /**
     * Manually resets the circuit breaker to CLOSED state.
     *
     * <p>This is useful for administrative recovery or testing.
     */
    public void reset() {
        State oldState = state.getAndSet(State.CLOSED);
        if (oldState != State.CLOSED) {
            LOG.info("Circuit breaker manually reset to CLOSED for {}", endpoint);
            lastStateTransitionTime.set(System.currentTimeMillis());
            resetClosedMetrics();
        }
    }

    /** Checks if the circuit should open based on failure rate. */
    private boolean shouldOpenCircuit() {
        int total = totalRequests.get();
        int failed = failedRequests.get();

        // Need minimum number of requests to make a decision
        if (total < MIN_REQUESTS_THRESHOLD) {
            return false;
        }

        double failureRate = (double) failed / total;
        return failureRate >= failureThreshold;
    }

    /** Checks if enough time has passed to transition from OPEN to HALF_OPEN. */
    private boolean shouldTransitionToHalfOpen() {
        long elapsed = System.currentTimeMillis() - lastStateTransitionTime.get();
        return elapsed >= openStateDuration.toMillis();
    }

    /** Gets remaining time in OPEN state (for error messages). */
    private long getRemainingOpenTimeSeconds() {
        long elapsed = System.currentTimeMillis() - lastStateTransitionTime.get();
        long remaining = openStateDuration.toMillis() - elapsed;
        return Math.max(0, remaining / 1000);
    }

    /** Resets metrics for CLOSED state. */
    private void resetClosedMetrics() {
        totalRequests.set(0);
        failedRequests.set(0);
    }

    /** Resets metrics for HALF_OPEN state. */
    private void resetHalfOpenMetrics() {
        halfOpenSuccesses.set(0);
        halfOpenFailures.set(0);
        halfOpenRequests.set(0);
    }

    // Getters for monitoring and testing

    public State getState() {
        return state.get();
    }

    public String getEndpoint() {
        return endpoint;
    }

    public int getTotalRequests() {
        return totalRequests.get();
    }

    public int getFailedRequests() {
        return failedRequests.get();
    }

    public double getCurrentFailureRate() {
        int total = totalRequests.get();
        if (total == 0) {
            return 0.0;
        }
        return (double) failedRequests.get() / total;
    }

    public long getTimeInCurrentState() {
        return System.currentTimeMillis() - lastStateTransitionTime.get();
    }
}
