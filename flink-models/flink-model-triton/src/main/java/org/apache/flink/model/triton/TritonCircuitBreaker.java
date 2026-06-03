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
import java.util.concurrent.TimeUnit;
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

    /**
     * Cached sanitized view of {@link #endpoint} used for all log lines and user-visible exception
     * messages. The raw endpoint may contain basic-auth credentials (e.g. {@code
     * http://user:password@host:8000}); {@link TritonUtils#sanitizeUrl(String)} strips the userInfo
     * component so those credentials cannot leak into INFO/WARN logs or into the message of {@link
     * TritonCircuitBreakerOpenException} propagated to user pipelines.
     */
    private final String loggedEndpoint;

    private final double failureThreshold;
    private final Duration openStateDuration;
    private final int halfOpenMaxRequests;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);

    /**
     * Timestamp of the last state transition, captured from {@link System#nanoTime()}.
     *
     * <p>We deliberately use {@code nanoTime} (a monotonic clock) rather than {@code
     * currentTimeMillis} (wall-clock) so that the OPEN -> HALF_OPEN timeout is not affected by NTP
     * adjustments or manual clock changes. A backwards wall-clock jump would otherwise make the
     * breaker believe its timeout has regressed (leaving it OPEN indefinitely); a forward jump
     * would spuriously trigger an early probe.
     *
     * <p>The value is only meaningful when compared against another {@code nanoTime()} reading
     * taken on the same JVM process; it is never serialised or exposed to external clients.
     */
    private final AtomicLong lastStateTransitionNanos = new AtomicLong(System.nanoTime());

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
     * Maximum number of requests to track in CLOSED state before decaying the counters. Decay
     * (halving the counters - see {@link #incrementClosedMetrics(boolean)}) prevents historical
     * successes from diluting the current failure rate while still preserving the current rate at
     * the decay boundary.
     */
    private static final int MAX_CLOSED_STATE_REQUESTS = 10000;

    /**
     * Bounded number of retries when re-evaluating circuit breaker state after losing a CAS race.
     * In practice one or two iterations are always sufficient; this bound merely guards against
     * pathological contention.
     */
    private static final int MAX_STATE_EVAL_ATTEMPTS = 4;

    /**
     * Lock used to make CLOSED-state metric resets atomic with respect to readers.
     *
     * <p>Using a dedicated lock (rather than synchronizing on {@code this}) avoids accidentally
     * blocking unrelated operations.
     */
    private final Object closedMetricsLock = new Object();

    /**
     * Lock that serialises the OPEN -> HALF_OPEN transition so that {@link #resetHalfOpenMetrics()}
     * cannot race with concurrent probe increments done by {@link #allowHalfOpenRequest()}.
     *
     * <p>Without this lock, two threads both observing OPEN would each call {@code
     * resetHalfOpenMetrics()} and then CAS; the second reset could wipe probe counts that another
     * thread had already recorded after the first CAS succeeded.
     */
    private final Object halfOpenTransitionLock = new Object();

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
        this.loggedEndpoint = TritonUtils.sanitizeUrl(endpoint);
        if (failureThreshold <= 0.0 || failureThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "failureThreshold must be in range (0.0, 1.0], got: " + failureThreshold);
        }
        this.failureThreshold = failureThreshold;
        // openStateDuration governs the mandatory OPEN -> HALF_OPEN cool-off. A null or
        // non-positive value would either NPE at every state check (null) or make the breaker
        // immediately re-admit traffic while the server is still being considered down
        // (non-positive), which defeats the purpose of the OPEN state. Reject both eagerly so
        // misconfiguration fails fast at operator construction time, not at first failure.
        if (openStateDuration == null) {
            throw new IllegalArgumentException("openStateDuration must not be null");
        }
        if (openStateDuration.isZero() || openStateDuration.isNegative()) {
            throw new IllegalArgumentException(
                    "openStateDuration must be strictly positive, got: " + openStateDuration);
        }
        this.openStateDuration = openStateDuration;
        if (halfOpenMaxRequests <= 0) {
            throw new IllegalArgumentException(
                    "halfOpenMaxRequests must be positive, got: " + halfOpenMaxRequests);
        }
        this.halfOpenMaxRequests = halfOpenMaxRequests;

        LOG.info(
                "Circuit breaker created for endpoint {} with threshold={}, openDuration={}, halfOpenRequests={}",
                loggedEndpoint,
                failureThreshold,
                openStateDuration,
                halfOpenMaxRequests);
    }

    /**
     * Checks if a request is allowed through the circuit breaker.
     *
     * <p>This method either allows the request to proceed (returning {@code true}) or fails fast by
     * throwing {@link TritonCircuitBreakerOpenException}. It never returns {@code false}; the
     * boolean return type is preserved for API stability and to read naturally at call sites.
     *
     * @return {@code true} when the request is permitted
     * @throws TritonCircuitBreakerOpenException if circuit is OPEN, or if HALF_OPEN has already
     *     dispatched its maximum probe requests
     */
    public boolean isRequestAllowed() throws TritonCircuitBreakerOpenException {
        // Use a bounded retry loop so that if the state transitions during our evaluation
        // (e.g. OPEN -> HALF_OPEN CAS loses to another thread), we re-evaluate against the
        // new state instead of incorrectly rejecting a request that the new state would allow.
        for (int attempt = 0; attempt < MAX_STATE_EVAL_ATTEMPTS; attempt++) {
            State currentState = state.get();

            switch (currentState) {
                case CLOSED:
                    return true;

                case OPEN:
                    // Check if it's time to transition to HALF_OPEN
                    if (shouldTransitionToHalfOpen()) {
                        // Serialise the reset + CAS under a dedicated lock. This prevents a
                        // second thread from resetting the HALF_OPEN counters after another
                        // thread has already transitioned and started granting probes.
                        synchronized (halfOpenTransitionLock) {
                            // Re-check the state under the lock: another thread may have
                            // already completed the transition while we were blocked.
                            if (state.get() == State.OPEN) {
                                resetHalfOpenMetrics();
                                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                                    lastStateTransitionNanos.set(System.nanoTime());
                                    LOG.info(
                                            "Circuit breaker transitioning from OPEN to HALF_OPEN for {}",
                                            loggedEndpoint);
                                }
                            }
                        }
                        // Fall through the outer loop so this request competes for a probe
                        // slot via allowHalfOpenRequest(), ensuring the budget is respected
                        // regardless of which thread performed the actual transition.
                        continue;
                    }
                    throw new TritonCircuitBreakerOpenException(
                            String.format(
                                    "Circuit breaker is OPEN for endpoint %s. "
                                            + "Server is considered unhealthy. Will retry in %d seconds.",
                                    loggedEndpoint, getRemainingOpenTimeSeconds()));

                case HALF_OPEN:
                    // Allow limited number of requests in HALF_OPEN state using CAS loop.
                    // The inner loop can only exit via {@code return} or {@code throw}, so no
                    // fall-through to {@code default} is possible.
                    return allowHalfOpenRequest();

                default:
                    // Unreachable in practice: all enum values are handled above.
                    throw new IllegalStateException(
                            "Unexpected circuit breaker state: " + currentState);
            }
        }

        // Safety net: if the state kept flipping under heavy contention, fail fast with
        // the OPEN message rather than looping indefinitely.
        throw new TritonCircuitBreakerOpenException(
                String.format(
                        "Circuit breaker state evaluation exceeded %d attempts for endpoint %s.",
                        MAX_STATE_EVAL_ATTEMPTS, loggedEndpoint));
    }

    /**
     * Acquires a slot for a probe request while in HALF_OPEN state.
     *
     * <p>Extracted to a helper so that the caller's {@code switch} can simply {@code return} the
     * result, avoiding any appearance of fall-through to the {@code default} branch.
     */
    private boolean allowHalfOpenRequest() throws TritonCircuitBreakerOpenException {
        while (true) {
            int current = halfOpenRequests.get();
            if (current >= halfOpenMaxRequests) {
                throw new TritonCircuitBreakerOpenException(
                        String.format(
                                "Circuit breaker is HALF_OPEN for endpoint %s. "
                                        + "Maximum test requests (%d) reached. Please retry later.",
                                loggedEndpoint, halfOpenMaxRequests));
            }
            if (halfOpenRequests.compareAndSet(current, current + 1)) {
                LOG.debug(
                        "Allowing request {}/{} in HALF_OPEN state for {}",
                        current + 1,
                        halfOpenMaxRequests,
                        loggedEndpoint);
                return true;
            }
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
                {
                    incrementClosedMetrics(false);

                    // A pure success can only lower the failure rate. However, when the very
                    // first sample that pushes total over MIN_REQUESTS_THRESHOLD happens to be
                    // a success (e.g. 9 prior failures followed by one success => total=10,
                    // failed=9, rate 0.9), this is the earliest point at which
                    // shouldOpenCircuit() can legitimately fire. We therefore still evaluate it
                    // here; the cost is a single atomic read per success.
                    //
                    // Snapshot ordering: we read the counters AFTER incrementClosedMetrics()
                    // returns (so this sample is folded in) and BEFORE the CLOSED -> OPEN CAS.
                    // Two consequences matter for readers of this code:
                    //   1. The snapshot reflects the state that the breaker is deciding on,
                    //      including the current call's contribution. snapshotClosedMetrics()
                    //      reads both counters under closedMetricsLock so the pair is
                    //      internally consistent even if another thread is concurrently
                    //      incrementing or halving the counters.
                    //   2. Logging and the shouldOpenCircuit() decision use the SAME snapshot,
                    //      so the "X/Y failure rate" line always matches the values the
                    //      decision was actually made on - even though a concurrent thread may
                    //      have advanced the counters by the time the log line is formatted.
                    //      This is intentional: a readable, reproducible log is more useful
                    //      than a re-read that would race with other writers anyway.
                    Snapshot triggeringSnap = snapshotClosedMetrics();
                    if (shouldOpenCircuit(triggeringSnap)) {
                        if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                            LOG.warn(
                                    "Circuit breaker opening for {} due to high failure rate: {}/{}",
                                    loggedEndpoint,
                                    triggeringSnap.failed,
                                    triggeringSnap.total);
                            lastStateTransitionNanos.set(System.nanoTime());
                        }
                    }
                    break;
                }

            case HALF_OPEN:
                {
                    int successes = halfOpenSuccesses.incrementAndGet();
                    LOG.debug(
                            "Circuit breaker recorded success {}/{} in HALF_OPEN for {}",
                            successes,
                            halfOpenMaxRequests,
                            loggedEndpoint);

                    if (successes >= halfOpenMaxRequests) {
                        // Enough successful probes, close the circuit
                        if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                            LOG.info(
                                    "Circuit breaker transitioning from HALF_OPEN to CLOSED for {} "
                                            + "after {} successful probes",
                                    loggedEndpoint,
                                    successes);
                            lastStateTransitionNanos.set(System.nanoTime());
                            resetClosedMetrics();
                        } else {
                            // CAS failed: state changed under us (e.g. recorded failure reopened
                            // the circuit). Log so the probe isn't silently lost from metrics view.
                            LOG.debug(
                                    "HALF_OPEN -> CLOSED transition lost race for {}; state is now {}",
                                    loggedEndpoint,
                                    state.get());
                        }
                    }
                    break;
                }

            case OPEN:
                // Shouldn't happen, but log if it does
                LOG.warn("Recorded success while circuit breaker is OPEN for {}", loggedEndpoint);
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
                {
                    incrementClosedMetrics(true);

                    // Snapshot ordering: taken AFTER the increment (so this failure is included
                    // in the denominator / numerator) and BEFORE the CLOSED -> OPEN CAS. See the
                    // matching comment in recordSuccess() for the full reasoning. Short version:
                    // the snapshot read is guarded by the same lock as the increment, so the
                    // {total, failed} pair is internally consistent, and the log line below
                    // reports the exact numbers that drove the decision.
                    Snapshot triggeringSnap = snapshotClosedMetrics();
                    // Check if we should open the circuit
                    if (shouldOpenCircuit(triggeringSnap)) {
                        if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                            LOG.warn(
                                    "Circuit breaker opening for {} due to high failure rate: {}/{}",
                                    loggedEndpoint,
                                    triggeringSnap.failed,
                                    triggeringSnap.total);
                            lastStateTransitionNanos.set(System.nanoTime());
                        }
                    }
                    break;
                }

            case HALF_OPEN:
                // Any failure in HALF_OPEN immediately reopens the circuit. Only bump the
                // failure counter when the CAS succeeds: if the CAS loses, the state has
                // already transitioned (typically another thread already moved HALF_OPEN ->
                // OPEN), and incrementing halfOpenFailures here would otherwise pollute the
                // next HALF_OPEN cycle's monitoring view between its OPEN->HALF_OPEN reset
                // and the first real probe.
                if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                    halfOpenFailures.incrementAndGet();
                    LOG.warn(
                            "Circuit breaker reopening for {} due to failure in HALF_OPEN state",
                            loggedEndpoint);
                    lastStateTransitionNanos.set(System.nanoTime());
                } else {
                    // CAS failed: state already transitioned (e.g. another thread reset).
                    // Log so the failure isn't silently dropped from diagnostic view.
                    LOG.debug(
                            "HALF_OPEN -> OPEN transition lost race for {}; state is now {}",
                            loggedEndpoint,
                            state.get());
                }
                break;

            case OPEN:
                // Already open, nothing to do
                break;
        }
    }

    /**
     * Atomically increments CLOSED-state counters, performing the periodic decay under the same
     * lock so that concurrent increments and decays cannot drop samples.
     *
     * <p>When {@code totalRequests} reaches {@link #MAX_CLOSED_STATE_REQUESTS}, we halve both
     * counters (integer division) rather than zeroing them. Halving preserves the current failure
     * <i>rate</i> while capping the sample count, so:
     *
     * <ul>
     *   <li>A burst of failures that drove the rate near the threshold is not silently wiped just
     *       because the cap was reached. Previously, a hard reset could force the breaker to wait
     *       for another {@link #MIN_REQUESTS_THRESHOLD} samples before it could ever trip, hiding
     *       an ongoing incident.
     *   <li>Failure-rate computations remain well-defined the entire time (we only halve when
     *       {@code total} has already reached the cap, so {@code total} after decay is still {@code
     *       >= MIN_REQUESTS_THRESHOLD} for any realistic cap and threshold values).
     *   <li>Memory footprint stays bounded exactly as before.
     * </ul>
     *
     * <p>This is a cheap approximation of an exponentially-weighted moving average: each decay
     * halves the influence of older samples without requiring us to remember them individually.
     *
     * @param isFailure whether the recorded outcome was a failure
     */
    private void incrementClosedMetrics(boolean isFailure) {
        synchronized (closedMetricsLock) {
            int newTotal = totalRequests.incrementAndGet();
            if (isFailure) {
                failedRequests.incrementAndGet();
            }
            // Decay counters when the cap is hit. See Javadoc for why halving is preferred
            // over zeroing. Done inside the same lock as the increment so no sample crosses
            // the decay boundary.
            if (newTotal >= MAX_CLOSED_STATE_REQUESTS) {
                int decayedTotal = totalRequests.get() / 2;
                int decayedFailed = failedRequests.get() / 2;
                totalRequests.set(decayedTotal);
                failedRequests.set(decayedFailed);
            }
        }
    }

    /**
     * Records the outcome of an out-of-band health probe (e.g. from {@link TritonHealthChecker}).
     *
     * <p>This is intentionally distinct from {@link #recordSuccess()}/{@link #recordFailure()},
     * which are reserved for callers that actually went through {@link #isRequestAllowed()}:
     *
     * <ul>
     *   <li>In {@link State#CLOSED} the probe result is folded into the failure-rate computation
     *       just like a real inference call: using a consistent denominator prevents the rate from
     *       being skewed by counting only failures.
     *   <li>In {@link State#HALF_OPEN} a successful probe is <b>ignored</b>: the probe budget is
     *       reserved for real inference traffic, otherwise the breaker could close purely on the
     *       strength of health-endpoint 200s without ever exercising the inference path. A failing
     *       probe, however, is treated as a signal to reopen immediately - if the server cannot
     *       even answer /v2/health/live, it is clearly not ready to serve inference.
     *   <li>In {@link State#OPEN} the probe result is ignored: the scheduled transition back to
     *       HALF_OPEN is driven exclusively by {@link #openStateDuration}.
     * </ul>
     *
     * <p>Implementation note: we deliberately do <b>not</b> delegate to {@link #recordSuccess()}/
     * {@link #recordFailure()} after observing a CLOSED state. Doing so would introduce a TOCTOU
     * race - the state could flip to HALF_OPEN between our read and the delegated method's own
     * {@code state.get()}, causing a health-probe success to silently consume a probe slot in
     * HALF_OPEN (which is exactly what this method is designed to avoid). Instead, we perform the
     * CLOSED-state accounting inline via {@link #recordClosedProbeOutcome(boolean)}, which is a
     * no-op when the state is no longer CLOSED.
     *
     * @param healthy whether the health probe reported the server as healthy
     */
    public void recordHealthProbe(boolean healthy) {
        State currentState = state.get();
        switch (currentState) {
            case CLOSED:
                recordClosedProbeOutcome(healthy);
                break;
            case HALF_OPEN:
                // Only failing probes can affect state in HALF_OPEN; successful probes must not
                // consume or progress the probe budget that belongs to real inference traffic.
                // A failing probe reopens the breaker directly, without going through
                // recordFailure() (which would branch on a possibly stale state re-read).
                if (!healthy) {
                    reopenFromHalfOpen();
                }
                break;
            case OPEN:
                // Timing of the OPEN -> HALF_OPEN transition is driven by the configured timeout
                // alone; health probes are informational only.
                break;
        }
    }

    /**
     * Performs CLOSED-state accounting for a health-probe outcome without going through {@link
     * #recordSuccess()}/{@link #recordFailure()}.
     *
     * <p>Each counter mutation is guarded by a CAS on the state so that if the breaker transitions
     * out of CLOSED between our caller's state read and our own, the sample is dropped rather than
     * leaking into HALF_OPEN probe counters or into a reset-boundary CLOSED counter set.
     */
    private void recordClosedProbeOutcome(boolean healthy) {
        // Only account the sample if the state is still CLOSED. If another thread moved us to
        // OPEN/HALF_OPEN in the meantime, the sample is simply dropped - probes are best-effort
        // signals, not load-bearing inputs to the state machine in those states.
        if (state.get() != State.CLOSED) {
            return;
        }
        incrementClosedMetrics(!healthy);
        // Re-check state after the increment: the CLOSED -> OPEN transition is only evaluated
        // when we are still CLOSED, using the same pre-CAS snapshot used for the log line.
        if (state.get() != State.CLOSED) {
            return;
        }
        Snapshot triggeringSnap = snapshotClosedMetrics();
        if (shouldOpenCircuit(triggeringSnap)) {
            if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                LOG.warn(
                        "Circuit breaker opening for {} due to high failure rate: {}/{} "
                                + "(triggered by health probe)",
                        loggedEndpoint,
                        triggeringSnap.failed,
                        triggeringSnap.total);
                lastStateTransitionNanos.set(System.nanoTime());
            }
        }
    }

    /**
     * Attempts to reopen the breaker from HALF_OPEN after a failing health probe.
     *
     * <p>Mirrors the HALF_OPEN branch of {@link #recordFailure()} but does <i>not</i> call that
     * method, so we avoid re-reading the state and accidentally taking the CLOSED or OPEN branches
     * instead (which would at best drop the signal and at worst pollute CLOSED counters).
     */
    private void reopenFromHalfOpen() {
        if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
            halfOpenFailures.incrementAndGet();
            LOG.warn(
                    "Circuit breaker reopening for {} due to failing health probe in HALF_OPEN",
                    loggedEndpoint);
            lastStateTransitionNanos.set(System.nanoTime());
        } else {
            LOG.debug(
                    "Health probe failure in HALF_OPEN for {} lost race; state is now {}",
                    loggedEndpoint,
                    state.get());
        }
    }

    /**
     * Manually resets the circuit breaker to CLOSED state.
     *
     * <p>This is useful for administrative recovery or testing. All counters (both CLOSED and
     * HALF_OPEN) are cleared so that monitoring does not observe stale values from the previous
     * state.
     */
    public void reset() {
        State oldState = state.getAndSet(State.CLOSED);
        if (oldState != State.CLOSED) {
            LOG.info("Circuit breaker manually reset to CLOSED for {}", loggedEndpoint);
            lastStateTransitionNanos.set(System.nanoTime());
            resetClosedMetrics();
            resetHalfOpenMetrics();
        }
    }

    /**
     * Checks if the circuit should open based on the given counter snapshot.
     *
     * <p>Callers that also want to log the triggering counts should pass the same snapshot to the
     * log statement so the decision and its diagnostic output are perfectly consistent.
     *
     * <p>Every production caller takes its own snapshot (typically right after mutating the
     * counters via {@link #incrementClosedMetrics(boolean)}) and therefore always routes through
     * this single overload. A previous no-arg variant that re-read the counters internally was
     * removed as dead code - keeping it would have duplicated the snapshot logic and risked the log
     * line and the decision being based on different reads.
     */
    private boolean shouldOpenCircuit(Snapshot snap) {
        // Need minimum number of requests to make a decision
        if (snap.total < MIN_REQUESTS_THRESHOLD) {
            return false;
        }

        double failureRate = (double) snap.failed / snap.total;
        return failureRate >= failureThreshold;
    }

    /**
     * Returns a consistent snapshot of CLOSED-state counters.
     *
     * <p>Callers should prefer this over two independent {@code .get()} calls to avoid observing a
     * reset that happens between them.
     */
    private Snapshot snapshotClosedMetrics() {
        synchronized (closedMetricsLock) {
            return new Snapshot(totalRequests.get(), failedRequests.get());
        }
    }

    /** Immutable pair of CLOSED-state counter values. */
    private static final class Snapshot {
        final int total;
        final int failed;

        Snapshot(int total, int failed) {
            this.total = total;
            this.failed = failed;
        }
    }

    /**
     * Checks if enough time has passed to transition from OPEN to HALF_OPEN.
     *
     * <p>Uses {@link System#nanoTime()} so that NTP adjustments cannot make the breaker either (a)
     * skip its cool-off because wall-clock jumped forward, or (b) remain OPEN forever because
     * wall-clock jumped backward. {@code nanoTime()} deltas are the only guarantee-monotonic way to
     * measure elapsed time on the JVM.
     */
    private boolean shouldTransitionToHalfOpen() {
        long elapsedNanos = System.nanoTime() - lastStateTransitionNanos.get();
        return elapsedNanos >= openStateDuration.toNanos();
    }

    /** Gets remaining time in OPEN state (for error messages). */
    private long getRemainingOpenTimeSeconds() {
        long elapsedNanos = System.nanoTime() - lastStateTransitionNanos.get();
        long remainingNanos = openStateDuration.toNanos() - elapsedNanos;
        return Math.max(0L, TimeUnit.NANOSECONDS.toSeconds(remainingNanos));
    }

    /** Resets metrics for CLOSED state atomically. */
    private void resetClosedMetrics() {
        // Reset both counters under a common lock so that no reader can observe
        // {@code totalRequests == 0 && failedRequests > 0} during the reset.
        // shouldOpenCircuit() and getCurrentFailureRate() also acquire this lock
        // when reading the pair.
        synchronized (closedMetricsLock) {
            totalRequests.set(0);
            failedRequests.set(0);
        }
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
        Snapshot snap = snapshotClosedMetrics();
        if (snap.total == 0) {
            return 0.0;
        }
        return (double) snap.failed / snap.total;
    }

    public long getTimeInCurrentState() {
        long elapsedNanos = System.nanoTime() - lastStateTransitionNanos.get();
        return TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
    }
}
