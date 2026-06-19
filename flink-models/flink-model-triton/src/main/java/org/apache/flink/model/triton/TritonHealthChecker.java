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

import org.apache.flink.annotation.VisibleForTesting;

import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Health checker for Triton Inference Server.
 *
 * <p>This component periodically polls the Triton server's health endpoint to verify the server is
 * running and can accept requests. It works in conjunction with {@link TritonCircuitBreaker} to
 * provide comprehensive fault tolerance.
 *
 * <p><b>Health Check Endpoints:</b>
 *
 * <ul>
 *   <li><b>/v2/health/live</b>: Checks if server is alive (primary endpoint)
 *   <li><b>/v2/health/ready</b>: Checks if server is ready to accept requests (fallback)
 * </ul>
 *
 * <p><b>Fallback semantics:</b> The readiness endpoint is only tried when the liveness endpoint
 * responded with an HTTP error (i.e. the server is reachable but reports not-live). On a network
 * error (connection refused, DNS failure, timeout) we do <i>not</i> fall back, because a second
 * failing call would only double the time spent per health-check cycle without improving diagnostic
 * value.
 *
 * <p><b>Dedicated HTTP client:</b> Health checks are performed on an HTTP client derived from the
 * inference client but configured with short, independent timeouts. This isolates the health
 * checker from long inference timeouts that would otherwise cause health-check cycles to overlap or
 * stall.
 *
 * <p><b>Integration with Circuit Breaker:</b>
 *
 * <ul>
 *   <li>The circuit breaker is optional. When {@code null}, health status is only logged and no
 *       circuit breaker state is maintained.
 *   <li>When a circuit breaker is provided, health-check results are forwarded
 *       <b>symmetrically</b>: both successes and failures are reported so the breaker's
 *       CLOSED-state failure rate is not skewed by only counting failures.
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class manages its own background thread for periodic checks and is
 * safe for concurrent access.
 *
 * @see TritonCircuitBreaker
 */
public class TritonHealthChecker implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TritonHealthChecker.class);

    /**
     * Short, independent timeout for health-check HTTP calls.
     *
     * <p>Health probes should fail fast so that {@link #checkInterval} is not starved by long
     * inference timeouts on the shared {@link OkHttpClient}. A 2-second budget is generous for a
     * healthy Triton server (whose health endpoints are local and in-memory) yet short enough that
     * two sequential probes (liveness + fallback) still fit well within any realistic check
     * interval.
     */
    private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofSeconds(2);

    private final String endpoint;

    /** Sanitised endpoint used for log output; strips any user-info (credentials). */
    private final String loggedEndpoint;

    /** Dedicated HTTP client with short timeouts, used exclusively for health probes. */
    private final OkHttpClient healthHttpClient;

    @Nullable private final TritonCircuitBreaker circuitBreaker;
    private final Duration checkInterval;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean lastCheckResult = new AtomicBoolean(true);
    private final AtomicLong lastCheckTime = new AtomicLong(0);
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private final AtomicLong consecutiveSuccesses = new AtomicLong(0);

    private static final String HEALTH_LIVE_PATH = "/v2/health/live";
    private static final String HEALTH_READY_PATH = "/v2/health/ready";

    /**
     * Outcome categories for a single endpoint probe. Distinguishing {@link #UNHEALTHY_RESPONSE}
     * (server answered with a non-2xx) from {@link #IO_ERROR} (network-level failure) lets the
     * server-health check decide whether a readiness fallback makes sense.
     */
    private enum CheckResult {
        /** Endpoint returned 2xx. */
        HEALTHY,
        /** Endpoint returned a non-2xx HTTP response. */
        UNHEALTHY_RESPONSE,
        /** Network-level failure (connection refused, timeout, DNS, TLS, ...). */
        IO_ERROR
    }

    /**
     * Creates a new health checker.
     *
     * @param endpoint The Triton server base URL
     * @param httpClient The inference HTTP client; a derived client with short timeouts is used
     *     internally for probes so that the inference client's long timeouts do not block health
     *     checks
     * @param circuitBreaker The circuit breaker to notify about health status, or {@code null} to
     *     disable circuit-breaker integration (status will only be logged)
     * @param checkInterval How often to perform health checks
     */
    public TritonHealthChecker(
            String endpoint,
            OkHttpClient httpClient,
            @Nullable TritonCircuitBreaker circuitBreaker,
            Duration checkInterval) {
        this.endpoint = endpoint;
        this.loggedEndpoint = sanitizeEndpoint(endpoint);
        // IMPORTANT: OkHttpClient#newBuilder() shares the Dispatcher and ConnectionPool with the
        // source client by default. If we kept that sharing, a later call to
        // {@code healthHttpClient.dispatcher().cancelAll()} in {@link #close()} would cancel
        // every in-flight INFERENCE call as well - exactly the opposite of what the health
        // checker should be doing. Installing a dedicated {@link Dispatcher} isolates the
        // lifecycle of probe calls from inference calls; the {@link okhttp3.ConnectionPool}
        // can still be shared safely because {@code cancelAll()} only affects the dispatcher
        // that owns the running calls.
        this.healthHttpClient =
                httpClient
                        .newBuilder()
                        .dispatcher(new Dispatcher())
                        .callTimeout(HEALTH_CHECK_TIMEOUT)
                        .connectTimeout(HEALTH_CHECK_TIMEOUT)
                        .readTimeout(HEALTH_CHECK_TIMEOUT)
                        .writeTimeout(HEALTH_CHECK_TIMEOUT)
                        // Disable OkHttp's own retry-on-failure so the short timeout bound is
                        // respected (otherwise a single probe may internally retry and exceed it).
                        .retryOnConnectionFailure(false)
                        .build();
        this.circuitBreaker = circuitBreaker;
        this.checkInterval = checkInterval;
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread =
                                    new Thread(r, "triton-health-checker-" + loggedEndpoint);
                            thread.setDaemon(true);
                            return thread;
                        });

        LOG.info(
                "Health checker created for endpoint {} with interval {} (circuit breaker {})",
                loggedEndpoint,
                checkInterval,
                circuitBreaker != null ? "enabled" : "disabled");
    }

    /**
     * Starts the periodic health checking.
     *
     * <p>Health checks will run at the configured interval on a background thread. The first
     * scheduled check fires one full interval after {@code start()} is called, leaving callers free
     * to perform an eager {@link #checkNow()} on initialisation without producing an immediate
     * duplicate check.
     */
    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            LOG.info("Starting health checker for {}", loggedEndpoint);
            long intervalMillis = checkInterval.toMillis();
            scheduler.scheduleAtFixedRate(
                    this::performHealthCheck,
                    // Initial delay equals the check interval to avoid duplicating an eager
                    // checkNow() performed by the caller immediately before start().
                    intervalMillis,
                    intervalMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stops the health checker and releases resources.
     *
     * <p>This method is idempotent and safe to call multiple times. Any in-flight health probe on
     * the dedicated client is cancelled so shutdown cannot hang on a slow server response.
     */
    @Override
    public void close() {
        if (isRunning.compareAndSet(true, false)) {
            LOG.info("Stopping health checker for {}", loggedEndpoint);
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
            // Cancel any probe that is still in flight on the dedicated health-check client.
            // Because we installed a private {@link Dispatcher} in the constructor, this only
            // affects probe calls - inference calls on the shared inference client are NOT
            // cancelled.
            //
            // Note: health probes use synchronous {@link okhttp3.Call#execute()}, which runs on
            // the calling thread rather than on the dispatcher's executor. Shutting down that
            // executor here is therefore primarily a hygiene step to release any lazily-created
            // cached thread pool deterministically, rather than a functional fix for a leak
            // (OkHttp's default cached pool would itself idle out after 60s).
            try {
                Dispatcher healthDispatcher = healthHttpClient.dispatcher();
                healthDispatcher.cancelAll();
                healthDispatcher.executorService().shutdown();
            } catch (RuntimeException e) {
                LOG.debug(
                        "Ignoring error while cancelling in-flight health probes for {}: {}",
                        loggedEndpoint,
                        e.getMessage());
            }
        }
    }

    /**
     * Performs a single health check.
     *
     * <p>This method is called periodically by the scheduler and should not be called directly in
     * normal operation.
     */
    private void performHealthCheck() {
        boolean healthy;
        try {
            healthy = checkServerHealth();
        } catch (RuntimeException e) {
            // checkServerHealth() only invokes HTTP calls that catch IOException internally;
            // anything propagating here is an unexpected runtime condition. We deliberately do
            // NOT catch Throwable (OOM / StackOverflow should surface), and InterruptedException
            // is a checked exception that cannot reach us.
            LOG.error("Unexpected runtime error during health check for {}", loggedEndpoint, e);
            healthy = false;
        }

        recordCheckOutcome(healthy);
        notifyCircuitBreaker(healthy);
    }

    /**
     * Updates {@link #lastCheckTime}, {@link #lastCheckResult} and consecutive-run counters after a
     * probe. Shared by the periodic {@link #performHealthCheck()} and the synchronous {@link
     * #checkNow()} paths to keep their accounting identical.
     */
    private void recordCheckOutcome(boolean healthy) {
        lastCheckTime.set(System.currentTimeMillis());
        lastCheckResult.set(healthy);

        if (healthy) {
            consecutiveFailures.set(0);
            long successes = consecutiveSuccesses.incrementAndGet();
            LOG.debug("Health check passed for {} (consecutive: {})", loggedEndpoint, successes);
        } else {
            consecutiveSuccesses.set(0);
            long failures = consecutiveFailures.incrementAndGet();
            LOG.warn("Health check failed for {} (consecutive: {})", loggedEndpoint, failures);
        }
    }

    /**
     * Forwards a health-check outcome to the circuit breaker, if configured.
     *
     * <p>Delegates to {@link TritonCircuitBreaker#recordHealthProbe(boolean)} so that the breaker
     * can apply the right state-dependent semantics:
     *
     * <ul>
     *   <li>In CLOSED, both successes and failures feed the failure-rate computation.
     *   <li>In HALF_OPEN, probe successes are ignored (otherwise the breaker could close on
     *       health-endpoint 200s without any real inference traffic being exercised) while probe
     *       failures immediately reopen the breaker.
     *   <li>In OPEN, probes are informational only.
     * </ul>
     */
    private void notifyCircuitBreaker(boolean healthy) {
        if (circuitBreaker == null) {
            return;
        }
        circuitBreaker.recordHealthProbe(healthy);
    }

    /**
     * Checks the Triton server health by calling its health endpoints.
     *
     * <p>Try the liveness endpoint first. Fall back to readiness <i>only</i> when liveness returned
     * an HTTP response that indicates "reachable but not live" &mdash; a network-level failure
     * means a second probe would very likely incur another full timeout without adding signal.
     *
     * @return true if server is healthy, false otherwise
     */
    private boolean checkServerHealth() {
        CheckResult liveResult = checkEndpointDetailed(HEALTH_LIVE_PATH);
        if (liveResult == CheckResult.HEALTHY) {
            return true;
        }
        if (liveResult == CheckResult.IO_ERROR) {
            // Avoid doubling the per-cycle cost on network errors.
            return false;
        }

        LOG.debug("Liveness responded unhealthy for {}, trying readiness check", loggedEndpoint);
        return checkEndpointDetailed(HEALTH_READY_PATH) == CheckResult.HEALTHY;
    }

    /**
     * Probes a single health endpoint and returns a detailed outcome.
     *
     * @param path The health endpoint path (e.g., "/v2/health/live")
     * @return one of {@link CheckResult#HEALTHY}, {@link CheckResult#UNHEALTHY_RESPONSE} or {@link
     *     CheckResult#IO_ERROR}
     */
    private CheckResult checkEndpointDetailed(String path) {
        String healthUrl = buildHealthUrl(path);

        Request request = new Request.Builder().url(healthUrl).get().build();

        try (Response response = healthHttpClient.newCall(request).execute()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Health check {} for {}: status={}, body={}",
                        path,
                        loggedEndpoint,
                        response.code(),
                        response.body() != null ? response.body().string() : "null");
            }
            return response.isSuccessful() ? CheckResult.HEALTHY : CheckResult.UNHEALTHY_RESPONSE;
        } catch (IOException e) {
            LOG.debug(
                    "Health check {} failed for {} ({}): {}",
                    path,
                    loggedEndpoint,
                    e.getClass().getSimpleName(),
                    e.getMessage());
            return CheckResult.IO_ERROR;
        }
    }

    /**
     * Builds the full URL for a health endpoint.
     *
     * <p>The configured endpoint may already point at a Triton sub-path such as {@code
     * http://host:8000/v2/models/.../infer}. We strip any existing path while preserving scheme,
     * host, port and user-info, then append the health path. Using {@link URI} (rather than naive
     * substring matching on {@code "/v2"}) keeps hostnames that happen to contain the literal
     * {@code "/v2"} substring &mdash; e.g. {@code http://triton-v2.example.com:8000} &mdash;
     * intact.
     *
     * @param path The health endpoint path
     * @return The complete health check URL
     */
    @VisibleForTesting
    String buildHealthUrl(String path) {
        String trimmed = endpoint.replaceAll("/*$", "");
        try {
            URI uri = new URI(trimmed);
            // Only strip the path when the URI parses as an absolute URL with authority.
            if (uri.isAbsolute() && uri.getHost() != null) {
                URI base =
                        new URI(
                                uri.getScheme(),
                                uri.getUserInfo(),
                                uri.getHost(),
                                uri.getPort(),
                                path,
                                null,
                                null);
                return base.toString();
            }
        } catch (URISyntaxException e) {
            LOG.debug(
                    "Endpoint {} is not a valid URI ({}); falling back to string-based health URL construction.",
                    loggedEndpoint,
                    e.getMessage());
        }

        // Fallback: conservative path-segment match. Only strip "/v2/..." or a trailing "/v2",
        // never a host-level substring like "triton-v2.example.com".
        String baseUrl = trimmed;
        int idx = baseUrl.indexOf("/v2/");
        if (idx >= 0) {
            baseUrl = baseUrl.substring(0, idx);
        } else if (baseUrl.endsWith("/v2")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 3);
        }
        return baseUrl + path;
    }

    /**
     * Returns an endpoint representation safe to log: any {@code user:password@} prefix in the
     * authority is stripped so credentials cannot leak to INFO/WARN logs.
     *
     * <p>Delegates to {@link TritonUtils#sanitizeUrl(String)} so that every Triton component shares
     * a single, tested redaction implementation; kept here as a thin {@link VisibleForTesting}
     * wrapper to preserve the existing test entry-point.
     */
    @VisibleForTesting
    static String sanitizeEndpoint(String endpoint) {
        return TritonUtils.sanitizeUrl(endpoint);
    }

    /**
     * Performs an immediate synchronous health check.
     *
     * <p><b>INTENTIONALLY BYPASSES THE CIRCUIT BREAKER.</b> Unlike {@link #performHealthCheck()},
     * this method does <i>not</i> call {@link TritonCircuitBreaker#recordHealthProbe(boolean)},
     * because its designed use case is a one-shot probe during initialisation - folding that single
     * synchronous sample into the circuit-breaker's failure-rate or half-open budget would bias
     * both the CLOSED-state rate computation and the HALF_OPEN probe accounting in ways callers
     * almost never want.
     *
     * <p><b>Do not</b> invoke this method from a recurring schedule as a substitute for {@link
     * #start()}; the periodic path is the only one that keeps the breaker in sync with server
     * health over time. The intended callers are (a) {@link AbstractTritonModelFunction#open} for
     * the initial probe and (b) tests. Monitoring dashboards should read {@link #isHealthy()}
     * rather than calling this method repeatedly.
     *
     * @return true if server is healthy, false otherwise
     */
    public boolean checkNow() {
        boolean healthy;
        try {
            healthy = checkServerHealth();
        } catch (RuntimeException e) {
            LOG.error(
                    "Unexpected runtime error during immediate health check for {}",
                    loggedEndpoint,
                    e);
            healthy = false;
        }
        // Keep lastCheckTime/lastCheckResult in sync with the periodic path so monitors observe
        // a consistent timestamp regardless of which code path produced the latest result.
        recordCheckOutcome(healthy);
        return healthy;
    }

    // Getters for monitoring

    public boolean isHealthy() {
        return lastCheckResult.get();
    }

    public long getLastCheckTime() {
        return lastCheckTime.get();
    }

    public long getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    public long getConsecutiveSuccesses() {
        return consecutiveSuccesses.get();
    }

    public boolean isRunning() {
        return isRunning.get();
    }
}
