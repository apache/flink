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

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
 * <p><b>Integration with Circuit Breaker:</b>
 *
 * <ul>
 *   <li>Health check failures increment the circuit breaker's failure count
 *   <li>Health check successes can help recover from HALF_OPEN state
 *   <li>Health checks are independent of inference requests
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class manages its own background thread for periodic checks and is
 * safe for concurrent access.
 *
 * @see TritonCircuitBreaker
 */
public class TritonHealthChecker implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TritonHealthChecker.class);

    private final String endpoint;
    private final OkHttpClient httpClient;
    private final TritonCircuitBreaker circuitBreaker;
    private final Duration checkInterval;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean lastCheckResult = new AtomicBoolean(true);
    private final AtomicLong lastCheckTime = new AtomicLong(0);
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private final AtomicLong consecutiveSuccesses = new AtomicLong(0);

    private static final String HEALTH_LIVE_PATH = "/v2/health/live";
    private static final String HEALTH_READY_PATH = "/v2/health/ready";
    private static final int HEALTH_CHECK_TIMEOUT_MS = 5000;

    /**
     * Creates a new health checker.
     *
     * @param endpoint The Triton server base URL
     * @param httpClient The HTTP client to use for health checks
     * @param circuitBreaker The circuit breaker to notify about health status
     * @param checkInterval How often to perform health checks
     */
    public TritonHealthChecker(
            String endpoint,
            OkHttpClient httpClient,
            TritonCircuitBreaker circuitBreaker,
            Duration checkInterval) {
        this.endpoint = endpoint;
        this.httpClient = httpClient;
        this.circuitBreaker = circuitBreaker;
        this.checkInterval = checkInterval;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "triton-health-checker-" + endpoint);
            thread.setDaemon(true);
            return thread;
        });

        LOG.info(
                "Health checker created for endpoint {} with interval {}",
                endpoint,
                checkInterval);
    }

    /**
     * Starts the periodic health checking.
     *
     * <p>Health checks will run at the configured interval on a background thread.
     */
    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            LOG.info("Starting health checker for {}", endpoint);
            scheduler.scheduleAtFixedRate(
                    this::performHealthCheck,
                    0, // Initial delay
                    checkInterval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stops the health checker and releases resources.
     *
     * <p>This method is idempotent and safe to call multiple times.
     */
    @Override
    public void close() {
        if (isRunning.compareAndSet(true, false)) {
            LOG.info("Stopping health checker for {}", endpoint);
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
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
        try {
            boolean healthy = checkServerHealth();
            lastCheckTime.set(System.currentTimeMillis());
            lastCheckResult.set(healthy);

            if (healthy) {
                consecutiveFailures.set(0);
                long successes = consecutiveSuccesses.incrementAndGet();
                LOG.debug("Health check passed for {} (consecutive: {})", endpoint, successes);

                // Notify circuit breaker of success
                // This can help transition from HALF_OPEN to CLOSED
                if (circuitBreaker.getState() == TritonCircuitBreaker.State.HALF_OPEN) {
                    circuitBreaker.recordSuccess();
                }
            } else {
                consecutiveSuccesses.set(0);
                long failures = consecutiveFailures.incrementAndGet();
                LOG.warn("Health check failed for {} (consecutive: {})", endpoint, failures);

                // Notify circuit breaker of failure
                circuitBreaker.recordFailure();
            }
        } catch (Exception e) {
            LOG.error("Error during health check for " + endpoint, e);
            consecutiveSuccesses.set(0);
            consecutiveFailures.incrementAndGet();
            lastCheckResult.set(false);
            circuitBreaker.recordFailure();
        }
    }

    /**
     * Checks the Triton server health by calling its health endpoints.
     *
     * <p>This method first tries the /v2/health/live endpoint, then falls back to
     * /v2/health/ready if needed.
     *
     * @return true if server is healthy, false otherwise
     */
    private boolean checkServerHealth() {
        // Try liveness endpoint first
        if (checkEndpoint(HEALTH_LIVE_PATH)) {
            return true;
        }

        // Fallback to readiness endpoint
        LOG.debug("Liveness check failed, trying readiness check for {}", endpoint);
        return checkEndpoint(HEALTH_READY_PATH);
    }

    /**
     * Checks a specific health endpoint.
     *
     * @param path The health endpoint path (e.g., "/v2/health/live")
     * @return true if endpoint responds with 200 OK, false otherwise
     */
    private boolean checkEndpoint(String path) {
        String healthUrl = buildHealthUrl(path);

        Request request = new Request.Builder()
                .url(healthUrl)
                .get()
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            boolean isHealthy = response.isSuccessful();

            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Health check {} for {}: status={}, body={}",
                        path,
                        endpoint,
                        response.code(),
                        response.body() != null ? response.body().string() : "null");
            }

            return isHealthy;
        } catch (IOException e) {
            LOG.debug("Health check {} failed for {}: {}", path, endpoint, e.getMessage());
            return false;
        }
    }

    /**
     * Builds the full URL for a health endpoint.
     *
     * @param path The health endpoint path
     * @return The complete health check URL
     */
    private String buildHealthUrl(String path) {
        String baseUrl = endpoint.replaceAll("/*$", "");

        // Remove any existing path components
        if (baseUrl.contains("/v2")) {
            baseUrl = baseUrl.substring(0, baseUrl.indexOf("/v2"));
        }

        return baseUrl + path;
    }

    /**
     * Performs an immediate synchronous health check.
     *
     * <p>This can be used for on-demand health verification, for example during initialization.
     *
     * @return true if server is healthy, false otherwise
     */
    public boolean checkNow() {
        try {
            boolean healthy = checkServerHealth();
            lastCheckTime.set(System.currentTimeMillis());
            lastCheckResult.set(healthy);
            return healthy;
        } catch (Exception e) {
            LOG.error("Error during immediate health check for " + endpoint, e);
            lastCheckResult.set(false);
            return false;
        }
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
