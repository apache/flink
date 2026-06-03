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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Preconditions;

import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Abstract parent class for {@link AsyncPredictFunction}s for Triton Inference Server API.
 *
 * <p>This implementation uses REST-based HTTP communication with Triton Inference Server. Each
 * Flink record triggers a separate HTTP request (no Flink-side batching). Triton's server-side
 * dynamic batching can aggregate concurrent requests.
 *
 * <p><b>HTTP Client Lifecycle:</b> A shared HTTP client pool is maintained per JVM with reference
 * counting. Multiple function instances with identical timeout settings share the same client
 * instance to avoid resource exhaustion in high-parallelism scenarios.
 *
 * <p><b>Current Limitations (v1):</b>
 *
 * <ul>
 *   <li>Only single input column and single output column are supported
 *   <li>REST API only; gRPC may be introduced in future versions
 * </ul>
 *
 * <p><b>Future Roadmap:</b> Support for multi-input/multi-output models using ROW or MAP types, and
 * native gRPC protocol for improved performance.
 */
public abstract class AbstractTritonModelFunction extends AsyncPredictFunction {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTritonModelFunction.class);

    protected transient OkHttpClient httpClient;
    protected transient TritonCircuitBreaker circuitBreaker;
    protected transient TritonHealthChecker healthChecker;

    /**
     * Scheduler used to delay retry attempts with exponential backoff. Owned by this function so
     * that pending retries are cancelled on {@link #close()} — relying on {@link
     * java.util.concurrent.CompletableFuture#delayedExecutor(long, TimeUnit)} (which is backed by
     * {@link java.util.concurrent.ForkJoinPool#commonPool()}) would leak scheduled tasks past the
     * operator lifecycle and risk firing HTTP calls against an already-released client.
     */
    protected transient ScheduledExecutorService retryScheduler;

    private final String endpoint;

    /**
     * Cached sanitized view of {@link #endpoint} used exclusively for log and error-message
     * formatting. The raw {@link #endpoint} (which may carry {@code user:password@host}
     * credentials) is still required for HTTP calls and for constructing request URLs, but must
     * never be echoed to logs, metrics or exception messages, as those can end up in CI artifacts
     * or user-facing dashboards. Computed once in the constructor so we don't pay the URI parsing
     * cost on every log line.
     */
    private final String loggedEndpoint;

    private final String modelName;
    private final String modelVersion;
    private final Duration timeout;
    private final boolean flattenBatchDim;
    private final Integer priority;

    /**
     * Sequence ID used by Triton to correlate multiple inference requests that belong to the same
     * stateful sequence (e.g. RNN or streaming models).
     *
     * <p>See Triton Inference Server sequence batching documentation:
     * https://github.com/triton-inference-server/server/blob/main/docs/sequence_batcher.md
     */
    private final String sequenceId;

    private final boolean sequenceStart;
    private final boolean sequenceEnd;
    private final String compression;
    private final String authToken;
    private final Map<String, String> customHeaders;
    private final int maxRetries;
    private final Duration retryInitialBackoff;
    private final Duration retryMaxBackoff;
    private final String defaultValue;

    // Health check and circuit breaker configuration
    private final boolean healthCheckEnabled;
    private final Duration healthCheckInterval;
    private final boolean circuitBreakerEnabled;
    private final double circuitBreakerFailureThreshold;
    private final Duration circuitBreakerTimeout;
    private final int circuitBreakerHalfOpenRequests;

    public AbstractTritonModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        this.endpoint = config.get(TritonOptions.ENDPOINT);
        this.loggedEndpoint = TritonUtils.sanitizeUrl(this.endpoint);
        this.modelName = config.get(TritonOptions.MODEL_NAME);
        this.modelVersion = config.get(TritonOptions.MODEL_VERSION);
        this.timeout = config.get(TritonOptions.TIMEOUT);
        this.flattenBatchDim = config.get(TritonOptions.FLATTEN_BATCH_DIM);
        this.priority = config.get(TritonOptions.PRIORITY);
        this.sequenceId = config.get(TritonOptions.SEQUENCE_ID);
        this.sequenceStart = config.get(TritonOptions.SEQUENCE_START);
        this.sequenceEnd = config.get(TritonOptions.SEQUENCE_END);
        this.compression = config.get(TritonOptions.COMPRESSION);
        this.authToken = config.get(TritonOptions.AUTH_TOKEN);
        this.customHeaders = config.get(TritonOptions.CUSTOM_HEADERS);
        this.maxRetries = config.get(TritonOptions.MAX_RETRIES);
        this.retryInitialBackoff = config.get(TritonOptions.RETRY_INITIAL_BACKOFF);
        this.retryMaxBackoff = config.get(TritonOptions.RETRY_MAX_BACKOFF);
        this.defaultValue = config.get(TritonOptions.DEFAULT_VALUE);

        // Fail fast on invalid retry configuration. These are user-supplied values so we surface
        // misconfiguration as IllegalArgumentException before the function is ever opened,
        // instead of producing undefined behaviour (e.g. 1L << -1) at runtime.
        Preconditions.checkArgument(
                maxRetries >= 0,
                "%s must be >= 0, got %s",
                TritonOptions.MAX_RETRIES.key(),
                maxRetries);
        // Backoff durations only matter when retries are actually enabled. Validating them when
        // maxRetries == 0 would reject configurations that are otherwise meaningless (a user who
        // explicitly disables retries with max-retries=0 should not have to also keep the
        // backoff durations in a valid range, since they will never be consulted).
        if (maxRetries > 0) {
            Preconditions.checkArgument(
                    retryInitialBackoff != null
                            && !retryInitialBackoff.isNegative()
                            && !retryInitialBackoff.isZero(),
                    "%s must be a positive duration, got %s",
                    TritonOptions.RETRY_INITIAL_BACKOFF.key(),
                    retryInitialBackoff);
            Preconditions.checkArgument(
                    retryMaxBackoff != null
                            && !retryMaxBackoff.isNegative()
                            && !retryMaxBackoff.isZero()
                            && retryMaxBackoff.compareTo(retryInitialBackoff) >= 0,
                    "%s must be a positive duration >= %s (%s), got %s",
                    TritonOptions.RETRY_MAX_BACKOFF.key(),
                    TritonOptions.RETRY_INITIAL_BACKOFF.key(),
                    retryInitialBackoff,
                    retryMaxBackoff);
        }

        // Health check and circuit breaker configuration
        this.healthCheckEnabled = config.get(TritonOptions.HEALTH_CHECK_ENABLED);
        this.healthCheckInterval = config.get(TritonOptions.HEALTH_CHECK_INTERVAL);
        this.circuitBreakerEnabled = config.get(TritonOptions.CIRCUIT_BREAKER_ENABLED);
        this.circuitBreakerFailureThreshold =
                config.get(TritonOptions.CIRCUIT_BREAKER_FAILURE_THRESHOLD);
        this.circuitBreakerTimeout = config.get(TritonOptions.CIRCUIT_BREAKER_TIMEOUT);
        this.circuitBreakerHalfOpenRequests =
                config.get(TritonOptions.CIRCUIT_BREAKER_HALF_OPEN_REQUESTS);

        // Validate input schema - support multiple types
        validateInputSchema(factoryContext.getCatalogModel().getResolvedInputSchema());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.debug("Creating Triton HTTP client.");
        this.httpClient = TritonUtils.createHttpClient(timeout.toMillis());

        // Provision a private single-thread scheduler for delayed retries so that retry tasks are
        // bound to this operator's lifecycle. Previously CompletableFuture.delayedExecutor was
        // used, but it schedules on the shared ForkJoinPool.commonPool() whose tasks are not
        // cancellable from close() and would happily fire an HTTP call with an already-released
        // client. Only allocate when retries are actually enabled to avoid idle threads.
        if (maxRetries > 0) {
            // Single-thread executor → the thread name does not need an index suffix; a fixed
            // suffix would always be "-0" and carry no diagnostic value. The subtask index IS
            // included so that thread dumps from a parallelism>1 deployment can be attributed
            // to a specific subtask rather than aliasing every parallel instance under the same
            // "triton-retry-scheduler-<model>" name.
            final int subtaskIndex = context.getTaskInfo().getIndexOfThisSubtask();
            final String threadName = "triton-retry-scheduler-" + modelName + "-" + subtaskIndex;
            this.retryScheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, threadName);
                                t.setDaemon(true);
                                return t;
                            });
        }

        // Initialize circuit breaker if enabled
        if (circuitBreakerEnabled) {
            LOG.info(
                    "Initializing circuit breaker for endpoint {} with threshold={}, timeout={}, halfOpenRequests={}",
                    loggedEndpoint,
                    circuitBreakerFailureThreshold,
                    circuitBreakerTimeout,
                    circuitBreakerHalfOpenRequests);

            this.circuitBreaker =
                    new TritonCircuitBreaker(
                            endpoint,
                            circuitBreakerFailureThreshold,
                            circuitBreakerTimeout,
                            circuitBreakerHalfOpenRequests);
        }

        // Initialize health checker if enabled
        if (healthCheckEnabled) {
            if (circuitBreaker == null) {
                LOG.warn(
                        "Health check is enabled but circuit breaker is disabled for endpoint {}. "
                                + "Health check will run but failures will not trigger circuit breaking. "
                                + "Consider enabling circuit-breaker-enabled for better fault tolerance.",
                        loggedEndpoint);
            }

            LOG.info(
                    "Initializing health checker for endpoint {} with interval {}",
                    loggedEndpoint,
                    healthCheckInterval);

            // Pass the (possibly null) circuit breaker directly. The health checker treats a null
            // breaker as "log-only" mode rather than relying on a dummy instance whose state
            // nobody reads.
            this.healthChecker =
                    new TritonHealthChecker(
                            endpoint, httpClient, circuitBreaker, healthCheckInterval);

            // Perform an immediate health check
            boolean initialHealth = healthChecker.checkNow();
            if (initialHealth) {
                LOG.info("Initial health check passed for endpoint {}", loggedEndpoint);
            } else {
                LOG.warn(
                        "Initial health check failed for endpoint {}. "
                                + "Inference requests may fail until server becomes healthy.",
                        loggedEndpoint);
            }

            // Start periodic health checking. The scheduler uses an initial delay equal to
            // checkInterval so it does not duplicate the eager checkNow() above.
            healthChecker.start();
        }
    }

    @Override
    public void close() throws Exception {
        // Each cleanup step is isolated so that a failure in one does not prevent the others
        // from running. The HTTP client in particular must always be released back to the
        // reference-counted pool; leaking its reference across restarts would keep the shared
        // client alive forever.
        Exception firstFailure = null;

        try {
            super.close();
        } catch (Exception e) {
            firstFailure = e;
        }

        // Stop health checker first so it cannot race with the HTTP client teardown below.
        if (this.healthChecker != null) {
            LOG.debug("Stopping health checker for {}", loggedEndpoint);
            try {
                this.healthChecker.close();
            } catch (Exception e) {
                LOG.warn("Error closing health checker for " + loggedEndpoint, e);
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
            this.healthChecker = null;
        }

        // Release circuit breaker (no-op, just drop the reference).
        this.circuitBreaker = null;

        // Shut down the retry scheduler before releasing the HTTP client so that any pending
        // retry tasks are cancelled and cannot fire a request through a client that is about to
        // be released back to the shared pool. shutdownNow() is safe here: a cancelled retry
        // simply means the caller sees the original inference failure rather than a spurious
        // post-close one.
        if (this.retryScheduler != null) {
            LOG.debug("Shutting down Triton retry scheduler for {}", loggedEndpoint);
            try {
                this.retryScheduler.shutdownNow();
                // Best-effort await so in-flight retry tasks observe the interrupt before the
                // client is closed. A short timeout is fine because retries only schedule a
                // single short-lived Runnable.
                if (!this.retryScheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Triton retry scheduler did not terminate within 1s for {}",
                            loggedEndpoint);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            } finally {
                this.retryScheduler = null;
            }
        }

        // Release HTTP client last so it's always attempted even if earlier steps threw.
        if (this.httpClient != null) {
            LOG.debug("Releasing Triton HTTP client.");
            try {
                TritonUtils.releaseHttpClient(this.httpClient);
            } catch (Exception e) {
                LOG.warn("Error releasing Triton HTTP client for " + loggedEndpoint, e);
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            } finally {
                this.httpClient = null;
            }
        }

        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    /**
     * Validates the input schema. Subclasses can override for custom validation.
     *
     * @param schema The input schema to validate
     */
    protected void validateInputSchema(ResolvedSchema schema) {
        validateSingleColumnSchema(schema, null, "input");
    }

    /**
     * Validates that the schema has exactly one physical column, optionally checking the type.
     *
     * <p><b>Version 1 Limitation:</b> Only single input/single output models are supported. For
     * models requiring multiple tensors, consider these workarounds:
     *
     * <ul>
     *   <li>Flatten inputs into a JSON STRING and parse server-side
     *   <li>Use ARRAY&lt;T&gt; to pack multiple values
     *   <li>Wait for future ROW&lt;...&gt; support (planned for v2)
     * </ul>
     *
     * @param schema The schema to validate
     * @param expectedType The expected type, or null to skip type checking
     * @param inputOrOutput Description of whether this is input or output schema
     */
    protected void validateSingleColumnSchema(
            ResolvedSchema schema, LogicalType expectedType, String inputOrOutput) {
        List<Column> columns = schema.getColumns();
        Preconditions.checkArgument(
                columns.size() == 1,
                "Model should have exactly one %s column, but actually has %s columns: %s. "
                        + "Current version only supports single input/output. "
                        + "For multi-tensor models, consider using JSON STRING encoding or ARRAY<T> packing.",
                inputOrOutput,
                columns.size(),
                columns.stream().map(Column::getName).collect(Collectors.toList()));

        Column column = columns.get(0);
        Preconditions.checkArgument(
                column.isPhysical(),
                "%s column %s should be a physical column, but is a %s.",
                inputOrOutput,
                column.getName(),
                column.getClass());

        // The previous condition ({@code expectedType != null && !expectedType.equals(actual)})
        // inverted the {@link Preconditions#checkArgument} contract: it threw whenever
        // {@code expectedType} was null (the documented "skip type check" mode) and also whenever
        // the actual type matched the expected one. All current call sites pass {@code
        // expectedType = null}, so this bug would have rejected every schema had the code path
        // been exercised. The corrected form only enforces the equality check when a specific
        // expected type is supplied.
        if (expectedType != null) {
            LogicalType actualType = column.getDataType().getLogicalType();
            Preconditions.checkArgument(
                    expectedType.equals(actualType),
                    "%s column %s should be %s, but is %s.",
                    inputOrOutput,
                    column.getName(),
                    expectedType,
                    actualType);
        }

        // Validate that the type is supported by Triton
        try {
            TritonTypeMapper.toTritonDataType(column.getDataType().getLogicalType());
        } catch (IllegalArgumentException e) {
            String suggestedType = getSuggestedTypeForTriton(column.getDataType().getLogicalType());
            throw new IllegalArgumentException(
                    String.format(
                            "%s column %s has unsupported type %s for Triton. %s%s",
                            inputOrOutput,
                            column.getName(),
                            column.getDataType().getLogicalType(),
                            e.getMessage(),
                            suggestedType.isEmpty() ? "" : "\nSuggestion: " + suggestedType));
        }

        // Enhanced validation for type compatibility
        validateTritonTypeCompatibility(
                column.getDataType().getLogicalType(), column.getName(), inputOrOutput);
    }

    /**
     * Validates Triton type compatibility with enhanced checks.
     *
     * <p>This method performs additional validation beyond basic type support:
     *
     * <ul>
     *   <li>Checks for nested arrays (multi-dimensional tensors not supported in v1)
     *   <li>Warns about STRING to BYTES mapping
     *   <li>Provides structured error messages with troubleshooting hints
     * </ul>
     *
     * @param type The logical type to validate
     * @param columnName The name of the column
     * @param inputOrOutput Description of whether this is input or output
     */
    private void validateTritonTypeCompatibility(
            LogicalType type, String columnName, String inputOrOutput) {

        // Check for nested arrays (multi-dimensional tensors)
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            LogicalType elementType = arrayType.getElementType();

            // Reject nested arrays
            Preconditions.checkArgument(
                    !(elementType instanceof ArrayType),
                    "%s column '%s' has nested array type: %s\n"
                            + "Multi-dimensional tensors (ARRAY<ARRAY<T>>) are not supported in v1.\n"
                            + "=== Supported Types ===\n"
                            + "  - Scalars: INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, STRING\n"
                            + "  - 1-D Arrays: ARRAY<INT>, ARRAY<FLOAT>, ARRAY<DOUBLE>, etc.\n"
                            + "=== Workarounds ===\n"
                            + "  - Flatten to 1-D array: ARRAY<FLOAT> with size = rows * cols\n"
                            + "  - Use JSON STRING encoding for complex structures\n"
                            + "  - Wait for v2+ which will support ROW<...> types",
                    inputOrOutput,
                    columnName,
                    type);
        }

        // Log info about STRING to BYTES mapping
        if (type instanceof VarCharType) {
            LOG.info(
                    "{} column '{}' uses STRING type, which will be mapped to Triton BYTES dtype. "
                            + "Ensure your Triton model expects string/text inputs.",
                    inputOrOutput,
                    columnName);
        }
    }

    /** Provides user-friendly type suggestions for unsupported types. */
    private String getSuggestedTypeForTriton(LogicalType unsupportedType) {
        String typeName = unsupportedType.getTypeRoot().name();

        if (typeName.contains("ARRAY") && unsupportedType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) unsupportedType;
            if (arrayType.getElementType() instanceof ArrayType) {
                return "Flatten nested array to 1-D: ARRAY<FLOAT> instead of ARRAY<ARRAY<FLOAT>>";
            }
        }

        if (typeName.contains("MAP")) {
            return "Use ARRAY<T> instead of MAP, or serialize to JSON STRING";
        } else if (typeName.contains("ROW") || typeName.contains("STRUCT")) {
            return "Flatten ROW into single column, use ARRAY<T> packing, or serialize to JSON STRING";
        } else if (typeName.contains("TIME") || typeName.contains("DATE")) {
            return "Convert timestamp/date to BIGINT (epoch milliseconds) or STRING (ISO-8601)";
        } else if (typeName.contains("DECIMAL")) {
            return "Use DOUBLE for numeric precision or STRING for exact decimal representation";
        } else if (typeName.contains("BINARY") || typeName.contains("VARBINARY")) {
            return "Consider using STRING (VARCHAR) type, which maps to Triton BYTES";
        }

        return "";
    }

    // Getters for configuration values
    protected String getEndpoint() {
        return endpoint;
    }

    protected String getModelName() {
        return modelName;
    }

    protected String getModelVersion() {
        return modelVersion;
    }

    protected Duration getTimeout() {
        return timeout;
    }

    protected boolean isFlattenBatchDim() {
        return flattenBatchDim;
    }

    protected Integer getPriority() {
        return priority;
    }

    protected String getSequenceId() {
        return sequenceId;
    }

    protected boolean isSequenceStart() {
        return sequenceStart;
    }

    protected boolean isSequenceEnd() {
        return sequenceEnd;
    }

    protected String getCompression() {
        return compression;
    }

    protected String getAuthToken() {
        return authToken;
    }

    protected Map<String, String> getCustomHeaders() {
        return customHeaders;
    }

    /**
     * Checks if a request is allowed through the circuit breaker.
     *
     * <p>Subclasses should call this method before making inference requests. If the circuit
     * breaker is OPEN, this method will throw an exception to fail fast.
     *
     * <p>The nullness of {@link #circuitBreaker} is the single source of truth: it is set in {@link
     * #open(FunctionContext)} only when circuit breaking is enabled, so an explicit {@code
     * circuitBreakerEnabled} check here would be redundant (and could drift out of sync with the
     * field).
     *
     * @throws org.apache.flink.model.triton.exception.TritonCircuitBreakerOpenException if circuit
     *     is OPEN
     */
    protected void checkCircuitBreaker() {
        if (circuitBreaker != null) {
            circuitBreaker.isRequestAllowed();
        }
    }

    /**
     * Records a successful inference request with the circuit breaker.
     *
     * <p>Subclasses should call this method after successful inference requests to update circuit
     * breaker metrics.
     */
    protected void recordSuccess() {
        if (circuitBreaker != null) {
            circuitBreaker.recordSuccess();
        }
    }

    /**
     * Records a failed inference request with the circuit breaker.
     *
     * <p>Subclasses should call this method after failed inference requests to update circuit
     * breaker metrics.
     */
    protected void recordFailure() {
        if (circuitBreaker != null) {
            circuitBreaker.recordFailure();
        }
    }

    /**
     * Gets the current circuit breaker instance.
     *
     * @return the circuit breaker, or null if not enabled
     */
    protected TritonCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    /**
     * Gets the current health checker instance.
     *
     * @return the health checker, or null if not enabled
     */
    protected TritonHealthChecker getHealthChecker() {
        return healthChecker;
    }

    /**
     * Checks if health checking is enabled.
     *
     * @return true if health checking is enabled
     */
    protected boolean isHealthCheckEnabled() {
        return healthCheckEnabled;
    }

    /**
     * Checks if circuit breaker is enabled.
     *
     * @return true if circuit breaker is enabled
     */
    protected boolean isCircuitBreakerEnabled() {
        return circuitBreakerEnabled;
    }

    protected int getMaxRetries() {
        return maxRetries;
    }

    protected Duration getRetryInitialBackoff() {
        return retryInitialBackoff;
    }

    protected Duration getRetryMaxBackoff() {
        return retryMaxBackoff;
    }

    protected String getDefaultValue() {
        return defaultValue;
    }
}
