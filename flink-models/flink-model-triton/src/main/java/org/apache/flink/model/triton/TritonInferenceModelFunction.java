/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.model.triton.exception.TritonCircuitBreakerOpenException;
import org.apache.flink.model.triton.exception.TritonClientException;
import org.apache.flink.model.triton.exception.TritonException;
import org.apache.flink.model.triton.exception.TritonNetworkException;
import org.apache.flink.model.triton.exception.TritonSchemaException;
import org.apache.flink.model.triton.exception.TritonServerException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * {@link AsyncPredictFunction} for Triton Inference Server generic inference task.
 *
 * <p><b>Request Model (v1):</b> This implementation processes records one-by-one. Each {@link
 * #asyncPredict(RowData)} call triggers one HTTP request to Triton server. There is no Flink-side
 * mini-batch aggregation in the current version.
 *
 * <p><b>Batch Efficiency:</b> Inference throughput benefits from:
 *
 * <ul>
 *   <li><b>Triton Dynamic Batching</b>: Configure {@code dynamic_batching} in model's {@code
 *       config.pbtxt} to aggregate concurrent requests server-side
 *   <li><b>Flink Parallelism</b>: High parallelism naturally creates concurrent requests that
 *       Triton can batch together
 *   <li><b>AsyncDataStream Capacity</b>: Buffer size controls concurrent in-flight requests,
 *       increasing opportunities for server-side batching
 * </ul>
 *
 * <p><b>Future Roadmap (v2+):</b> Flink-side mini-batch aggregation will be added to reduce HTTP
 * overhead (configurable via {@code batch-size} and {@code batch-timeout} options).
 *
 * @see <a
 *     href="https://github.com/triton-inference-server/server/blob/main/docs/user_guide/model_configuration.md#dynamic-batcher">Triton
 *     Dynamic Batching Documentation</a>
 */
public class TritonInferenceModelFunction extends AbstractTritonModelFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TritonInferenceModelFunction.class);

    private static final MediaType JSON_MEDIA_TYPE =
            MediaType.get("application/json; charset=utf-8");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Initial capacity for the per-call gzip compression buffer.
     *
     * <p>A previous implementation kept a single {@link ByteArrayOutputStream} as an instance field
     * to avoid repeated allocations. That was unsafe: {@link #asyncPredict(RowData)} is invoked
     * concurrently from the AsyncIO operator (capacity is typically far greater than 1), so
     * concurrent calls would race on {@code reset()} / {@code toByteArray()} and produce torn,
     * corrupt bytes. Allocating a fresh buffer per call trades a cheap allocation for correctness;
     * {@link ThreadLocal} was considered but rejected because the AsyncIO runner may dispatch
     * different records to the same thread after the network callback, leaving stale residue
     * visible if we forgot to {@code reset()}.
     */
    private static final int COMPRESSION_BUFFER_INITIAL_CAPACITY = 1024;

    private final LogicalType inputType;
    private final LogicalType outputType;
    private final String inputName;
    private final String outputName;

    /**
     * Pre-parsed default-value payload, computed once at {@link #open(FunctionContext)} and reused
     * to build per-call fallback rows. Parsing is a pure function of the configured string plus the
     * output {@link LogicalType}, so caching trades a one-time startup cost for zero per-record
     * parsing work — important because a sustained Triton outage will route every record through
     * the fallback path.
     *
     * <p>We deliberately cache only the deserialized <b>field payload</b> (a {@code BinaryString},
     * primitive box, array, or {@code null}) rather than a {@code Collection<RowData>}: handing the
     * same {@code GenericRowData}/{@code List} instance to every failing record would create
     * implicit aliasing across emitted rows, which breaks the "each emitted row is independent"
     * contract that downstream operators (aggregations keeping per-row references, changelog
     * collectors, serializers reusing fields) rely on. Building a fresh {@link
     * GenericRowData#of(Object...)} plus a fresh {@link Collections#singletonList} per fallback is
     * effectively free and sidesteps that aliasing hazard.
     *
     * <p>{@link #defaultValueConfigured} is the single source of truth for whether a fallback is
     * enabled; {@code cachedDefaultPayload} alone is ambiguous because a configured {@code
     * default-value='null'} legitimately parses to {@code null}.
     */
    private transient Object cachedDefaultPayload;

    /**
     * Whether a {@code default-value} fallback is configured. Tracked separately from {@link
     * #cachedDefaultPayload} because the SQL-NULL fallback (configured as the literal string {@code
     * 'null'}) legitimately deserializes to a Java {@code null}, which would otherwise be
     * indistinguishable from "no fallback configured".
     */
    private transient boolean defaultValueConfigured;

    public TritonInferenceModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        super(factoryContext, config);

        // Validate and store input/output types
        validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedOutputSchema(),
                null, // Allow any supported type
                "output");

        // Get input and output column information
        Column inputColumn =
                factoryContext.getCatalogModel().getResolvedInputSchema().getColumns().get(0);
        Column outputColumn =
                factoryContext.getCatalogModel().getResolvedOutputSchema().getColumns().get(0);

        this.inputType = inputColumn.getDataType().getLogicalType();
        this.outputType = outputColumn.getDataType().getLogicalType();
        this.inputName = inputColumn.getName();
        this.outputName = outputColumn.getName();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        // Eagerly parse the configured default-value so that (a) a malformed default-value
        // surfaces as a job-submission failure instead of masking itself as a silent runtime
        // error on the first failing record, and (b) we pay zero JSON-parse cost on the hot
        // fallback path during a sustained backend outage.
        if (getDefaultValue() != null) {
            try {
                this.cachedDefaultPayload = parseDefaultPayload(getDefaultValue());
                this.defaultValueConfigured = true;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Failed to parse configured default-value '%s' for output type %s. "
                                        + "Expected formats: STRING → plain text; numeric → string "
                                        + "representation (e.g. '-1'); ARRAY → JSON array (e.g. "
                                        + "'[0.0, 0.0]'); SQL NULL → 'null'.",
                                getDefaultValue(), outputType),
                        e);
            }
        }
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncPredict(RowData rowData) {
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        asyncPredictWithRetry(rowData, future, 0);
        return future;
    }

    /**
     * Executes inference request with retry logic and default value fallback.
     *
     * @param rowData Input data for inference
     * @param future The future to complete with result or exception
     * @param attemptNumber Current attempt number (0-indexed)
     */
    private void asyncPredictWithRetry(
            RowData rowData, CompletableFuture<Collection<RowData>> future, int attemptNumber) {

        // If the operator is tearing down, do not schedule/execute further retry attempts.
        // `attemptNumber > 0` means we are on a retry path (a scheduled continuation); dropping
        // it here lets Flink's normal cancellation path clean up the pending future instead of
        // us completing it on a dying thread pool.
        if (attemptNumber > 0
                && (Thread.currentThread().isInterrupted()
                        || retryScheduler == null
                        || retryScheduler.isShutdown())) {
            LOG.debug(
                    "Skipping retry attempt {} because retry scheduler is shutting down",
                    attemptNumber + 1);
            return;
        }

        try {
            // Check circuit breaker before making request
            checkCircuitBreaker();

            String requestBody = buildInferenceRequest(rowData);
            String url =
                    TritonUtils.buildInferenceUrl(getEndpoint(), getModelName(), getModelVersion());
            // Sanitized URL used exclusively for log lines and exception messages so that an
            // endpoint carrying basic-auth credentials (user:password@host) does not leak into
            // logs or surface in stack traces that may reach users.
            String loggedUrl = TritonUtils.sanitizeUrl(url);

            Request.Builder requestBuilder = new Request.Builder().url(url);

            // Handle compression and request body
            if (getCompression() != null) {
                Preconditions.checkArgument(
                        "gzip".equalsIgnoreCase(getCompression()),
                        "Unsupported compression algorithm: '%s'. Currently only 'gzip' is supported.",
                        getCompression());
                // Only support GZIP. Allocate a fresh per-call buffer: see
                // COMPRESSION_BUFFER_INITIAL_CAPACITY Javadoc for why reusing an instance-level
                // buffer would be unsafe under concurrent asyncPredict() invocations.
                ByteArrayOutputStream compressionBuffer =
                        new ByteArrayOutputStream(COMPRESSION_BUFFER_INITIAL_CAPACITY);
                try (GZIPOutputStream gzos = new GZIPOutputStream(compressionBuffer)) {
                    gzos.write(requestBody.getBytes(StandardCharsets.UTF_8));
                }
                byte[] compressedData = compressionBuffer.toByteArray();

                requestBuilder.addHeader("Content-Encoding", "gzip");
                requestBuilder.post(RequestBody.create(compressedData, JSON_MEDIA_TYPE));
            } else {
                requestBuilder.post(RequestBody.create(requestBody, JSON_MEDIA_TYPE));
            }

            // Add authentication header if provided
            if (getAuthToken() != null) {
                requestBuilder.addHeader("Authorization", "Bearer " + getAuthToken());
            }

            // Add custom headers if provided
            if (getCustomHeaders() != null && !getCustomHeaders().isEmpty()) {
                getCustomHeaders().forEach((key, value) -> requestBuilder.addHeader(key, value));
            }

            Request request = requestBuilder.build();

            httpClient
                    .newCall(request)
                    .enqueue(
                            new Callback() {
                                @Override
                                public void onFailure(Call call, IOException e) {
                                    LOG.error(
                                            "Triton inference request failed (attempt {}/{}) due to network error: {}",
                                            attemptNumber + 1,
                                            getMaxRetries() + 1,
                                            e.getMessage());

                                    // Wrap IOException in TritonNetworkException. The sanitized
                                    // URL is used in the user-visible message so that basic-auth
                                    // credentials embedded in the endpoint cannot leak through
                                    // exception stacks, CI logs, or error dashboards.
                                    //
                                    // NOTE: We deliberately do NOT call recordFailure() here.
                                    // Retry state is "one logical request, N physical attempts",
                                    // and the circuit breaker counts logical failures only. The
                                    // failure is recorded exactly once in handleFailureWithRetry
                                    // when all retries are exhausted; otherwise a 3-retry request
                                    // that eventually succeeds would be counted as 3 failures +
                                    // 1 success, unfairly biasing the breaker toward OPEN.
                                    TritonNetworkException networkException =
                                            new TritonNetworkException(
                                                    String.format(
                                                            "Failed to connect to Triton server at %s: %s. "
                                                                    + "This may indicate network connectivity issues, DNS resolution failure, or server unavailability.",
                                                            loggedUrl, e.getMessage()),
                                                    e);

                                    handleFailureWithRetry(
                                            rowData,
                                            future,
                                            attemptNumber,
                                            networkException,
                                            /* countAsBreakerFailure */ true,
                                            /* retryable */ true);
                                }

                                @Override
                                public void onResponse(Call call, Response response)
                                        throws IOException {
                                    try {
                                        if (!response.isSuccessful()) {
                                            // Let handleErrorResponseWithRetry classify 4xx vs 5xx
                                            // and route to retry / default-value fallback. The
                                            // circuit breaker is updated exactly once per logical
                                            // request (inside handleFailureWithRetry when retries
                                            // are exhausted); see the comment in onFailure() for
                                            // why per-attempt recording would be incorrect.
                                            //
                                            // Historical context: 4xx responses are intentionally
                                            // NOT fed into the breaker even at final failure -
                                            // they represent client-side configuration problems
                                            // (wrong model name, bad shape, missing auth) that
                                            // would persist regardless of server health. Folding
                                            // them into the breaker would cause a user with a
                                            // persistent config error to force-open it and deny
                                            // traffic to an otherwise healthy server.
                                            handleErrorResponseWithRetry(
                                                    response, rowData, future, attemptNumber);
                                            return;
                                        }

                                        // OkHttp guarantees that a response produced by
                                        // Call.enqueue() has a non-null body for successful
                                        // responses, but we guard defensively: a malformed
                                        // proxy/interceptor could violate that invariant, and
                                        // we prefer a typed client exception to an NPE in the
                                        // user's pipeline.
                                        okhttp3.ResponseBody body = response.body();
                                        if (body == null) {
                                            // Treat a missing body like any other transient
                                            // response failure: route through the retry / default-
                                            // value pipeline so the operator behaves consistently
                                            // across 5xx, network errors and malformed proxy
                                            // responses. Circuit-breaker accounting happens
                                            // exactly once in handleFailureWithRetry on final
                                            // exhaustion.
                                            handleFailureWithRetry(
                                                    rowData,
                                                    future,
                                                    attemptNumber,
                                                    new TritonClientException(
                                                            "Triton response has no body for "
                                                                    + loggedUrl
                                                                    + ". This typically indicates a misbehaving "
                                                                    + "proxy or interceptor between the client and Triton.",
                                                            response.code()),
                                                    /* countAsBreakerFailure */ true,
                                                    /* retryable */ true);
                                            return;
                                        }
                                        String responseBody;
                                        try {
                                            responseBody = body.string();
                                        } catch (IOException bodyReadFailure) {
                                            // Reading the 2xx response body can still fail mid-
                                            // stream (connection reset, proxy truncation). Route
                                            // this through the retry pipeline with breaker
                                            // accounting: it is a network-level failure, not a
                                            // server-side or client-side logic error.
                                            LOG.error(
                                                    "Failed to read Triton 2xx response body (attempt {}/{}): {}",
                                                    attemptNumber + 1,
                                                    getMaxRetries() + 1,
                                                    bodyReadFailure.getMessage());
                                            TritonNetworkException wrapped =
                                                    new TritonNetworkException(
                                                            String.format(
                                                                    "Failed to read Triton response body from %s: %s. "
                                                                            + "The connection was lost while streaming the response.",
                                                                    loggedUrl,
                                                                    bodyReadFailure.getMessage()),
                                                            bodyReadFailure);
                                            handleFailureWithRetry(
                                                    rowData,
                                                    future,
                                                    attemptNumber,
                                                    wrapped,
                                                    /* countAsBreakerFailure */ true,
                                                    /* retryable */ true);
                                            return;
                                        }
                                        Collection<RowData> result =
                                                parseInferenceResponse(responseBody);

                                        // Record success with circuit breaker
                                        recordSuccess();

                                        future.complete(result);
                                    } catch (JsonProcessingException e) {
                                        LOG.error("Failed to parse Triton inference response", e);
                                        // Don't record as circuit breaker failure - this is a
                                        // client parsing issue (deterministic bug in either our
                                        // response handling or the server's response schema),
                                        // and retrying cannot help. Short-circuit to fallback.
                                        TritonClientException parseException =
                                                new TritonClientException(
                                                        "Failed to parse Triton response JSON: "
                                                                + e.getMessage()
                                                                + ". This may indicate an incompatible response format.",
                                                        400);
                                        handleFailureWithRetry(
                                                rowData,
                                                future,
                                                attemptNumber,
                                                parseException,
                                                /* countAsBreakerFailure */ false,
                                                /* retryable */ false);
                                    } catch (Exception e) {
                                        LOG.error("Failed to process Triton inference response", e);
                                        // Don't record as circuit breaker failure - client-side
                                        // processing error (post-response), not a backend health
                                        // signal. Also not retryable (deterministic).
                                        handleFailureWithRetry(
                                                rowData,
                                                future,
                                                attemptNumber,
                                                e,
                                                /* countAsBreakerFailure */ false,
                                                /* retryable */ false);
                                    } finally {
                                        response.close();
                                    }
                                }
                            });

        } catch (TritonCircuitBreakerOpenException e) {
            // Circuit breaker is OPEN - fail fast. The entire point of the breaker is to shed
            // load when the backend is known-bad; retrying (with or without backoff) would
            // defeat that protection and keep hammering an unhealthy server. We still honour the
            // default-value fallback so callers that opted into graceful degradation see a
            // fallback row instead of a propagated exception.
            //
            // Breaker accounting rule: "one logical request counts once". The first physical
            // attempt (attemptNumber == 0) never touches the backend when the breaker is
            // already OPEN — it short-circuits here, so recording a failure would double-count
            // against the next real failure and also let a healthy-but-shedding breaker
            // accelerate its own OPEN decisions. However, if we reach here on a retry
            // (attemptNumber > 0), the *original* attempt did produce a genuine backend
            // failure that triggered the retry path; that failure was intentionally deferred
            // to the "retries exhausted" branch of handleFailureWithRetry so an eventual
            // success would not over-count transient blips. Because the breaker has since
            // tripped (likely via concurrent requests), there will be no "retries exhausted"
            // call for this logical request, and without recording here the original backend
            // failure would be silently dropped from breaker statistics.
            if (attemptNumber > 0) {
                recordFailure();
            }
            LOG.debug(
                    "Circuit breaker OPEN; skipping retry and routing to default-value fallback (if configured)");
            completeWithDefaultValueOrFail(future, e);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            // Deterministic client-side bug (bad input shape, unsupported compression algo, etc.).
            // Not retryable; a standalone client bug should not count against the breaker. On a
            // retry path (attemptNumber > 0) we must still flush the prior deferred 5xx/network
            // failure — see shouldRecordBreakerFailureOnFinalCompletion.
            LOG.error("Failed to build Triton inference request", e);
            if (attemptNumber > 0) {
                recordFailure();
            }
            completeWithDefaultValueOrFail(future, e);
        } catch (Exception e) {
            // Unexpected failure during request dispatch (e.g. dispatcher shutdown race). Treat
            // as a transient infrastructure error: retry if budget remains, but do NOT count
            // against the breaker (the backend never saw this request).
            LOG.error("Unexpected error dispatching Triton request", e);
            handleFailureWithRetry(
                    rowData,
                    future,
                    attemptNumber,
                    e,
                    /* countAsBreakerFailure */ false,
                    /* retryable */ true);
        }
    }

    /**
     * Handles request failure with retry logic or default value fallback.
     *
     * <p>Retry is only attempted for transient failures; circuit-breaker-open and unrecoverable
     * errors are short-circuited by the caller. The circuit breaker is updated at most once per
     * logical request (here, on final exhaustion) and only when the caller asserts that the failure
     * is a backend-health signal — deterministic client-side bugs (JSON parse errors, malformed
     * input) must not poison the breaker for an otherwise-healthy server.
     *
     * @param rowData Input data for inference
     * @param future The future to complete
     * @param attemptNumber Current attempt number (0-indexed)
     * @param error The error that caused the failure
     * @param countAsBreakerFailure Whether this failure should be counted against the circuit
     *     breaker when retries are exhausted. {@code true} for 5xx / network errors; {@code false}
     *     for 4xx / parse / deterministic client bugs.
     * @param retryable Whether retry should even be attempted. {@code false} short-circuits to the
     *     fallback/fail path immediately (e.g. deterministic client bugs).
     */
    private void handleFailureWithRetry(
            RowData rowData,
            CompletableFuture<Collection<RowData>> future,
            int attemptNumber,
            Throwable error,
            boolean countAsBreakerFailure,
            boolean retryable) {

        if (retryable && attemptNumber < getMaxRetries()) {
            long delayMs = computeBackoffDelayMillis(attemptNumber);

            // +2 = (0-indexed attemptNumber + 1 for the next attempt) + (1 to make 1-based for
            // human-readable logs). Total attempts is also 1-based (N retries => N+1 attempts).
            int nextAttemptOneBased = attemptNumber + 2;
            int totalAttempts = getMaxRetries() + 1;
            LOG.info(
                    "Retrying Triton inference request (attempt {}/{}) after {} ms",
                    nextAttemptOneBased,
                    totalAttempts,
                    delayMs);

            // Schedule retry on the function-owned scheduler so that cancellation on close()
            // takes effect and we never fire an HTTP call against a released client.
            if (retryScheduler == null || retryScheduler.isShutdown()) {
                // Should not happen while the operator is open with maxRetries > 0, but guard
                // defensively against race with close().
                LOG.warn("Retry scheduler unavailable; failing request without further retries");
                // Same accounting rule as the "retries exhausted" branch below: record once
                // when the current failure is breaker-worthy OR when a prior attempt
                // deferred its accounting here.
                if (shouldRecordBreakerFailureOnFinalCompletion(
                        countAsBreakerFailure, attemptNumber)) {
                    recordFailure();
                }
                completeWithDefaultValueOrFail(future, error);
                return;
            }
            try {
                retryScheduler.schedule(
                        () -> asyncPredictWithRetry(rowData, future, attemptNumber + 1),
                        delayMs,
                        TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException rejected) {
                // TOCTOU: scheduler was alive when we checked above but got shutdown between
                // isShutdown() and schedule() (close() racing against an in-flight callback).
                // Propagating would leave the AsyncIO future uncompleted and stall the operator,
                // so we must complete synchronously here.
                LOG.warn(
                        "Retry scheduler rejected task (operator closing?); failing request without further retries",
                        rejected);
                if (shouldRecordBreakerFailureOnFinalCompletion(
                        countAsBreakerFailure, attemptNumber)) {
                    recordFailure();
                }
                completeWithDefaultValueOrFail(future, error);
            }
        } else {
            // No more retries (exhausted or not retryable). See
            // shouldRecordBreakerFailureOnFinalCompletion for the accounting rule.
            if (shouldRecordBreakerFailureOnFinalCompletion(countAsBreakerFailure, attemptNumber)) {
                recordFailure();
            }

            // attemptNumber + 1 is the accurate attempt count: 1 for a non-retryable failure
            // at attempt 0, getMaxRetries()+1 when retries were exhausted.
            int attemptsMade = attemptNumber + 1;
            if (defaultValueConfigured) {
                LOG.warn(
                        "Triton inference failed after {} attempt(s). Returning configured default value. Original error: {}",
                        attemptsMade,
                        error.getMessage(),
                        error);
            } else {
                LOG.error(
                        "Triton inference failed after {} attempt(s). No default value configured.",
                        attemptsMade);
            }
            completeWithDefaultValueOrFail(future, error);
        }
    }

    /**
     * Whether the final-completion site should invoke {@link #recordFailure()} for this logical
     * request.
     *
     * <p>Record when either the current-attempt failure is breaker-worthy ({@code
     * countAsBreakerFailure}) or we are on a retry path ({@code attemptNumber > 0}), since only
     * breaker-worthy failures enter the retry path — so {@code attemptNumber > 0} implies a prior
     * deferred breaker-worthy failure still needs accounting.
     *
     * <p>Without the {@code attemptNumber > 0} clause, a 5xx on attempt 0 followed by a
     * non-retryable failure (4xx / JSON parse / request-build bug) on a retry would silently drop
     * the original 5xx from breaker statistics — the same class of bug previously fixed for the
     * {@link TritonCircuitBreakerOpenException} short-circuit.
     *
     * <p>Package-private + static for direct unit-test coverage of the decision rule.
     */
    static boolean shouldRecordBreakerFailureOnFinalCompletion(
            boolean countAsBreakerFailure, int attemptNumber) {
        return countAsBreakerFailure || attemptNumber > 0;
    }

    /**
     * Computes the exponential-backoff delay for the given attempt, clamped to {@code
     * retry-max-backoff} to prevent overflow and unreasonably long sleeps when {@code max-retries}
     * is large, and randomized with equal jitter to avoid a thundering herd of concurrent retries.
     *
     * <p>The nominal delay is {@code base * 2^attempt}, clamped to {@code capMs}. With equal
     * jitter, the returned delay is drawn uniformly from {@code [nominal/2, nominal]}: we keep half
     * of the backoff as a deterministic floor (so the server still gets the intended breathing room
     * before the next burst) and randomize the other half to spread concurrent retries across a
     * time window. Without jitter a cluster of N parallel AsyncIO slots failing on the same Triton
     * outage would all retry at the exact same instant, producing a synchronized thundering herd
     * that defeats the purpose of exponential backoff.
     *
     * <p>Package-private + static so that the deterministic portion of the algorithm can be
     * exercised in isolation without standing up a full {@link TritonInferenceModelFunction}
     * fixture. See {@link #computeBackoffWithJitter(long)} for the jitter wrapper.
     *
     * <p><b>Preconditions (enforced by callers):</b> {@code attemptNumber >= 0}, {@code baseMs >
     * 0}, {@code capMs >= baseMs}. These are validated in {@link AbstractTritonModelFunction#open}
     * via {@link org.apache.flink.util.Preconditions}, so direct callers of this static helper
     * (including tests) must uphold the same contract — passing {@code baseMs == 0} returns {@code
     * 0}, but that is a degenerate configuration that the option-validation layer rejects.
     */
    static long computeBackoffDelayMillis(int attemptNumber, long baseMs, long capMs) {
        // Cap the shift amount defensively. `1L << 62` already exceeds any realistic base*cap
        // product; beyond 62 the Java spec masks the shift amount (mod 64), which would silently
        // wrap and produce bogus delays. Clamp before the shift so the math is always monotone.
        int shift = Math.max(0, Math.min(attemptNumber, 30));
        long multiplier = 1L << shift;

        // Guard against base * multiplier overflow: if it would overflow, use the cap directly.
        long delay;
        if (baseMs > 0 && multiplier > Long.MAX_VALUE / baseMs) {
            delay = capMs;
        } else {
            delay = baseMs * multiplier;
        }
        return Math.min(delay, capMs);
    }

    /**
     * Applies equal jitter to an already-computed deterministic backoff: returns a value drawn
     * uniformly from {@code [nominalDelayMs / 2, nominalDelayMs]}. See {@link
     * #computeBackoffDelayMillis(int, long, long)} for the rationale.
     *
     * <p>Kept separate (and package-private) from the deterministic helper so that existing unit
     * tests can continue to assert exact values for the backoff curve, while the randomized
     * integration still exercises jitter in production paths.
     *
     * <p>Uses {@link ThreadLocalRandom} to avoid contention when many AsyncIO slots retry in
     * parallel — a shared {@link java.util.Random} would serialize on its internal seed field and
     * itself become a coordination point under exactly the high-concurrency conditions that make
     * jitter necessary.
     */
    static long computeBackoffWithJitter(long nominalDelayMs) {
        if (nominalDelayMs <= 1L) {
            // A delay of 0 or 1 ms is already so small that jitter would be meaningless and
            // could introduce negative / zero delays via truncation. Pass through.
            return nominalDelayMs;
        }
        long halfDelay = nominalDelayMs / 2L;
        // nextLong(origin, bound) requires origin < bound; halfDelay >= 1 and nominalDelayMs + 1
        // is strictly greater, so this is safe.
        return ThreadLocalRandom.current().nextLong(halfDelay, nominalDelayMs + 1L);
    }

    private long computeBackoffDelayMillis(int attemptNumber) {
        long nominal =
                computeBackoffDelayMillis(
                        attemptNumber,
                        getRetryInitialBackoff().toMillis(),
                        getRetryMaxBackoff().toMillis());
        return computeBackoffWithJitter(nominal);
    }

    /**
     * Completes the future with a freshly-built default-value result when {@code default-value} is
     * configured, or with the supplied error otherwise. Centralises the "either / or" decision so
     * that both the retry-exhausted path and the non-retryable (4xx, circuit-breaker-open) path
     * share one implementation.
     *
     * <p>A new {@link GenericRowData} / singleton {@link Collection} is built per invocation — see
     * {@link #cachedDefaultPayload} for why sharing a single pre-built collection across all
     * fallback emissions would be unsafe.
     */
    private void completeWithDefaultValueOrFail(
            CompletableFuture<Collection<RowData>> future, Throwable error) {
        if (defaultValueConfigured) {
            future.complete(buildDefaultResult(cachedDefaultPayload));
        } else {
            future.completeExceptionally(error);
        }
    }

    /**
     * Builds a fresh singleton {@link Collection} wrapping a fresh {@link GenericRowData} around
     * the given payload. Exposed as package-private + static so that the "every fallback emits a
     * distinct instance" invariant can be asserted in isolation — see {@link #cachedDefaultPayload}
     * for why sharing a single pre-built collection across all fallback emissions would be unsafe.
     */
    static Collection<RowData> buildDefaultResult(Object cachedPayload) {
        return Collections.singletonList(GenericRowData.of(cachedPayload));
    }

    /**
     * Handles HTTP error response with retry logic.
     *
     * @param response The HTTP response
     * @param rowData Input data for inference
     * @param future The future to complete
     * @param attemptNumber Current attempt number
     */
    private void handleErrorResponseWithRetry(
            Response response,
            RowData rowData,
            CompletableFuture<Collection<RowData>> future,
            int attemptNumber) {

        int statusCode = response.code();

        // Read the error body in its own try/catch so a body-read failure does NOT lose the
        // original HTTP status. Without this guard, an IOException from response.body().string()
        // would propagate out of onResponse() into the catch (Exception e) handler, which routes
        // failures as non-retryable / non-breaker — causing a genuine 5xx (which is retryable
        // and a real backend-health signal) to be silently downgraded.
        String errorBody;
        try {
            errorBody =
                    response.body() != null
                            ? response.body().string()
                            : "No error details provided";
        } catch (IOException bodyReadFailure) {
            LOG.warn(
                    "Failed to read Triton error response body for HTTP {} (attempt {}/{}); "
                            + "preserving status code for routing decision",
                    statusCode,
                    attemptNumber + 1,
                    getMaxRetries() + 1,
                    bodyReadFailure);
            errorBody = "<error body unreadable: " + bodyReadFailure.getMessage() + ">";
        }

        LOG.error(
                "Triton inference failed (attempt {}/{}) with HTTP {}: {}",
                attemptNumber + 1,
                getMaxRetries() + 1,
                statusCode,
                errorBody);

        // Build detailed error message with context
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append(
                String.format("Triton inference failed with HTTP %d: %s\n", statusCode, errorBody));
        errorMsg.append("\n=== Request Configuration ===\n");
        errorMsg.append(
                String.format("  Model: %s (version: %s)\n", getModelName(), getModelVersion()));
        // Use the sanitized endpoint in user-facing error output so that basic-auth credentials
        // in the configured endpoint cannot leak through error messages surfaced in job logs,
        // metrics, or downstream exception stacks.
        String sanitizedEndpoint = TritonUtils.sanitizeUrl(getEndpoint());
        errorMsg.append(String.format("  Endpoint: %s\n", sanitizedEndpoint));
        errorMsg.append(String.format("  Input column: %s\n", inputName));
        errorMsg.append(String.format("  Input Flink type: %s\n", inputType));
        errorMsg.append(
                String.format(
                        "  Input Triton dtype: %s\n",
                        TritonTypeMapper.toTritonDataType(inputType).getTritonName()));

        // Check if this is a shape mismatch error
        boolean isShapeMismatch =
                errorBody.toLowerCase().contains("shape")
                        || errorBody.toLowerCase().contains("dimension");

        TritonException exception;

        if (statusCode >= 400 && statusCode < 500) {
            // Client error - user configuration issue
            errorMsg.append("\n=== Troubleshooting (Client Error) ===\n");

            if (statusCode == 400) {
                errorMsg.append("  - Verify input shape matches model's config.pbtxt\n");
                errorMsg.append("  - For scalar: use INT/FLOAT/DOUBLE/STRING\n");
                errorMsg.append("  - For 1-D tensor: use ARRAY<type>\n");
                errorMsg.append(
                        "  - Try flatten-batch-dim=true if model expects [N] but gets [1,N]\n");

                if (isShapeMismatch) {
                    exception =
                            new TritonSchemaException(
                                    errorMsg.toString(),
                                    "See Triton model config.pbtxt",
                                    String.format("Flink type: %s", inputType));
                } else {
                    exception = new TritonClientException(errorMsg.toString(), statusCode);
                }
            } else if (statusCode == 404) {
                errorMsg.append("  - Verify model-name: ").append(getModelName()).append("\n");
                errorMsg.append("  - Verify model-version: ")
                        .append(getModelVersion())
                        .append("\n");
                errorMsg.append("  - Check model is loaded: GET ")
                        .append(sanitizedEndpoint)
                        .append("\n");
                exception = new TritonClientException(errorMsg.toString(), statusCode);
            } else if (statusCode == 401 || statusCode == 403) {
                errorMsg.append("  - Check auth-token configuration\n");
                errorMsg.append("  - Verify server authentication requirements\n");
                exception = new TritonClientException(errorMsg.toString(), statusCode);
            } else {
                exception = new TritonClientException(errorMsg.toString(), statusCode);
            }

            // 4xx is a persistent client-side configuration problem: no retry, and a
            // standalone 4xx is not fed to the breaker (would force-open on user misconfig
            // against an otherwise-healthy server). On a retry path (attemptNumber > 0) the
            // prior 5xx/network failure was deferred; flush it once here — same rule as
            // shouldRecordBreakerFailureOnFinalCompletion. Bypasses handleFailureWithRetry
            // because there's nothing to retry.
            if (attemptNumber > 0) {
                recordFailure();
            }
            if (defaultValueConfigured) {
                LOG.warn(
                        "Client error (HTTP {}). Returning default value. Original error: {}",
                        statusCode,
                        exception.getMessage(),
                        exception);
            } else {
                // Make the "not retrying, no fallback, propagating exception" decision
                // explicit in the operator log so that operators chasing a misconfiguration
                // don't have to infer it from the absence of a retry log line.
                LOG.error(
                        "Client error (HTTP {}) is non-retryable and no default-value is configured; "
                                + "propagating exception to AsyncIO",
                        statusCode);
            }
            completeWithDefaultValueOrFail(future, exception);

        } else if (statusCode >= 500 && statusCode < 600) {
            // Server error - Triton service issue - retryable
            errorMsg.append("\n=== Troubleshooting (Server Error) ===\n");

            if (statusCode == 500) {
                errorMsg.append("  - Check Triton server logs for inference crash details\n");
                errorMsg.append("  - Model may have run out of memory\n");
                errorMsg.append("  - Input data may trigger model bug\n");
            } else if (statusCode == 503) {
                errorMsg.append("  - Server is overloaded or unavailable\n");
                errorMsg.append("  - This error is retryable with backoff\n");
                errorMsg.append("  - Consider scaling Triton server resources\n");
            } else if (statusCode == 504) {
                errorMsg.append("  - Inference exceeded gateway timeout\n");
                errorMsg.append("  - This error is retryable\n");
                errorMsg.append("  - Consider increasing timeout configuration\n");
            }

            exception = new TritonServerException(errorMsg.toString(), statusCode);
            // 5xx is a genuine backend-health signal: count against the breaker and retry.
            handleFailureWithRetry(
                    rowData,
                    future,
                    attemptNumber,
                    exception,
                    /* countAsBreakerFailure */ true,
                    /* retryable */ true);

        } else {
            // Unexpected status code
            errorMsg.append("\n=== Unexpected Status Code ===\n");
            errorMsg.append("  - This status code is not standard for Triton\n");
            errorMsg.append("  - Check if proxy/load balancer is involved\n");

            exception = new TritonClientException(errorMsg.toString(), statusCode);
            // Non-standard status - could be a misbehaving proxy. Retry transparently but do
            // not conflate with backend health (the real Triton server never produced it).
            handleFailureWithRetry(
                    rowData,
                    future,
                    attemptNumber,
                    exception,
                    /* countAsBreakerFailure */ false,
                    /* retryable */ true);
        }
    }

    /**
     * Parses the configured default-value string into the single field payload that will be wrapped
     * into a fresh {@link GenericRowData} on every fallback emission.
     *
     * <p>Called once in {@link #open(FunctionContext)} so that a malformed default-value fails the
     * job at startup rather than masking the original inference error at runtime, and so that the
     * hot fallback path performs no JSON parsing.
     *
     * <p>We return a single payload object (not a {@code Collection<RowData>}) because sharing a
     * pre-built {@link RowData} across every failing record would alias mutable row state across
     * emitted rows — see the field-level Javadoc on {@link #cachedDefaultPayload} for details.
     *
     * @param defaultValueStr The default-value string to parse
     * @return The deserialized payload for the single output column (may be {@code null} when the
     *     user configures the literal string {@code 'null'} to request a SQL-NULL fallback)
     * @throws JsonProcessingException If parsing the JSON default value fails
     */
    private Object parseDefaultPayload(String defaultValueStr) throws JsonProcessingException {
        // Accept the literal string 'null' as an explicit SQL-NULL fallback for every output
        // type. Users who leave the option unset disable the fallback entirely (exceptions are
        // propagated), so we need an affirmative way to say "fall back to NULL". Handling it
        // uniformly here avoids the surprising difference between STRING (explicitly special-
        // cased) and ARRAY/numeric (where `objectMapper.readTree("null")` returned a NullNode
        // whose downstream mapping was implicit and undocumented).
        //
        // NOTE: As a consequence, a VARCHAR model cannot use the literal string "null" (lower-
        // case) as a non-null failure sentinel - it will always be interpreted as SQL NULL.
        // Users who need that exact sentinel should choose a different marker (e.g. "NULL",
        // "FAILED", "<null>"); this trade-off is documented in TritonOptions.DEFAULT_VALUE.
        if ("null".equals(defaultValueStr)) {
            return null;
        }
        if (outputType instanceof VarCharType) {
            return BinaryStringData.fromString(defaultValueStr);
        }
        // Array and other scalar types - parse JSON.
        JsonNode jsonNode = objectMapper.readTree(defaultValueStr);
        return TritonTypeMapper.deserializeFromJson(jsonNode, outputType);
    }

    private String buildInferenceRequest(RowData rowData) throws JsonProcessingException {
        ObjectNode requestNode = objectMapper.createObjectNode();

        // Add request ID if sequence ID is provided
        if (getSequenceId() != null) {
            requestNode.put("id", getSequenceId());
        }

        // Add parameters
        ObjectNode parametersNode = objectMapper.createObjectNode();
        if (getPriority() != null) {
            parametersNode.put("priority", getPriority());
        }
        if (isSequenceStart()) {
            parametersNode.put("sequence_start", true);
        }
        if (isSequenceEnd()) {
            parametersNode.put("sequence_end", true);
        }
        if (parametersNode.size() > 0) {
            requestNode.set("parameters", parametersNode);
        }

        // Add inputs
        ArrayNode inputsArray = objectMapper.createArrayNode();
        ObjectNode inputNode = objectMapper.createObjectNode();
        inputNode.put("name", inputName.toUpperCase());

        // Map Flink type to Triton type
        TritonDataType tritonType = TritonTypeMapper.toTritonDataType(inputType);
        inputNode.put("datatype", tritonType.getTritonName());

        // Serialize input data first to get actual size
        ArrayNode dataArray = objectMapper.createArrayNode();
        TritonTypeMapper.serializeToJsonArray(rowData, 0, inputType, dataArray);

        // Calculate and add shape based on actual data
        int[] shape = TritonTypeMapper.calculateShape(inputType, 1, rowData, 0);

        // Apply flatten-batch-dim if configured
        if (isFlattenBatchDim() && shape.length > 1 && shape[0] == 1) {
            // Remove the batch dimension: [1, N] -> [N]
            int[] flattenedShape = new int[shape.length - 1];
            System.arraycopy(shape, 1, flattenedShape, 0, flattenedShape.length);
            shape = flattenedShape;
        }

        ArrayNode shapeArray = objectMapper.createArrayNode();
        for (int dim : shape) {
            shapeArray.add(dim);
        }
        inputNode.set("shape", shapeArray);
        inputNode.set("data", dataArray);

        inputsArray.add(inputNode);
        requestNode.set("inputs", inputsArray);

        // Add outputs (request all outputs)
        ArrayNode outputsArray = objectMapper.createArrayNode();
        ObjectNode outputNode = objectMapper.createObjectNode();
        outputNode.put("name", outputName.toUpperCase());
        outputsArray.add(outputNode);
        requestNode.set("outputs", outputsArray);

        String requestJson = objectMapper.writeValueAsString(requestNode);

        // Log the request for debugging
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Triton inference request - Model: {}, Version: {}, Input: {}, Shape: {}",
                    getModelName(),
                    getModelVersion(),
                    inputName,
                    java.util.Arrays.toString(shape));
            LOG.debug("Request body: {}", requestJson);
        }

        return requestJson;
    }

    private Collection<RowData> parseInferenceResponse(String responseBody)
            throws JsonProcessingException {
        JsonNode responseNode = objectMapper.readTree(responseBody);
        List<RowData> results = new ArrayList<>();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Triton response body: {}", responseBody);
        }

        JsonNode outputsNode = responseNode.get("outputs");
        if (outputsNode != null && outputsNode.isArray()) {
            for (JsonNode outputNode : outputsNode) {
                JsonNode dataNode = outputNode.get("data");

                if (dataNode != null && dataNode.isArray()) {
                    if (dataNode.size() > 0) {
                        // Check if output is array type or scalar
                        // If outputType is scalar but dataNode is array, extract first element
                        JsonNode nodeToDeserialize = dataNode;
                        if (!(outputType instanceof ArrayType) && dataNode.isArray()) {
                            // Scalar type - extract first element from array
                            nodeToDeserialize = dataNode.get(0);
                        }

                        Object deserializedData =
                                TritonTypeMapper.deserializeFromJson(nodeToDeserialize, outputType);

                        results.add(GenericRowData.of(deserializedData));
                    }
                }
            }
        } else {
            LOG.warn("No outputs found in Triton response");
        }

        // If no outputs were produced, prefer the user-configured default value when available so
        // that downstream operators see the same fallback payload as on retry-exhausted and
        // breaker-open paths. Only when no default-value is configured do we fall back to the
        // previous "type-specific empty sentinel" behaviour — but in that case we also warn,
        // because an empty / null row can easily masquerade as a successful prediction and
        // propagate silently through aggregations.
        if (results.isEmpty()) {
            if (defaultValueConfigured) {
                LOG.warn("Triton response contained no outputs; emitting configured default value");
                return buildDefaultResult(cachedDefaultPayload);
            }
            LOG.warn(
                    "Triton response contained no outputs and no default-value is configured; "
                            + "emitting a type-specific empty sentinel. Configure default-value "
                            + "to get a more explicit fallback.");
            Object defaultValue;
            if (outputType instanceof VarCharType) {
                defaultValue = BinaryStringData.EMPTY_UTF8;
            } else {
                defaultValue = null;
            }
            results.add(GenericRowData.of(defaultValue));
        }

        return results;
    }
}
