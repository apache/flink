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
import org.apache.flink.model.triton.exception.TritonClientException;
import org.apache.flink.model.triton.exception.TritonNetworkException;
import org.apache.flink.model.triton.exception.TritonSchemaException;
import org.apache.flink.model.triton.exception.TritonServerException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

    /** Reusable buffer for gzip compression to avoid repeated allocations. */
    private final ByteArrayOutputStream compressionBuffer = new ByteArrayOutputStream(1024);

    private final LogicalType inputType;
    private final LogicalType outputType;
    private final String inputName;
    private final String outputName;

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

        try {
            String requestBody = buildInferenceRequest(rowData);
            String url =
                    TritonUtils.buildInferenceUrl(getEndpoint(), getModelName(), getModelVersion());

            Request.Builder requestBuilder = new Request.Builder().url(url);

            // Handle compression and request body
            if (getCompression() != null) {
                Preconditions.checkArgument(
                        "gzip".equalsIgnoreCase(getCompression()),
                        "Unsupported compression algorithm: '%s'. Currently only 'gzip' is supported.",
                        getCompression());
                // Only support GZIP: Compress request body with gzip using reusable buffer.
                compressionBuffer.reset();
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

                                    // Wrap IOException in TritonNetworkException
                                    TritonNetworkException networkException =
                                            new TritonNetworkException(
                                                    String.format(
                                                            "Failed to connect to Triton server at %s: %s. "
                                                                    + "This may indicate network connectivity issues, DNS resolution failure, or server unavailability.",
                                                            url, e.getMessage()),
                                                    e);

                                    handleFailureWithRetry(
                                            rowData, future, attemptNumber, networkException);
                                }

                                @Override
                                public void onResponse(Call call, Response response)
                                        throws IOException {
                                    try {
                                        if (!response.isSuccessful()) {
                                            handleErrorResponseWithRetry(
                                                    response, rowData, future, attemptNumber, url);
                                            return;
                                        }

                                        String responseBody = response.body().string();
                                        Collection<RowData> result =
                                                parseInferenceResponse(responseBody);
                                        future.complete(result);
                                    } catch (JsonProcessingException e) {
                                        LOG.error("Failed to parse Triton inference response", e);
                                        TritonClientException parseException =
                                                new TritonClientException(
                                                        "Failed to parse Triton response JSON: "
                                                                + e.getMessage()
                                                                + ". This may indicate an incompatible response format.",
                                                        400);
                                        handleFailureWithRetry(
                                                rowData, future, attemptNumber, parseException);
                                    } catch (Exception e) {
                                        LOG.error("Failed to process Triton inference response", e);
                                        handleFailureWithRetry(rowData, future, attemptNumber, e);
                                    } finally {
                                        response.close();
                                    }
                                }
                            });

        } catch (Exception e) {
            LOG.error("Failed to build Triton inference request", e);
            handleFailureWithRetry(rowData, future, attemptNumber, e);
        }
    }

    /**
     * Handles request failure with retry logic or default value fallback.
     *
     * @param rowData Input data for inference
     * @param future The future to complete
     * @param attemptNumber Current attempt number
     * @param error The error that caused the failure
     */
    private void handleFailureWithRetry(
            RowData rowData,
            CompletableFuture<Collection<RowData>> future,
            int attemptNumber,
            Throwable error) {

        if (attemptNumber < getMaxRetries()) {
            // Calculate exponential backoff delay
            long delayMs = getRetryBackoff().toMillis() * (1L << attemptNumber);

            LOG.info(
                    "Retrying Triton inference request (attempt {}/{}) after {} ms",
                    attemptNumber + 2,
                    getMaxRetries() + 1,
                    delayMs);

            // Schedule retry with exponential backoff
            CompletableFuture.delayedExecutor(delayMs, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .execute(() -> asyncPredictWithRetry(rowData, future, attemptNumber + 1));
        } else {
            // All retries exhausted
            if (getDefaultValue() != null) {
                LOG.warn(
                        "All {} retry attempts failed. Returning configured default value. Original error: {}",
                        getMaxRetries() + 1,
                        error.getMessage(),
                        error);

                try {
                    Collection<RowData> defaultResult = parseDefaultValue();
                    future.complete(defaultResult);
                } catch (Exception e) {
                    LOG.error("Failed to parse default value", e);
                    // Chain both the original inference error and the parse error
                    IllegalArgumentException parseException =
                            new IllegalArgumentException(
                                    String.format(
                                            "Failed to parse default-value after %d retry attempts. "
                                                    + "Original inference error: %s. Parse error: %s",
                                            getMaxRetries() + 1,
                                            error.getMessage(),
                                            e.getMessage()),
                                    e);
                    parseException.addSuppressed(error);
                    future.completeExceptionally(parseException);
                }
            } else {
                LOG.error(
                        "All {} retry attempts failed. No default value configured.",
                        getMaxRetries() + 1);
                future.completeExceptionally(error);
            }
        }
    }

    /**
     * Handles HTTP error response with retry logic.
     *
     * @param response The HTTP response
     * @param rowData Input data for inference
     * @param future The future to complete
     * @param attemptNumber Current attempt number
     * @param url Request URL
     * @throws IOException If reading response fails
     */
    private void handleErrorResponseWithRetry(
            Response response,
            RowData rowData,
            CompletableFuture<Collection<RowData>> future,
            int attemptNumber,
            String url)
            throws IOException {

        String errorBody =
                response.body() != null ? response.body().string() : "No error details provided";
        int statusCode = response.code();

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
        errorMsg.append(String.format("  Endpoint: %s\n", getEndpoint()));
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

        Throwable exception;

        if (statusCode >= 400 && statusCode < 500) {
            // Client error - user configuration issue
            errorMsg.append("\n=== Troubleshooting (Client Error) ===\n");

            if (statusCode == 400) {
                errorMsg.append("  • Verify input shape matches model's config.pbtxt\n");
                errorMsg.append("  • For scalar: use INT/FLOAT/DOUBLE/STRING\n");
                errorMsg.append("  • For 1-D tensor: use ARRAY<type>\n");
                errorMsg.append(
                        "  • Try flatten-batch-dim=true if model expects [N] but gets [1,N]\n");

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
                errorMsg.append("  • Verify model-name: ").append(getModelName()).append("\n");
                errorMsg.append("  • Verify model-version: ")
                        .append(getModelVersion())
                        .append("\n");
                errorMsg.append("  • Check model is loaded: GET ")
                        .append(getEndpoint())
                        .append("\n");
                exception = new TritonClientException(errorMsg.toString(), statusCode);
            } else if (statusCode == 401 || statusCode == 403) {
                errorMsg.append("  • Check auth-token configuration\n");
                errorMsg.append("  • Verify server authentication requirements\n");
                exception = new TritonClientException(errorMsg.toString(), statusCode);
            } else {
                exception = new TritonClientException(errorMsg.toString(), statusCode);
            }

            // Client errors are not retryable - fail immediately
            if (getDefaultValue() != null) {
                LOG.warn(
                        "Client error (HTTP {}). Returning default value. Original error: {}",
                        statusCode,
                        exception.getMessage(),
                        exception);
                try {
                    Collection<RowData> defaultResult = parseDefaultValue();
                    future.complete(defaultResult);
                } catch (Exception e) {
                    LOG.error("Failed to parse default value", e);
                    IllegalArgumentException parseException =
                            new IllegalArgumentException(
                                    String.format(
                                            "Failed to parse default-value after client error (HTTP %d). "
                                                    + "Original error: %s. Parse error: %s",
                                            statusCode, exception.getMessage(), e.getMessage()),
                                    e);
                    parseException.addSuppressed(exception);
                    future.completeExceptionally(parseException);
                }
            } else {
                future.completeExceptionally(exception);
            }

        } else if (statusCode >= 500 && statusCode < 600) {
            // Server error - Triton service issue - retryable
            errorMsg.append("\n=== Troubleshooting (Server Error) ===\n");

            if (statusCode == 500) {
                errorMsg.append("  • Check Triton server logs for inference crash details\n");
                errorMsg.append("  • Model may have run out of memory\n");
                errorMsg.append("  • Input data may trigger model bug\n");
            } else if (statusCode == 503) {
                errorMsg.append("  • Server is overloaded or unavailable\n");
                errorMsg.append("  • This error is retryable with backoff\n");
                errorMsg.append("  • Consider scaling Triton server resources\n");
            } else if (statusCode == 504) {
                errorMsg.append("  • Inference exceeded gateway timeout\n");
                errorMsg.append("  • This error is retryable\n");
                errorMsg.append("  • Consider increasing timeout configuration\n");
            }

            exception = new TritonServerException(errorMsg.toString(), statusCode);
            handleFailureWithRetry(rowData, future, attemptNumber, exception);

        } else {
            // Unexpected status code
            errorMsg.append("\n=== Unexpected Status Code ===\n");
            errorMsg.append("  • This status code is not standard for Triton\n");
            errorMsg.append("  • Check if proxy/load balancer is involved\n");

            exception = new TritonClientException(errorMsg.toString(), statusCode);
            handleFailureWithRetry(rowData, future, attemptNumber, exception);
        }
    }

    /**
     * Parses the configured default value string into RowData collection.
     *
     * @return Collection containing single RowData with default value
     * @throws JsonProcessingException If parsing JSON default value fails
     */
    private Collection<RowData> parseDefaultValue() throws JsonProcessingException {
        List<RowData> results = new ArrayList<>();
        String defaultValueStr = getDefaultValue();

        Object deserializedData;

        if (outputType instanceof VarCharType) {
            // String type - use value directly
            deserializedData = BinaryStringData.fromString(defaultValueStr);
        } else {
            // Array and other scalar types - parse JSON
            JsonNode jsonNode = objectMapper.readTree(defaultValueStr);
            deserializedData = TritonTypeMapper.deserializeFromJson(jsonNode, outputType);
        }

        results.add(GenericRowData.of(deserializedData));
        return results;
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

        // If no outputs found, return default value based on type
        if (results.isEmpty()) {
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
