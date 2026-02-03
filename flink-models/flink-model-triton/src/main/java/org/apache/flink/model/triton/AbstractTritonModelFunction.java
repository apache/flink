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

import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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

    private final String endpoint;
    private final String modelName;
    private final String modelVersion;
    private final Duration timeout;
    private final boolean flattenBatchDim;
    private final Integer priority;
    private final String sequenceId;
    private final boolean sequenceStart;
    private final boolean sequenceEnd;
    private final String compression;
    private final String authToken;
    private final Map<String, String> customHeaders;

    public AbstractTritonModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        this.endpoint = config.get(TritonOptions.ENDPOINT);
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

        // Validate input schema - support multiple types
        validateInputSchema(factoryContext.getCatalogModel().getResolvedInputSchema());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.debug("Creating Triton HTTP client.");
        this.httpClient = TritonUtils.createHttpClient(timeout.toMillis());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.httpClient != null) {
            LOG.debug("Releasing Triton HTTP client.");
            TritonUtils.releaseHttpClient(this.httpClient);
            httpClient = null;
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
        if (columns.size() != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Model should have exactly one %s column, but actually has %s columns: %s. "
                                    + "Current version only supports single input/output. "
                                    + "For multi-tensor models, consider using JSON STRING encoding or ARRAY<T> packing.",
                            inputOrOutput,
                            columns.size(),
                            columns.stream().map(Column::getName).collect(Collectors.toList())));
        }

        Column column = columns.get(0);
        if (!column.isPhysical()) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s column %s should be a physical column, but is a %s.",
                            inputOrOutput, column.getName(), column.getClass()));
        }

        if (expectedType != null && !expectedType.equals(column.getDataType().getLogicalType())) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s column %s should be %s, but is a %s.",
                            inputOrOutput,
                            column.getName(),
                            expectedType,
                            column.getDataType().getLogicalType()));
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
            if (elementType instanceof ArrayType) {
                throw new IllegalArgumentException(
                        String.format(
                                "%s column '%s' has nested array type: %s\n"
                                        + "Multi-dimensional tensors (ARRAY<ARRAY<T>>) are not supported in v1.\n"
                                        + "=== Supported Types ===\n"
                                        + "  • Scalars: INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, STRING\n"
                                        + "  • 1-D Arrays: ARRAY<INT>, ARRAY<FLOAT>, ARRAY<DOUBLE>, etc.\n"
                                        + "=== Workarounds ===\n"
                                        + "  • Flatten to 1-D array: ARRAY<FLOAT> with size = rows * cols\n"
                                        + "  • Use JSON STRING encoding for complex structures\n"
                                        + "  • Wait for v2+ which will support ROW<...> types",
                                inputOrOutput, columnName, type));
            }
        }

        // Log info about STRING to BYTES mapping
        if (type instanceof org.apache.flink.table.types.logical.VarCharType) {
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
}
