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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.Description;
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

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.description.TextElement.code;

/**
 * Abstract parent class for {@link AsyncPredictFunction}s for Triton Inference Server API.
 *
 * <p>This implementation uses REST-based HTTP communication with Triton Inference Server. Each
 * Flink record triggers a separate HTTP request (no Flink-side batching). Triton's server-side
 * dynamic batching can aggregate concurrent requests.
 *
 * <p><b>HTTP Client Lifecycle:</b> A shared HTTP client pool is maintained per JVM with reference
 * counting. Multiple function instances with identical timeout/retry settings share the same client
 * instance to avoid resource exhaustion in high-parallelism scenarios.
 *
 * <p><b>Current Limitations (v1):</b>
 *
 * <ul>
 *   <li>Only single input column and single output column are supported
 *   <li>REST API only; gRPC may be introduced in future versions
 *   <li>Binary data mode is declared but not fully implemented
 * </ul>
 *
 * <p><b>Future Roadmap:</b> Support for multi-input/multi-output models using ROW or MAP types, and
 * native gRPC protocol for improved performance.
 */
public abstract class AbstractTritonModelFunction extends AsyncPredictFunction {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTritonModelFunction.class);

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Full URL of the Triton Inference Server endpoint, e.g., %s",
                                            code("http://localhost:8000/v2/models"))
                                    .build());

    public static final ConfigOption<String> MODEL_NAME =
            ConfigOptions.key("model-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the model to invoke on Triton server.");

    public static final ConfigOption<String> MODEL_VERSION =
            ConfigOptions.key("model-version")
                    .stringType()
                    .defaultValue("latest")
                    .withDescription("Version of the model to use. Defaults to 'latest'.");

    public static final ConfigOption<Long> TIMEOUT =
            ConfigOptions.key("timeout")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "HTTP request timeout in milliseconds (connect + read + write). "
                                    + "This applies per individual request and is separate from Flink's async timeout. "
                                    + "Defaults to 30000ms (30 seconds).");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch-size")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Reserved for future use (v2+). Currently has NO effect in v1. "
                                                    + "Each Flink record triggers one HTTP request regardless of this setting. "
                                                    + "Future versions will support Flink-side mini-batch aggregation "
                                                    + "(buffer N records or T milliseconds before sending). "
                                                    + "For batching efficiency in v1: "
                                                    + "1) Configure Triton model's dynamic_batching in config.pbtxt, "
                                                    + "2) Tune Flink AsyncDataStream capacity for concurrent requests, "
                                                    + "3) Increase Flink parallelism to create more concurrent requests. "
                                                    + "Defaults to 1.")
                                    .build());

    public static final ConfigOption<Boolean> FLATTEN_BATCH_DIM =
            ConfigOptions.key("flatten-batch-dim")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to flatten the batch dimension for array inputs. "
                                    + "When true, shape [1,N] becomes [N]. Defaults to false.");

    public static final ConfigOption<Integer> PRIORITY =
            ConfigOptions.key("priority")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Request priority level (0-255). Higher values indicate higher priority.");

    public static final ConfigOption<String> SEQUENCE_ID =
            ConfigOptions.key("sequence-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Sequence ID for stateful models.");

    public static final ConfigOption<Boolean> SEQUENCE_START =
            ConfigOptions.key("sequence-start")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether this is the start of a sequence for stateful models.");

    public static final ConfigOption<Boolean> SEQUENCE_END =
            ConfigOptions.key("sequence-end")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether this is the end of a sequence for stateful models.");

    public static final ConfigOption<String> COMPRESSION =
            ConfigOptions.key("compression")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Compression algorithm to use (e.g., 'gzip').");

    public static final ConfigOption<String> AUTH_TOKEN =
            ConfigOptions.key("auth-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Authentication token for secured Triton servers.");

    public static final ConfigOption<String> CUSTOM_HEADERS =
            ConfigOptions.key("custom-headers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Custom HTTP headers in JSON format, e.g., '{\"X-Custom-Header\":\"value\"}'.");

    protected transient OkHttpClient httpClient;

    private final String endpoint;
    private final String modelName;
    private final String modelVersion;
    private final long timeout;
    private final int batchSize;
    private final boolean flattenBatchDim;
    private final Integer priority;
    private final String sequenceId;
    private final boolean sequenceStart;
    private final boolean sequenceEnd;
    private final String compression;
    private final String authToken;
    private final String customHeaders;

    public AbstractTritonModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        this.endpoint = config.get(ENDPOINT);
        this.modelName = config.get(MODEL_NAME);
        this.modelVersion = config.get(MODEL_VERSION);
        this.timeout = config.get(TIMEOUT);
        this.batchSize = config.get(BATCH_SIZE);
        this.flattenBatchDim = config.get(FLATTEN_BATCH_DIM);
        this.priority = config.get(PRIORITY);
        this.sequenceId = config.get(SEQUENCE_ID);
        this.sequenceStart = config.get(SEQUENCE_START);
        this.sequenceEnd = config.get(SEQUENCE_END);
        this.compression = config.get(COMPRESSION);
        this.authToken = config.get(AUTH_TOKEN);
        this.customHeaders = config.get(CUSTOM_HEADERS);

        // Validate input schema - support multiple types
        validateInputSchema(factoryContext.getCatalogModel().getResolvedInputSchema());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.debug("Creating Triton HTTP client.");
        this.httpClient = TritonUtils.createHttpClient(timeout);
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

    protected long getTimeout() {
        return timeout;
    }

    protected int getBatchSize() {
        return batchSize;
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

    protected String getCustomHeaders() {
        return customHeaders;
    }
}
