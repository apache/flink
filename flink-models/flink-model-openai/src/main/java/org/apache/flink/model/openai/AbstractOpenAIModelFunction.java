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

package org.apache.flink.model.openai;

import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import com.openai.client.OpenAIClientAsync;
import com.openai.core.http.Headers;
import com.openai.errors.OpenAIServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.description.TextElement.text;

/** Abstract parent class for {@link AsyncPredictFunction}s for OpenAI API. */
public abstract class AbstractOpenAIModelFunction extends AsyncPredictFunction {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOpenAIModelFunction.class);

    protected transient OpenAIClientAsync client;

    private final ErrorHandlingStrategy errorHandlingStrategy;
    private final int numRetry;
    private final RetryFallbackStrategy retryFallbackStrategy;
    private final String baseUrl;
    private final String apiKey;
    private final String model;
    @Nullable private final Integer maxContextSize;
    private final ContextOverflowAction contextOverflowAction;
    protected final List<String> outputColumnNames;

    public AbstractOpenAIModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        String endpoint = config.get(OpenAIOptions.ENDPOINT);
        this.baseUrl = endpoint.replaceAll(String.format("/%s/*$", getEndpointSuffix()), "");
        this.apiKey = config.get(OpenAIOptions.API_KEY);

        this.errorHandlingStrategy = config.get(OpenAIOptions.ERROR_HANDLING_STRATEGY);
        this.numRetry =
                this.errorHandlingStrategy == ErrorHandlingStrategy.RETRY
                        ? config.get(OpenAIOptions.RETRY_NUM)
                        : 0;
        this.model = config.get(OpenAIOptions.MODEL);
        this.maxContextSize = config.get(OpenAIOptions.MAX_CONTEXT_SIZE);
        this.contextOverflowAction = config.get(OpenAIOptions.CONTEXT_OVERFLOW_ACTION);
        this.retryFallbackStrategy = config.get(OpenAIOptions.RETRY_FALLBACK_STRATEGY);

        validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedInputSchema(),
                new VarCharType(VarCharType.MAX_LENGTH),
                "input");

        this.outputColumnNames =
                factoryContext.getCatalogModel().getResolvedOutputSchema().getColumnNames();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.debug("Creating an OpenAI client.");
        this.client = OpenAIUtils.createAsyncClient(baseUrl, apiKey, numRetry);
        this.contextOverflowAction.initializeEncodingForContextLimit(model, maxContextSize);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncPredict(RowData rowData) {
        if (rowData.isNullAt(0)) {
            LOG.warn("Input is null, skipping prediction.");
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        String input =
                contextOverflowAction.processTokensWithLimit(
                        model, rowData.getString(0).toString(), maxContextSize);
        if (input == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return asyncPredictInternal(input);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.client != null) {
            LOG.debug("Releasing the OpenAI client.");
            OpenAIUtils.releaseAsyncClient(baseUrl, apiKey);
            client = null;
        }
    }

    protected abstract String getEndpointSuffix();

    protected abstract CompletableFuture<Collection<RowData>> asyncPredictInternal(String input);

    protected void validateSingleColumnSchema(
            ResolvedSchema schema, LogicalType expectedType, String inputOrOutput) {
        List<Column> columns = schema.getColumns();
        List<String> physicalColumnNames =
                columns.stream()
                        .filter(Column::isPhysical)
                        .map(Column::getName)
                        .collect(Collectors.toList());
        if (physicalColumnNames.size() != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Model should have exactly one %s physical column, but actually has %s physical columns: %s",
                            inputOrOutput, physicalColumnNames.size(), physicalColumnNames));
        }

        Column column = schema.getColumn(physicalColumnNames.get(0)).get();
        if (!expectedType.equals(column.getDataType().getLogicalType())) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s column %s should be %s, but is a %s.",
                            inputOrOutput,
                            column.getName(),
                            expectedType,
                            column.getDataType().getLogicalType()));
        }

        List<Column> metadataColumns =
                columns.stream()
                        .filter(x -> x instanceof Column.MetadataColumn)
                        .collect(Collectors.toList());
        if (!metadataColumns.isEmpty()) {
            Preconditions.checkArgument(
                    "output".equals(inputOrOutput), "Only output schema supports metadata column");

            for (Column metadataColumn : metadataColumns) {
                ErrorMessageMetadata errorMessageMetadata =
                        ErrorMessageMetadata.get(metadataColumn.getName());
                Preconditions.checkNotNull(
                        errorMessageMetadata,
                        String.format(
                                "Unexpected metadata column %s. Supported metadata columns:\n%s",
                                metadataColumn.getName(),
                                ErrorMessageMetadata.getAllKeysAndDescriptions()));
                Preconditions.checkArgument(
                        errorMessageMetadata.dataType.equals(metadataColumn.getDataType()),
                        String.format(
                                "Expected metadata column %s to be of type %s, but is of type %s",
                                metadataColumn.getName(),
                                errorMessageMetadata.dataType,
                                metadataColumn.getDataType()));
            }
        }
    }

    protected Collection<RowData> handleErrorsAndRespond(Throwable t) {
        ErrorHandlingStrategy finalErrorHandlingStrategy =
                this.errorHandlingStrategy == ErrorHandlingStrategy.RETRY
                        ? this.retryFallbackStrategy.strategy
                        : this.errorHandlingStrategy;

        if (finalErrorHandlingStrategy == ErrorHandlingStrategy.FAILOVER) {
            throw new RuntimeException(t);
        } else if (finalErrorHandlingStrategy == ErrorHandlingStrategy.IGNORE) {
            LOG.warn(
                    "The input row data failed to acquire a valid response. Ignoring the input.",
                    t);
            GenericRowData rowData = new GenericRowData(this.outputColumnNames.size());
            boolean isMetadataSet = false;
            for (int i = 0; i < this.outputColumnNames.size(); i++) {
                String columnName = this.outputColumnNames.get(i);
                ErrorMessageMetadata errorMessageMetadata = ErrorMessageMetadata.get(columnName);
                if (errorMessageMetadata != null) {
                    rowData.setField(i, errorMessageMetadata.converter.apply(t));
                    isMetadataSet = true;
                }
            }
            return isMetadataSet ? Collections.singletonList(rowData) : Collections.emptyList();
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported error handling strategy: " + finalErrorHandlingStrategy);
        }
    }

    /** Strategy for handling errors during model requests. */
    public enum ErrorHandlingStrategy implements DescribedEnum {
        RETRY("Retry sending the request."),
        FAILOVER("Throw exceptions and fail the Flink job."),
        IGNORE(
                "Ignore the input that caused the error and continue. The error itself would be recorded in log.");

        private final String description;

        ErrorHandlingStrategy(String description) {
            this.description = description;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /**
     * The fallback strategy for when retry attempts are exhausted. It should be identical to {@link
     * ErrorHandlingStrategy} except that it does not support {@link ErrorHandlingStrategy#RETRY}.
     */
    public enum RetryFallbackStrategy implements DescribedEnum {
        FAILOVER(ErrorHandlingStrategy.FAILOVER),
        IGNORE(ErrorHandlingStrategy.IGNORE);
        private final ErrorHandlingStrategy strategy;

        RetryFallbackStrategy(ErrorHandlingStrategy strategy) {
            this.strategy = strategy;
        }

        @Override
        public InlineElement getDescription() {
            return text(strategy.description);
        }
    }

    /**
     * Metadata that can be read from the output row about error messages. Referenced from Flink
     * HTTP Connector's ReadableMetadata.
     */
    protected enum ErrorMessageMetadata {
        ERROR_STRING(
                "error-string",
                DataTypes.STRING(),
                x -> BinaryStringData.fromString(x.getMessage()),
                "A message associated with the error"),
        HTTP_STATUS_CODE(
                "http-status-code",
                DataTypes.INT(),
                e ->
                        ExceptionUtils.findThrowable(e, OpenAIServiceException.class)
                                .map(OpenAIServiceException::statusCode)
                                .orElse(null),
                "The HTTP status code"),
        HTTP_HEADERS_MAP(
                "http-headers-map",
                DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())),
                e ->
                        ExceptionUtils.findThrowable(e, OpenAIServiceException.class)
                                .map(
                                        e1 -> {
                                            Map<StringData, ArrayData> map = new HashMap<>();
                                            Headers headers = e1.headers();
                                            for (String name : headers.names()) {
                                                map.put(
                                                        BinaryStringData.fromString(name),
                                                        new GenericArrayData(
                                                                headers.values(name).stream()
                                                                        .map(
                                                                                BinaryStringData
                                                                                        ::fromString)
                                                                        .toArray()));
                                            }
                                            return new GenericMapData(map);
                                        })
                                .orElse(null),
                "The headers returned with the response");

        final String key;
        final DataType dataType;
        final Function<Throwable, Object> converter;
        final String description;

        ErrorMessageMetadata(
                String key,
                DataType dataType,
                Function<Throwable, Object> converter,
                String description) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
            this.description = description;
        }

        static @Nullable ErrorMessageMetadata get(String key) {
            for (ErrorMessageMetadata value : values()) {
                if (value.key.equals(key)) {
                    return value;
                }
            }
            return null;
        }

        static String getAllKeysAndDescriptions() {
            return Arrays.stream(values())
                    .map(value -> value.key + ":\t" + value.description)
                    .collect(Collectors.joining("\n"));
        }
    }
}
