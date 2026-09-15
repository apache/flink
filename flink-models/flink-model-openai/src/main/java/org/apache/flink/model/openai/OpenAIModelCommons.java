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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * State and behaviour shared between the asynchronous and synchronous OpenAI model functions:
 * configuration extracted from {@link OpenAIOptions}, output column metadata, input pre-processing
 * via {@link ContextOverflowAction}, and error handling per {@link ErrorHandlingStrategy} / {@link
 * RetryFallbackStrategy}.
 */
class OpenAIModelCommons implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(OpenAIModelCommons.class);

    final String baseUrl;
    final String apiKey;
    final String model;
    final int numRetry;
    final ErrorHandlingStrategy errorHandlingStrategy;
    final RetryFallbackStrategy retryFallbackStrategy;
    @Nullable final Integer maxContextSize;
    final ContextOverflowAction contextOverflowAction;
    final List<String> outputColumnNames;

    OpenAIModelCommons(
            ModelProviderFactory.Context factoryContext,
            ReadableConfig config,
            String endpointSuffix) {
        String endpoint = config.get(OpenAIOptions.ENDPOINT);
        this.baseUrl = endpoint.replaceAll(String.format("/%s/*$", endpointSuffix), "");
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

    /** Initializes per-task state at task-open time. Idempotent and cheap if called again. */
    void initializeAtOpen() {
        contextOverflowAction.initializeEncodingForContextLimit(model, maxContextSize);
    }

    /**
     * Returns the index of the single non-metadata physical output column among the model's output
     * columns. Schema validation has already guaranteed exactly one such column exists.
     */
    int resolvePhysicalOutputColumnIndex() {
        for (int i = 0; i < outputColumnNames.size(); i++) {
            if (ErrorMessageMetadata.get(outputColumnNames.get(i)) == null) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "There should be one and only one physical output column. Actual columns: "
                        + outputColumnNames);
    }

    /**
     * Pre-processes the single string input column, applying the configured context-overflow
     * action. Returns {@code null} when the input row's value is null or when the context-overflow
     * action drops the input.
     */
    @Nullable
    String prepareInput(RowData rowData) {
        if (rowData.isNullAt(0)) {
            LOG.warn("Input is null, skipping prediction.");
            return null;
        }
        return contextOverflowAction.processTokensWithLimit(
                model, rowData.getString(0).toString(), maxContextSize);
    }

    static void validateSingleColumnSchema(
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

    /**
     * Applies the configured error-handling strategy: rethrows on FAILOVER, returns a populated
     * error row (or empty) on IGNORE. Used identically by the async and sync paths.
     */
    java.util.Collection<RowData> handleErrorsAndRespond(Throwable t) {
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
}
