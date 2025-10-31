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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import com.openai.client.OpenAIClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Abstract parent class for {@link AsyncPredictFunction}s for OpenAI API. */
public abstract class AbstractOpenAIModelFunction extends AsyncPredictFunction {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOpenAIModelFunction.class);

    protected transient OpenAIClientAsync client;

    private final int numRetry;
    private final String baseUrl;
    private final String apiKey;
    private final String model;
    @Nullable private final Integer maxContextSize;
    private final ContextOverflowAction contextOverflowAction;

    public AbstractOpenAIModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        String endpoint = config.get(OpenAIOptions.ENDPOINT);
        this.baseUrl = endpoint.replaceAll(String.format("/%s/*$", getEndpointSuffix()), "");
        this.apiKey = config.get(OpenAIOptions.API_KEY);
        // The model service enforces rate-limiting constraints, necessitating retry mechanisms in
        // most operational scenarios. Within the asynchronous operator framework, the system is
        // designed to process up to
        // config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY) concurrent
        // requests in parallel. To mitigate potential performance degradation from simultaneous
        // requests, a dynamic retry strategy is implemented where the maximum retry count is
        // directly proportional to the configured parallelism level, ensuring robust error
        // resilience while maintaining throughput efficiency.
        this.numRetry =
                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY) * 10;
        this.model = config.get(OpenAIOptions.MODEL);
        this.maxContextSize = config.get(OpenAIOptions.MAX_CONTEXT_SIZE);
        this.contextOverflowAction = config.get(OpenAIOptions.CONTEXT_OVERFLOW_ACTION);

        validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedInputSchema(),
                new VarCharType(VarCharType.MAX_LENGTH),
                "input");
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
        if (columns.size() != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Model should have exactly one %s column, but actually has %s columns: %s",
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

        if (!expectedType.equals(column.getDataType().getLogicalType())) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s column %s should be %s, but is a %s.",
                            inputOrOutput,
                            column.getName(),
                            expectedType,
                            column.getDataType().getLogicalType()));
        }
    }
}
