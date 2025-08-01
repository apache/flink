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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import com.openai.client.OpenAIClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.description.TextElement.code;

/** Abstract parent class for {@link AsyncPredictFunction}s for OpenAI API. */
public abstract class AbstractOpenAIModelFunction extends AsyncPredictFunction {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOpenAIModelFunction.class);

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Full URL of the OpenAI API endpoint, e.g., %s or %s",
                                            code("https://api.openai.com/v1/chat/completions"),
                                            code("https://api.openai.com/v1/embeddings"))
                                    .build());

    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key("api-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OpenAI API key for authentication.");

    public static final ConfigOption<String> MODEL =
            ConfigOptions.key("model")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Model name, e.g., %s, %s.",
                                            code("gpt-3.5-turbo"), code("text-embedding-ada-002"))
                                    .build());

    protected transient OpenAIClientAsync client;

    private final int numRetry;
    private final String baseUrl;
    private final String apiKey;

    public AbstractOpenAIModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        String endpoint = config.get(ENDPOINT);
        this.baseUrl = endpoint.replaceAll(String.format("/%s/*$", getEndpointSuffix()), "");
        this.apiKey = config.get(API_KEY);
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
