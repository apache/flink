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

package org.apache.flink.model.openai;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.types.logical.VarCharType;

import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** {@link AsyncPredictFunction} for OpenAI chat completion task. */
public class OpenAIChatModelFunction extends AbstractOpenAIModelFunction {
    private static final long serialVersionUID = 1L;

    public static final String ENDPOINT_SUFFIX = "chat/completions";

    public static final ConfigOption<String> SYSTEM_PROMPT =
            ConfigOptions.key("system-prompt")
                    .stringType()
                    .defaultValue("You are a helpful assistant.")
                    .withDescription("System message for chat tasks.");

    public static final ConfigOption<Double> TEMPERATURE =
            ConfigOptions.key("temperature")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("Controls randomness of output, range [0.0, 1.0].");

    public static final ConfigOption<Double> TOP_P =
            ConfigOptions.key("top-p")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "Probability cutoff for token selection (used instead of temperature).");

    public static final String STOP_SEPARATOR = ",";

    public static final ConfigOption<String> STOP =
            ConfigOptions.key("stop")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stop sequences, comma-separated list.");

    public static final ConfigOption<Long> MAX_TOKENS =
            ConfigOptions.key("max-tokens")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Maximum number of tokens to generate.");

    private final String model;
    private final String systemPrompt;
    @Nullable private final Double temperature;
    @Nullable private final Double topP;
    @Nullable private final List<String> stop;
    @Nullable private final Long maxTokens;

    public OpenAIChatModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        super(factoryContext, config);
        model = config.get(MODEL);
        systemPrompt = config.get(SYSTEM_PROMPT);
        temperature = config.get(TEMPERATURE);
        topP = config.get(TOP_P);
        stop =
                config.get(STOP) == null
                        ? null
                        : Arrays.asList(config.get(STOP).split(STOP_SEPARATOR));
        maxTokens = config.get(MAX_TOKENS);
        validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedOutputSchema(),
                new VarCharType(VarCharType.MAX_LENGTH),
                "output");
    }

    @Override
    protected String getEndpointSuffix() {
        return ENDPOINT_SUFFIX;
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncPredict(RowData rowData) {
        ChatCompletionCreateParams.Builder builder =
                ChatCompletionCreateParams.builder()
                        .addSystemMessage(systemPrompt)
                        .addUserMessage(rowData.getString(0).toString())
                        .model(model);
        if (temperature != null) {
            builder.temperature(temperature);
        }
        if (topP != null) {
            builder.topP(topP);
        }
        if (stop != null) {
            builder.stopOfStrings(stop);
        }
        if (maxTokens != null) {
            builder.maxTokens(maxTokens);
        }

        return client.chat()
                .completions()
                .create(builder.build())
                .thenApply(this::convertToRowData);
    }

    private List<RowData> convertToRowData(ChatCompletion chatCompletion) {
        return chatCompletion.choices().stream()
                .map(
                        choice ->
                                GenericRowData.of(
                                        BinaryStringData.fromString(
                                                choice.message().content().orElse(""))))
                .collect(Collectors.toList());
    }
}
