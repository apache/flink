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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.types.logical.VarCharType;

import com.openai.models.ResponseFormatJsonObject;
import com.openai.models.ResponseFormatText;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionCreateParams.ResponseFormat;

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

    public static final ConfigOption<Double> PRESENCE_PENALTY =
            ConfigOptions.key("presence-penalty")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "Number between -2.0 and 2.0."
                                    + " Positive values penalize new tokens based on whether they appear in the text so far,"
                                    + " increasing the model's likelihood to talk about new topics.");

    public static final ConfigOption<Long> N =
            ConfigOptions.key("n")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "How many chat completion choices to generate for each input message."
                                    + " Note that you will be charged based on the number of generated tokens across all of the choices."
                                    + " Keep n as 1 to minimize costs.");

    public static final ConfigOption<Long> SEED =
            ConfigOptions.key("seed")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "If specified, the model platform will make a best effort to sample deterministically,"
                                    + " such that repeated requests with the same seed and parameters should return the same result."
                                    + " Determinism is not guaranteed.");

    public static final ConfigOption<ChatModelResponseFormat> RESPONSE_FORMAT =
            ConfigOptions.key("response-format")
                    .enumType(ChatModelResponseFormat.class)
                    .noDefaultValue()
                    .withDescription("The format of the response, e.g., 'text' or 'json_object'.");

    private final String model;
    private final String systemPrompt;
    private final Configuration config;

    public OpenAIChatModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        super(factoryContext, config);
        model = config.get(MODEL);
        systemPrompt = config.get(SYSTEM_PROMPT);
        this.config = Configuration.fromMap(config.toMap());
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
    public CompletableFuture<Collection<RowData>> asyncPredictInternal(String input) {
        ChatCompletionCreateParams.Builder builder =
                ChatCompletionCreateParams.builder()
                        .addSystemMessage(systemPrompt)
                        .addUserMessage(input)
                        .model(model);
        this.config.getOptional(TEMPERATURE).ifPresent(builder::temperature);
        this.config.getOptional(TOP_P).ifPresent(builder::topP);
        this.config
                .getOptional(STOP)
                .ifPresent(x -> builder.stopOfStrings(Arrays.asList(x.split(STOP_SEPARATOR))));
        this.config.getOptional(MAX_TOKENS).ifPresent(builder::maxTokens);
        this.config.getOptional(PRESENCE_PENALTY).ifPresent(builder::presencePenalty);
        this.config.getOptional(N).ifPresent(builder::n);
        this.config.getOptional(SEED).ifPresent(builder::seed);
        this.config
                .getOptional(RESPONSE_FORMAT)
                .ifPresent(x -> builder.responseFormat(x.getResponseFormat()));

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

    /**
     * The response format for Chat model function. It's an Enum representation for {@link
     * ResponseFormat}.
     */
    public enum ChatModelResponseFormat {
        TEXT("text") {
            @Override
            public ResponseFormat getResponseFormat() {
                return ResponseFormat.ofText(ResponseFormatText.builder().build());
            }
        },
        JSON_OBJECT("json_object") {
            @Override
            public ResponseFormat getResponseFormat() {
                return ResponseFormat.ofJsonObject(ResponseFormatJsonObject.builder().build());
            }
        };

        private final String value;

        ChatModelResponseFormat(String value) {
            this.value = value;
        }

        public abstract ResponseFormat getResponseFormat();

        @Override
        public String toString() {
            return value;
        }
    }
}
