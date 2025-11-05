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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.description.TextElement.code;

/** Options for OpenAI API Model Functions. */
@Experimental
public class OpenAIOptions {

    // ------------------------------------------------------------------------
    //  Common Options
    // ------------------------------------------------------------------------

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
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

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key("api-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("OpenAI API key for authentication.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
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

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<Integer> MAX_CONTEXT_SIZE =
            ConfigOptions.key("max-context-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Max number of tokens for context. context-overflow-action would be triggered if this threshold is exceeded.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<ContextOverflowAction> CONTEXT_OVERFLOW_ACTION =
            ConfigOptions.key("context-overflow-action")
                    .enumType(ContextOverflowAction.class)
                    .defaultValue(ContextOverflowAction.TRUNCATED_TAIL)
                    .withDescription(
                            Description.builder()
                                    .text("Action to handle context overflows.")
                                    .build());

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<AbstractOpenAIModelFunction.ErrorHandlingStrategy>
            ERROR_HANDLING_STRATEGY =
                    ConfigOptions.key("error-handling-strategy")
                            .enumType(AbstractOpenAIModelFunction.ErrorHandlingStrategy.class)
                            .defaultValue(AbstractOpenAIModelFunction.ErrorHandlingStrategy.RETRY)
                            .withDescription("Strategy for handling errors during model requests.");

    // The model service enforces rate-limiting constraints, necessitating retry mechanisms in
    // most operational scenarios.
    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<Integer> RETRY_NUM =
            ConfigOptions.key("retry-num")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Number of retry for OpenAI client requests.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_COMMON})
    public static final ConfigOption<AbstractOpenAIModelFunction.RetryFallbackStrategy>
            RETRY_FALLBACK_STRATEGY =
                    ConfigOptions.key("retry-fallback-strategy")
                            .enumType(AbstractOpenAIModelFunction.RetryFallbackStrategy.class)
                            .defaultValue(
                                    AbstractOpenAIModelFunction.RetryFallbackStrategy.FAILOVER)
                            .withDescription(
                                    "Fallback strategy to employ if the retry attempts are exhausted."
                                            + " This strategy is applied when error-handling-strategy is set to retry.");

    // ------------------------------------------------------------------------
    // Options for Chat Completion Model Functions
    // ------------------------------------------------------------------------

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<String> SYSTEM_PROMPT =
            ConfigOptions.key("system-prompt")
                    .stringType()
                    .defaultValue("You are a helpful assistant.")
                    .withDeprecatedKeys("systemPrompt")
                    .withDescription("The system message of a chat.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<Double> TEMPERATURE =
            ConfigOptions.key("temperature")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "Controls the randomness or “creativity” of the output. Typical values are between 0.0 and 1.0.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<Double> TOP_P =
            ConfigOptions.key("top-p")
                    .doubleType()
                    .noDefaultValue()
                    .withDeprecatedKeys("topP")
                    .withDescription(
                            "The probability cutoff for token selection. Usually, either temperature or topP are specified, but not both.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<String> STOP =
            ConfigOptions.key("stop")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A CSV list of strings to pass as stop sequences to the model.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<Long> MAX_TOKENS =
            ConfigOptions.key("max-tokens")
                    .longType()
                    .noDefaultValue()
                    .withDeprecatedKeys("maxTokens")
                    .withDescription(
                            "The maximum number of tokens that can be generated in the chat completion.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<Double> PRESENCE_PENALTY =
            ConfigOptions.key("presence-penalty")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "Number between -2.0 and 2.0."
                                    + " Positive values penalize new tokens based on whether they appear in the text so far,"
                                    + " increasing the model's likelihood to talk about new topics.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<Long> N =
            ConfigOptions.key("n")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "How many chat completion choices to generate for each input message."
                                    + " Note that you will be charged based on the number of generated tokens across all of the choices."
                                    + " Keep n as 1 to minimize costs.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<Long> SEED =
            ConfigOptions.key("seed")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "If specified, the model platform will make a best effort to sample deterministically,"
                                    + " such that repeated requests with the same seed and parameters should return the same result."
                                    + " Determinism is not guaranteed.");

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_CHAT})
    public static final ConfigOption<OpenAIChatModelFunction.ChatModelResponseFormat>
            RESPONSE_FORMAT =
                    ConfigOptions.key("response-format")
                            .enumType(OpenAIChatModelFunction.ChatModelResponseFormat.class)
                            .noDefaultValue()
                            .withDescription(
                                    "The format of the response, e.g., 'text' or 'json_object'.");

    // ------------------------------------------------------------------------
    // Options for Embedding Model Functions
    // ------------------------------------------------------------------------

    @Documentation.Section({Documentation.Sections.MODEL_OPENAI_EMBEDDING})
    public static final ConfigOption<Long> DIMENSION =
            ConfigOptions.key("dimension")
                    .longType()
                    .noDefaultValue()
                    .withDescription("The size of the embedding result array.");
}
