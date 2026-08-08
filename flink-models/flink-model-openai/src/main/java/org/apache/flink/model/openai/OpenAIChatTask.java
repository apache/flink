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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.types.logical.VarCharType;

import com.openai.models.ResponseFormatJsonObject;
import com.openai.models.ResponseFormatText;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionCreateParams.ResponseFormat;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Per-task state and SDK plumbing for OpenAI chat-completion requests, shared between the async and
 * sync chat model functions. Owns chat-specific configuration (system prompt and the remaining
 * tuning options) and the SDK request/response translation.
 */
class OpenAIChatTask implements Serializable {
    private static final long serialVersionUID = 1L;

    static final String ENDPOINT_SUFFIX = "chat/completions";

    static final String STOP_SEPARATOR = ",";

    private final OpenAIModelCommons commons;
    private final String systemPrompt;
    private final Configuration config;
    private final int outputColumnIndex;

    OpenAIChatTask(
            ModelProviderFactory.Context factoryContext,
            ReadableConfig config,
            OpenAIModelCommons commons) {
        this.commons = commons;
        this.systemPrompt = config.get(OpenAIOptions.SYSTEM_PROMPT);
        this.config = Configuration.fromMap(config.toMap());
        OpenAIModelCommons.validateSingleColumnSchema(
                factoryContext.getCatalogModel().getResolvedOutputSchema(),
                new VarCharType(VarCharType.MAX_LENGTH),
                "output");
        this.outputColumnIndex = commons.resolvePhysicalOutputColumnIndex();
    }

    ChatCompletionCreateParams buildParams(String input) {
        ChatCompletionCreateParams.Builder builder =
                ChatCompletionCreateParams.builder()
                        .addSystemMessage(systemPrompt)
                        .addUserMessage(input)
                        .model(commons.model);
        config.getOptional(OpenAIOptions.TEMPERATURE).ifPresent(builder::temperature);
        config.getOptional(OpenAIOptions.TOP_P).ifPresent(builder::topP);
        config.getOptional(OpenAIOptions.STOP)
                .ifPresent(x -> builder.stopOfStrings(Arrays.asList(x.split(STOP_SEPARATOR))));
        config.getOptional(OpenAIOptions.MAX_TOKENS).ifPresent(builder::maxTokens);
        config.getOptional(OpenAIOptions.PRESENCE_PENALTY).ifPresent(builder::presencePenalty);
        config.getOptional(OpenAIOptions.N).ifPresent(builder::n);
        config.getOptional(OpenAIOptions.SEED).ifPresent(builder::seed);
        config.getOptional(OpenAIOptions.RESPONSE_FORMAT)
                .ifPresent(x -> builder.responseFormat(x.getResponseFormat()));
        return builder.build();
    }

    Collection<RowData> convertToRowData(
            @Nullable ChatCompletion chatCompletion, @Nullable Throwable throwable) {
        if (throwable != null) {
            return commons.handleErrorsAndRespond(throwable);
        }
        return chatCompletion.choices().stream()
                .map(
                        choice -> {
                            GenericRowData rowData =
                                    new GenericRowData(commons.outputColumnNames.size());
                            rowData.setField(
                                    outputColumnIndex,
                                    BinaryStringData.fromString(
                                            choice.message().content().orElse("")));
                            return rowData;
                        })
                .collect(Collectors.toList());
    }

    /**
     * The response format for chat model functions. It's an Enum representation for {@link
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
