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

package org.apache.flink.model.qwen;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.openai.sdk.OpenAICompatibleModelFunction;
import org.apache.flink.table.factories.ModelProviderFactory;

import static org.apache.flink.configuration.description.TextElement.code;

/** {@link AbstractQwenModelFunction} for Qwen chat completion task. */
public abstract class AbstractQwenModelFunction extends OpenAICompatibleModelFunction {

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Full URL of the Qwen API endpoint, e.g., %s", // 只剩一个占位符
                                            code(
                                                    "https://dashscope.aliyuncs.com/compatible-mode/v1"))
                                    .build());

    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key("api-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Qwen API key for authentication.");

    public static final ConfigOption<String> MODEL =
            ConfigOptions.key("model")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Model name, e.g., %s, %s.",
                                            code("text-embedding-v4"), code("qwen-plus"))
                                    .build());

    public AbstractQwenModelFunction(
            ModelProviderFactory.Context factoryContext, ReadableConfig config) {
        super(factoryContext, config);
    }
}
