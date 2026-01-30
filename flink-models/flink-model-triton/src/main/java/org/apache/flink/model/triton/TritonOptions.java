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
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.description.TextElement.code;

/** Configuration options for Triton Inference Server model functions. */
public class TritonOptions {

    private TritonOptions() {
        // Utility class with static options only
    }

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
}
