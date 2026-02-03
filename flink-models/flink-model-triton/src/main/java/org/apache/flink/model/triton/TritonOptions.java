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
import org.apache.flink.configuration.description.LinkElement;

import java.time.Duration;
import java.util.Map;

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
                                            "Full URL of the Triton Inference Server endpoint, e.g., %s. "
                                                    + "Both HTTP and HTTPS are supported; HTTPS is recommended for production.",
                                            code("https://triton-server:8000/v2/models"))
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

    public static final ConfigOption<Duration> TIMEOUT =
            ConfigOptions.key("timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "HTTP request timeout (connect + read + write). "
                                    + "This applies per individual request and is separate from Flink's async timeout. "
                                    + "Defaults to 30 seconds.");

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
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Sequence ID for stateful models. A sequence represents a series of "
                                                    + "inference requests that must be routed to the same model instance "
                                                    + "to maintain state across requests (e.g., for RNN/LSTM models). "
                                                    + "See %s for more details.",
                                            LinkElement.link(
                                                    "https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/architecture.html#stateful-models",
                                                    "Triton Stateful Models"))
                                    .build());

    public static final ConfigOption<Boolean> SEQUENCE_START =
            ConfigOptions.key("sequence-start")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether this request marks the start of a new sequence for stateful models. "
                                                    + "When true, Triton will initialize the model's state before processing this request. "
                                                    + "See %s for more details.",
                                            LinkElement.link(
                                                    "https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/architecture.html#stateful-models",
                                                    "Triton Stateful Models"))
                                    .build());

    public static final ConfigOption<Boolean> SEQUENCE_END =
            ConfigOptions.key("sequence-end")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether this request marks the end of a sequence for stateful models. "
                                                    + "When true, Triton will release the model's state after processing this request. "
                                                    + "See %s for more details.",
                                            LinkElement.link(
                                                    "https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/architecture.html#stateful-models",
                                                    "Triton Stateful Models"))
                                    .build());

    public static final ConfigOption<String> COMPRESSION =
            ConfigOptions.key("compression")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Compression algorithm for request body. Currently only %s is supported. "
                                                    + "When enabled, the request body will be compressed to reduce network bandwidth.",
                                            code("gzip"))
                                    .build());

    public static final ConfigOption<String> AUTH_TOKEN =
            ConfigOptions.key("auth-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Authentication token for secured Triton servers.");

    public static final ConfigOption<Map<String, String>> CUSTOM_HEADERS =
            ConfigOptions.key("custom-headers")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Custom HTTP headers as key-value pairs. "
                                                    + "Example: %s",
                                            code("'X-Custom-Header:value,X-Another:value2'"))
                                    .build());
}
