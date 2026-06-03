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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.LinkElement;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.configuration.description.TextElement.code;

/** Configuration options for Triton Inference Server model functions. */
@Experimental
public class TritonOptions {

    private TritonOptions() {
        // Utility class with static options only
    }

    @Documentation.Section(
            value = {Documentation.Sections.MODEL_TRITON_COMMON},
            position = 1)
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

    @Documentation.Section(
            value = {Documentation.Sections.MODEL_TRITON_COMMON},
            position = 2)
    public static final ConfigOption<String> MODEL_NAME =
            ConfigOptions.key("model-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the model to invoke on Triton server.");

    @Documentation.Section(
            value = {Documentation.Sections.MODEL_TRITON_COMMON},
            position = 3)
    public static final ConfigOption<String> MODEL_VERSION =
            ConfigOptions.key("model-version")
                    .stringType()
                    .defaultValue("latest")
                    .withDescription("Version of the model to use. Defaults to 'latest'.");

    @Documentation.Section(
            value = {Documentation.Sections.MODEL_TRITON_COMMON},
            position = 4)
    public static final ConfigOption<Duration> TIMEOUT =
            ConfigOptions.key("timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "HTTP request timeout (connect + read + write). "
                                    + "This applies per individual request and is separate from Flink's async timeout. "
                                    + "Defaults to 30 seconds.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Boolean> FLATTEN_BATCH_DIM =
            ConfigOptions.key("flatten-batch-dim")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to flatten the batch dimension for array inputs. "
                                    + "When true, shape [1,N] becomes [N]. Defaults to false.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Integer> PRIORITY =
            ConfigOptions.key("priority")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Request priority level (0-255). Higher values indicate higher priority.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
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

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
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

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
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

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
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

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<String> AUTH_TOKEN =
            ConfigOptions.key("auth-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Authentication token for secured Triton servers.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
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

    // ==================== Health Check and Circuit Breaker Options ====================

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Boolean> HEALTH_CHECK_ENABLED =
            ConfigOptions.key("health-check-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to enable periodic health checks for the Triton server. "
                                                    + "When enabled, the health checker will periodically call %s endpoint "
                                                    + "to verify server availability. Defaults to false.",
                                            code("/v2/health/live"))
                                    .build());

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Duration> HEALTH_CHECK_INTERVAL =
            ConfigOptions.key("health-check-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Interval between health check requests. "
                                    + "Shorter intervals provide faster failure detection but increase server load. "
                                    + "Defaults to 30 seconds. Only effective when health-check-enabled is true.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Boolean> CIRCUIT_BREAKER_ENABLED =
            ConfigOptions.key("circuit-breaker-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to enable circuit breaker protection. "
                                                    + "When enabled, the circuit breaker will automatically fail fast when the server "
                                                    + "is unhealthy, preventing cascading failures and reducing load on the failing server. "
                                                    + "The circuit breaker implements a three-state model: CLOSED (normal), OPEN (failing fast), "
                                                    + "and HALF_OPEN (testing recovery). Defaults to false.")
                                    .build());

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Double> CIRCUIT_BREAKER_FAILURE_THRESHOLD =
            ConfigOptions.key("circuit-breaker-failure-threshold")
                    .doubleType()
                    .defaultValue(0.5)
                    .withDescription(
                            "Failure rate threshold (0.0-1.0) that triggers the circuit breaker to open. "
                                    + "For example, 0.5 means the circuit will open when 50% of recent requests fail. "
                                    + "Requires a minimum of 10 requests before evaluation. Defaults to 0.5 (50%). "
                                    + "Only effective when circuit-breaker-enabled is true.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Duration> CIRCUIT_BREAKER_TIMEOUT =
            ConfigOptions.key("circuit-breaker-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "Duration to keep the circuit breaker in OPEN state before transitioning to HALF_OPEN. "
                                    + "In HALF_OPEN state, limited requests are allowed to probe if the server has recovered. "
                                    + "Defaults to 60 seconds. Only effective when circuit-breaker-enabled is true.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Integer> CIRCUIT_BREAKER_HALF_OPEN_REQUESTS =
            ConfigOptions.key("circuit-breaker-half-open-requests")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "Number of successful test requests required in HALF_OPEN state to close the circuit. "
                                    + "If any request fails in HALF_OPEN state, the circuit immediately reopens. "
                                    + "Defaults to 3 requests. Only effective when circuit-breaker-enabled is true.");

    // ==================== Retry and Default Value Fallback Options ====================

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Maximum number of retries (additional attempts beyond the first) "
                                    + "for failed inference requests. With max-retries=2 the "
                                    + "request will be attempted up to 3 times in total "
                                    + "(1 initial attempt + 2 retries). When set to 0 (default), "
                                    + "no retry is performed. Only transient failures are "
                                    + "retried: network errors and 5xx responses. Client-side "
                                    + "4xx errors, response parsing failures, and circuit "
                                    + "breaker OPEN failures are never retried because they "
                                    + "indicate a persistent condition. Must be >= 0.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Duration> RETRY_INITIAL_BACKOFF =
            ConfigOptions.key("retry-initial-backoff")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription(
                            "Initial backoff duration between retry attempts. Uses exponential "
                                    + "backoff with equal jitter: the nominal delay is "
                                    + "initial-backoff * 2^attempt (first retry waits this "
                                    + "duration, second retry waits 2x, third waits 4x, and so "
                                    + "on), clamped to retry-max-backoff, then randomized in "
                                    + "the range [delay/2, delay] to prevent a thundering herd "
                                    + "of concurrent retries hitting the server at the exact "
                                    + "same instant. Defaults to 100ms. Must be > 0.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<Duration> RETRY_MAX_BACKOFF =
            ConfigOptions.key("retry-max-backoff")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Upper bound on the delay between retry attempts. Exponential "
                                    + "backoff computed from retry-initial-backoff is clamped "
                                    + "to this value so that a misconfigured max-retries "
                                    + "cannot produce hours-long sleeps or overflow the delay "
                                    + "computation. Defaults to 30s. Must be >= "
                                    + "retry-initial-backoff.");

    @Documentation.Section({Documentation.Sections.MODEL_TRITON_ADVANCED})
    public static final ConfigOption<String> DEFAULT_VALUE =
            ConfigOptions.key("default-value")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Fallback value to return when all retry attempts "
                                                    + "fail (transient errors) or when the request "
                                                    + "fails with a non-retryable error (4xx). "
                                                    + "This allows downstream processing to "
                                                    + "distinguish between successful and failed "
                                                    + "predictions without propagating exceptions. "
                                                    + "Format depends on output type: for STRING "
                                                    + "use plain text (e.g. %s); for numeric types "
                                                    + "use string representation (e.g. %s); for "
                                                    + "ARRAY types use JSON array format (e.g. %s); "
                                                    + "for SQL NULL use the literal %s. "
                                                    + "Note: the lower-case literal %s is ALWAYS "
                                                    + "interpreted as SQL NULL and cannot be used "
                                                    + "as a STRING sentinel; if you need a "
                                                    + "string-typed sentinel indicating failure, "
                                                    + "use %s, %s or %s instead. The value "
                                                    + "is parsed once at operator initialization; "
                                                    + "an unparseable value fails the job at "
                                                    + "startup rather than at the first error. "
                                                    + "If not specified, exceptions are thrown on "
                                                    + "failure.",
                                            code("'FAILED'"),
                                            code("'-1'"),
                                            code("'[0.0, 0.0]'"),
                                            code("'null'"),
                                            code("'null'"),
                                            code("'NULL'"),
                                            code("'FAILED'"),
                                            code("'<null>'"))
                                    .build());
}
