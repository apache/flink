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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** Configuration parameters for REST communication. */
@Internal
public class RestOptions {

    private static final String REST_PORT_KEY = "rest.port";

    /** The address that the server binds itself to. */
    @Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
    public static final ConfigOption<String> BIND_ADDRESS =
            key("rest.bind-address")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(WebOptions.ADDRESS.key())
                    .withDeprecatedKeys(
                            ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_ADDRESS.key())
                    .withDescription("The address that the server binds itself.");

    /** The port range that the server could bind itself to. */
    @Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
    public static final ConfigOption<String> BIND_PORT =
            key("rest.bind-port")
                    .stringType()
                    .defaultValue("8081")
                    .withFallbackKeys(REST_PORT_KEY)
                    .withDeprecatedKeys(
                            WebOptions.PORT.key(), ConfigConstants.JOB_MANAGER_WEB_PORT_KEY)
                    .withDescription(
                            "The port that the server binds itself. Accepts a list of ports (“50100,50101”), ranges"
                                    + " (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid"
                                    + " collisions when multiple Rest servers are running on the same machine.");

    /** The url prefix that should be used by clients to construct the full target url. */
    public static final ConfigOption<String> URL_PREFIX =
            key("rest.url-prefix")
                    .stringType()
                    .defaultValue("/")
                    .withDescription(
                            "The url prefix that should be used by clients to construct the full target url, must start and end with '/'."
                                    + " This will be added between the address and version prefix. For example, if the option is set to '/foo/',"
                                    + " the overview query URL will be transformed to 'localhost:8081/foo/v1/overview'."
                                    + " Attention: This option is respected only if the high-availability configuration is NONE.");

    /** The address that should be used by clients to connect to the server. */
    @Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
    public static final ConfigOption<String> ADDRESS =
            key("rest.address")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(JobManagerOptions.ADDRESS.key())
                    .withDescription(
                            "The address that should be used by clients to connect to the server. Attention: This option is respected only if the high-availability configuration is NONE.");

    /** The path that should be used by clients to interact with the server. */
    @Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
    public static final ConfigOption<String> PATH =
            key("rest.path")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "The path that should be used by clients to interact to the server which is accessible via URL.");

    /**
     * The port that the REST client connects to and the REST server binds to if {@link #BIND_PORT}
     * has not been specified.
     */
    @Documentation.Section(Documentation.Sections.COMMON_HOST_PORT)
    public static final ConfigOption<Integer> PORT =
            key(REST_PORT_KEY)
                    .intType()
                    .defaultValue(8081)
                    .withDeprecatedKeys(WebOptions.PORT.key())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The port that the client connects to. If %s has not been specified, then the REST server will bind to this port. Attention: This option is respected only if the high-availability configuration is NONE.",
                                            text(BIND_PORT.key()))
                                    .build());

    /**
     * The time in ms that the client waits for the leader address, e.g., Dispatcher or
     * WebMonitorEndpoint.
     */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Long> AWAIT_LEADER_TIMEOUT =
            key("rest.await-leader-timeout")
                    .longType()
                    .defaultValue(30_000L)
                    .withDescription(
                            "The time in ms that the client waits for the leader address, e.g., "
                                    + "Dispatcher or WebMonitorEndpoint");

    /**
     * The number of retries the client will attempt if a retryable operations fails.
     *
     * @see #RETRY_DELAY
     */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Integer> RETRY_MAX_ATTEMPTS =
            key("rest.retry.max-attempts")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "The number of retries the client will attempt if a retryable "
                                    + "operations fails.");

    /**
     * The time in ms that the client waits between retries.
     *
     * @see #RETRY_MAX_ATTEMPTS
     */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Long> RETRY_DELAY =
            key("rest.retry.delay")
                    .longType()
                    .defaultValue(3_000L)
                    .withDescription(
                            String.format(
                                    "The time in ms that the client waits between retries "
                                            + "(See also `%s`).",
                                    RETRY_MAX_ATTEMPTS.key()));

    /** The maximum time in ms for the client to establish a TCP connection. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Long> CONNECTION_TIMEOUT =
            key("rest.connection-timeout")
                    .longType()
                    .defaultValue(15_000L)
                    .withDescription(
                            "The maximum time in ms for the client to establish a TCP connection.");

    /** The maximum time in ms for a connection to stay idle before failing. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Long> IDLENESS_TIMEOUT =
            key("rest.idleness-timeout")
                    .longType()
                    .defaultValue(5L * 60L * 1_000L) // 5 minutes
                    .withDescription(
                            "The maximum time in ms for a connection to stay idle before failing.");

    /** The maximum content length that the server will handle. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Integer> SERVER_MAX_CONTENT_LENGTH =
            key("rest.server.max-content-length")
                    .intType()
                    .defaultValue(104_857_600)
                    .withDescription(
                            "The maximum content length in bytes that the server will handle.");

    /** The maximum content length that the client will handle. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Integer> CLIENT_MAX_CONTENT_LENGTH =
            key("rest.client.max-content-length")
                    .intType()
                    .defaultValue(104_857_600)
                    .withDescription(
                            "The maximum content length in bytes that the client will handle.");

    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Integer> SERVER_NUM_THREADS =
            key("rest.server.numThreads")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The number of threads for the asynchronous processing of requests.");

    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Integer> SERVER_THREAD_PRIORITY =
            key("rest.server.thread-priority")
                    .intType()
                    .defaultValue(Thread.NORM_PRIORITY)
                    .withDescription(
                            "Thread priority of the REST server's executor for processing asynchronous requests. "
                                    + "Lowering the thread priority will give Flink's main components more CPU time whereas "
                                    + "increasing will allocate more time for the REST server's processing.");

    /** Duration from write, after which cached checkpoints statistics are cleaned up. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Duration> CACHE_CHECKPOINT_STATISTICS_TIMEOUT =
            key("rest.cache.checkpoint-statistics.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(WebOptions.REFRESH_INTERVAL.defaultValue()))
                    .withFallbackKeys(WebOptions.REFRESH_INTERVAL.key())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Duration from write after which cached checkpoints statistics are cleaned up. For backwards compatibility, if no value is configured, %s will be used instead.",
                                            code(WebOptions.REFRESH_INTERVAL.key()))
                                    .build());

    /** Maximum number of entries in the checkpoint statistics cache. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Integer> CACHE_CHECKPOINT_STATISTICS_SIZE =
            key("rest.cache.checkpoint-statistics.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum number of entries in the checkpoint statistics cache.");

    /** Enables the experimental flame graph feature. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Boolean> ENABLE_FLAMEGRAPH =
            key("rest.flamegraph.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enables the experimental flame graph feature.");

    /**
     * "Time after which cached stats are cleaned up if not accessed. It can be specified using
     * notation: "100 s", "10 m".
     */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Duration> FLAMEGRAPH_CLEANUP_INTERVAL =
            key("rest.flamegraph.cleanup-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "Time after which cached stats are cleaned up if not accessed. It can"
                                    + " be specified using notation: \"100 s\", \"10 m\".");

    /**
     * Time after which available stats are deprecated and need to be refreshed (by resampling). It
     * can be specified using notation: "30 s", "1 m".
     */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Duration> FLAMEGRAPH_REFRESH_INTERVAL =
            key("rest.flamegraph.refresh-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "Time after which available stats are deprecated and need to be refreshed"
                                    + " (by resampling).  It can be specified using notation: \"30 s\", \"1 m\".");

    /** Number of samples to take to build a FlameGraph. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Integer> FLAMEGRAPH_NUM_SAMPLES =
            key("rest.flamegraph.num-samples")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Number of samples to take to build a FlameGraph.");

    /**
     * Delay between individual stack trace samples taken for building a FlameGraph. It can be
     * specified using notation: "100 ms", "1 s".
     */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Duration> FLAMEGRAPH_DELAY =
            key("rest.flamegraph.delay-between-samples")
                    .durationType()
                    .defaultValue(Duration.ofMillis(50))
                    .withDescription(
                            "Delay between individual stack trace samples taken for building a FlameGraph. It can be specified using notation: \"100 ms\", \"1 s\".");

    /** Maximum depth of stack traces used to create FlameGraphs. */
    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Integer> FLAMEGRAPH_STACK_TRACE_DEPTH =
            key("rest.flamegraph.stack-depth")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum depth of stack traces used to create FlameGraphs.");

    @Documentation.Section(Documentation.Sections.EXPERT_REST)
    public static final ConfigOption<Duration> ASYNC_OPERATION_STORE_DURATION =
            key("rest.async.store-duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription(
                            "Maximum duration that the result of an async operation is stored. Once elapsed the result of the operation can no longer be retrieved.");
}
