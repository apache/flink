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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;

/** The set of configuration options relating to network stack. */
@PublicEvolving
public class NettyShuffleEnvironmentOptions {
    private static final String HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH_OPTION_NAME =
            "taskmanager.network.hybrid-shuffle.remote.path";

    // ------------------------------------------------------------------------
    //  Network General Options
    // ------------------------------------------------------------------------

    /**
     * The default network port the task manager expects to receive transfer envelopes on. The
     * {@code 0} means that the TaskManager searches for a free port.
     */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_TASK_MANAGER
    })
    public static final ConfigOption<Integer> DATA_PORT =
            key("taskmanager.data.port")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The task manager’s external port used for data exchange operations.");

    /** The local network port that the task manager listen at for data exchange. */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_TASK_MANAGER
    })
    public static final ConfigOption<String> DATA_BIND_PORT =
            key("taskmanager.data.bind-port")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The task manager's bind port used for data exchange operations."
                                    + " Also accepts a list of ports (“50100,50101”), ranges (“50100-50200”) or a combination of both."
                                    + " If not configured, '"
                                    + DATA_PORT.key()
                                    + "' will be used.");

    /** Config parameter to override SSL support for taskmanager's data transport. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Boolean> DATA_SSL_ENABLED =
            key("taskmanager.data.ssl.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable SSL support for the taskmanager data transport. This is applicable only when the"
                                    + " global flag for internal SSL ("
                                    + SecurityOptions.SSL_INTERNAL_ENABLED.key()
                                    + ") is set to true");

    /** The codec to be used when compressing shuffle data. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<CompressionCodec> SHUFFLE_COMPRESSION_CODEC =
            key("taskmanager.network.compression.codec")
                    .enumType(CompressionCodec.class)
                    .defaultValue(CompressionCodec.LZ4)
                    .withDescription(
                            "The codec to be used when compressing shuffle data. If it is \"NONE\", "
                                    + "compression is disable. If it is not \"NONE\", only \"LZ4\", \"LZO\" "
                                    + "and \"ZSTD\" are supported now. Through tpc-ds test of these "
                                    + "three algorithms, the results show that \"LZ4\" algorithm has "
                                    + "the highest compression and decompression speed, but the "
                                    + "compression ratio is the lowest. \"ZSTD\" has the highest "
                                    + "compression ratio, but the compression and decompression "
                                    + "speed is the slowest, and LZO is between the two. Also note "
                                    + "that this option is experimental and might be changed in the future.");

    /** Supported compression codec. */
    public enum CompressionCodec {
        NONE,
        LZ4,
        LZO,
        ZSTD
    }

    /**
     * Boolean flag to enable/disable more detailed metrics about inbound/outbound network queue
     * lengths.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Boolean> NETWORK_DETAILED_METRICS =
            key("taskmanager.network.detailed-metrics")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Boolean flag to enable/disable more detailed metrics about inbound/outbound network queue lengths.");

    /**
     * The maximum number of network read buffers that are required by an input gate. (An input gate
     * is responsible for reading data from all subtasks of an upstream task.) The number of buffers
     * needed by an input gate is dynamically calculated in runtime, depending on various factors
     * (e.g., the parallelism of the upstream task). Among the calculated number of needed buffers,
     * the part below this configured value is required, while the excess part, if any, is optional.
     * A task will fail if the required buffers cannot be obtained in runtime. A task will not fail
     * due to not obtaining optional buffers, but may suffer a performance reduction.
     */
    @Experimental
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE =
            key("taskmanager.network.memory.read-buffer.required-per-gate.max")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of network read buffers that are required by an"
                                    + " input gate. (An input gate is responsible for reading data"
                                    + " from all subtasks of an upstream task.) The number of buffers"
                                    + " needed by an input gate is dynamically calculated in runtime,"
                                    + " depending on various factors (e.g., the parallelism of the"
                                    + " upstream task). Among the calculated number of needed buffers,"
                                    + " the part below this configured value is required, while the"
                                    + " excess part, if any, is optional. A task will fail if the"
                                    + " required buffers cannot be obtained in runtime. A task will"
                                    + " not fail due to not obtaining optional buffers, but may"
                                    + " suffer a performance reduction. If not explicitly configured,"
                                    + " the default value is Integer.MAX_VALUE for streaming workloads,"
                                    + " and 1000 for batch workloads. If explicitly configured, the"
                                    + " configured value should be at least 1.");

    /**
     * Number of network buffers for each outgoing/incoming channel (subpartition/input channel).
     * The minimum valid value for the option is 0. When the option is configured as 0, the
     * exclusive network buffers used per downstream incoming channel will be 0, but for each
     * upstream outgoing channel, max(1, configured value) will be used. In other words we ensure
     * that, for performance reasons, at least one buffer is used per outgoing channel regardless of
     * the configuration.
     *
     * @deprecated This option is deprecated in 1.20 and will be removed in 2.0 to simplify the
     *     configuration of network buffers.
     */
    @Deprecated
    public static final ConfigOption<Integer> NETWORK_BUFFERS_PER_CHANNEL =
            key("taskmanager.network.memory.buffers-per-channel")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            String.format(
                                    "Number of exclusive network buffers for each outgoing/incoming"
                                            + " channel (subpartition/input channel) in the credit-based"
                                            + " flow control model. For the outgoing channel(subpartition),"
                                            + " this value is the effective exclusive buffers per channel."
                                            + " For the incoming channel(input channel), this value"
                                            + " is the max number of exclusive buffers per channel,"
                                            + " the number of effective exclusive network buffers per"
                                            + " channel is dynamically calculated from %s and the"
                                            + " effective range is from 0 to the configured value."
                                            + " The minimum valid value for the option is 0. When"
                                            + " the option is configured as 0, the exclusive network"
                                            + " buffers used by downstream incoming channel will be"
                                            + " 0, but for each upstream outgoing channel, max(1,"
                                            + " configured value) will be used. In other words, we"
                                            + " ensure that, for performance reasons, at least one"
                                            + " buffer is used per outgoing channel regardless of"
                                            + " the configuration.",
                                    NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE.key()));

    /**
     * Number of floating network buffers for each outgoing/incoming gate (result partition/input
     * gate).
     *
     * @deprecated This option is deprecated in 1.20 and will be removed in 2.0 to simplify the
     *     configuration of network buffers.
     */
    @Deprecated
    public static final ConfigOption<Integer> NETWORK_EXTRA_BUFFERS_PER_GATE =
            key("taskmanager.network.memory.floating-buffers-per-gate")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            String.format(
                                    "Number of floating network buffers for each outgoing/incoming"
                                            + " gate (result partition/input gate). In credit-based"
                                            + " flow control mode, this indicates how many floating"
                                            + " credits are shared among all the channels. The floating"
                                            + " buffers can help relieve back-pressure caused by"
                                            + " unbalanced data distribution among the subpartitions."
                                            + " For the outgoing gate(result partition), this value"
                                            + " is the effective floating buffers per gate. For the"
                                            + " incoming gate(input gate), this value is a recommended"
                                            + " number of floating buffers, the number of effective"
                                            + " floating network buffers per gate is dynamically"
                                            + " calculated from %s and the range of effective floating"
                                            + " buffers is from 0 to (parallelism - 1).",
                                    NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE.key()));

    /**
     * Minimum number of network buffers required per blocking result partition for sort-shuffle.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_SORT_SHUFFLE_MIN_BUFFERS =
            key("taskmanager.network.sort-shuffle.min-buffers")
                    .intType()
                    .defaultValue(512)
                    .withDescription(
                            "Minimum number of network buffers required per blocking result partition"
                                    + " for sort-shuffle. For production usage, it is suggested to "
                                    + "increase this config value to at least 2048 (64M memory if "
                                    + "the default 32K memory segment size is used) to improve the "
                                    + "data compression ratio and reduce the small network packets."
                                    + " Usually, several hundreds of megabytes memory is enough for"
                                    + " large scale batch jobs. Note: you may also need to increase"
                                    + " the size of total network memory to avoid the 'insufficient"
                                    + " number of network buffers' error if you are increasing this"
                                    + " config value.");

    /**
     * Parallelism threshold to switch between sort-based blocking shuffle and hash-based blocking
     * shuffle.
     *
     * @deprecated The hash-based blocking shuffle is deprecated in 1.20 and will be totally removed
     *     in 2.0.
     */
    @Deprecated
    public static final ConfigOption<Integer> NETWORK_SORT_SHUFFLE_MIN_PARALLELISM =
            key("taskmanager.network.sort-shuffle.min-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            String.format(
                                    "Parallelism threshold to switch between sort-based blocking "
                                            + "shuffle and hash-based blocking shuffle, which means"
                                            + " for batch jobs of smaller parallelism, hash-shuffle"
                                            + " will be used and for batch jobs of larger or equal "
                                            + "parallelism, sort-shuffle will be used. The value 1 "
                                            + "means that sort-shuffle is the default option. Note:"
                                            + " For production usage, you may also need to tune "
                                            + "'%s' and '%s' for better performance.",
                                    NETWORK_SORT_SHUFFLE_MIN_BUFFERS.key(),
                                    // raw string key is used here to avoid interdependence, a test
                                    // is implemented to guard that when the target key is modified,
                                    // this raw value must be changed correspondingly
                                    "taskmanager.memory.framework.off-heap.batch-shuffle.size"));

    /**
     * Number of max buffers can be used for each output subpartition.
     *
     * @deprecated This option is deprecated in 1.20 and will be removed in 2.0 to simplify the
     *     configuration of network buffers.
     */
    @Deprecated
    public static final ConfigOption<Integer> NETWORK_MAX_BUFFERS_PER_CHANNEL =
            key("taskmanager.network.memory.max-buffers-per-channel")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Number of max buffers that can be used for each channel. If a channel exceeds the number of max"
                                    + " buffers, it will make the task become unavailable, cause the back pressure and block the data processing. This"
                                    + " might speed up checkpoint alignment by preventing excessive growth of the buffered in-flight data in"
                                    + " case of data skew and high number of configured floating buffers. This limit is not strictly guaranteed,"
                                    + " and can be ignored by things like flatMap operators, records spanning multiple buffers or single timer"
                                    + " producing large amount of data.");

    /** The timeout for requesting buffers for each channel. */
    @Documentation.ExcludeFromDocumentation(
            "This option is purely implementation related, and may be removed as the implementation changes.")
    public static final ConfigOption<Duration> NETWORK_BUFFERS_REQUEST_TIMEOUT =
            key("taskmanager.network.memory.buffers-request-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(30000L))
                    .withDeprecatedKeys(
                            "taskmanager.network.memory.exclusive-buffers-request-timeout-ms")
                    .withDescription(
                            "The timeout for requesting buffers for each channel. Since the number of maximum buffers and "
                                    + "the number of required buffers is not the same for local buffer pools, there may be deadlock cases that the upstream"
                                    + "tasks have occupied all the buffers and the downstream tasks are waiting for the exclusive buffers. The timeout breaks"
                                    + "the tie by failing the request of exclusive buffers and ask users to increase the number of total buffers.");

    /** The option to configure the tiered factory creator remote class name for hybrid shuffle. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    @Experimental
    public static final ConfigOption<String>
            NETWORK_HYBRID_SHUFFLE_EXTERNAL_REMOTE_TIER_FACTORY_CLASS_NAME =
                    key("taskmanager.network.hybrid-shuffle.external-remote-tier-factory.class")
                            .stringType()
                            .noDefaultValue()
                            .withDescription(
                                    "The option configures the class that is responsible for creating an "
                                            + "external remote tier factory for hybrid shuffle. If "
                                            + "configured, the hybrid shuffle will only initialize "
                                            + "the specified remote tier according to the given class "
                                            + "name. Currently, since the tier interfaces are not yet "
                                            + "public and are still actively evolving, it is recommended "
                                            + "that users do not independently implement the external "
                                            + "remote tier until the tier interfaces are stabilized. ");

    /** The option to configure the base remote storage path for hybrid shuffle. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    @Experimental
    public static final ConfigOption<String> NETWORK_HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH =
            key(HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH_OPTION_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The option is used to configure the base path of remote storage for hybrid shuffle. The shuffle data will be stored in "
                                    + "remote storage when the disk space is not enough. "
                                    + "Note: If this option is not configured the remote storage will be disabled.");

    /**
     * @deprecated The hash-based blocking shuffle is deprecated in 1.20 and will be totally removed
     *     in 2.0.
     */
    @Deprecated
    public static final ConfigOption<String> NETWORK_BLOCKING_SHUFFLE_TYPE =
            key("taskmanager.network.blocking-shuffle.type")
                    .stringType()
                    .defaultValue("file")
                    .withDescription(
                            "The blocking shuffle type, either \"mmap\" or \"file\". The \"auto\" means selecting the property type automatically"
                                    + " based on system memory architecture (64 bit for mmap and 32 bit for file). Note that the memory usage of mmap is not accounted"
                                    + " by configured memory limits, but some resource frameworks like yarn would track this memory usage and kill the container once"
                                    + " memory exceeding some threshold. Also note that this option is experimental and might be changed future.");

    /**
     * Whether to reuse tcp connections across multi jobs. If set to true, tcp connections will not
     * be released after job finishes. The subsequent jobs will be free from the overhead of the
     * connection re-establish. However, this may lead to an increase in the total number of
     * connections on your machine. When it reaches the upper limit, you can set it to false to
     * release idle connections.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Boolean> TCP_CONNECTION_REUSE_ACROSS_JOBS_ENABLED =
            key("taskmanager.network.tcp-connection.enable-reuse-across-jobs")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to reuse tcp connections across multi jobs. If set to true, tcp "
                                    + "connections will not be released after job finishes. The subsequent "
                                    + "jobs will be free from the overhead of the connection re-establish. "
                                    + "However, this may lead to an increase in the total number of connections "
                                    + "on your machine. When it reaches the upper limit, you can set it to false "
                                    + "to release idle connections.");

    // ------------------------------------------------------------------------
    //  Netty Options
    // ------------------------------------------------------------------------

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> CLIENT_CONNECT_TIMEOUT_SECONDS =
            key("taskmanager.network.netty.client.connectTimeoutSec")
                    .intType()
                    .defaultValue(120) // default: 120s = 2min
                    .withDeprecatedKeys("taskmanager.net.client.connectTimeoutSec")
                    .withDescription("The Netty client connection timeout.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_RETRIES =
            key("taskmanager.network.retries")
                    .intType()
                    .defaultValue(0)
                    .withDeprecatedKeys("taskmanager.network.retries")
                    .withDescription(
                            "The number of retry attempts for network communication."
                                    + " Currently it's only used for establishing input/output channel connections");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> CLIENT_TCP_KEEP_IDLE_SECONDS =
            key("taskmanager.network.netty.client.tcp.keepIdleSec")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The time (in seconds) the connection needs to remain idle before TCP starts sending keepalive probes. "
                                    + "Note: This will not take effect when using netty transport type of nio with an older version of JDK 8, "
                                    + "refer to https://bugs.openjdk.org/browse/JDK-8194298.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> CLIENT_TCP_KEEP_INTERVAL_SECONDS =
            key("taskmanager.network.netty.client.tcp.keepIntervalSec")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The time (in seconds) between individual keepalive probes. "
                                    + "Note: This will not take effect when using netty transport type of nio with an older version of JDK 8, "
                                    + "refer to https://bugs.openjdk.org/browse/JDK-8194298.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> CLIENT_TCP_KEEP_COUNT =
            key("taskmanager.network.netty.client.tcp.keepCount")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of keepalive probes TCP should send before Netty client dropping the connection. "
                                    + "Note: This will not take effect when using netty transport type of nio with an older version of JDK 8, "
                                    + "refer to https://bugs.openjdk.org/browse/JDK-8194298.");

    // ------------------------------------------------------------------------
    //  Partition Request Options
    // ------------------------------------------------------------------------

    /** Minimum backoff for partition requests of input channels. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_INITIAL =
            key("taskmanager.network.request-backoff.initial")
                    .intType()
                    .defaultValue(100)
                    .withDeprecatedKeys("taskmanager.net.request-backoff.initial")
                    .withDescription(
                            "Minimum backoff in milliseconds for partition requests of local input channels.");

    /** Maximum backoff for partition requests of input channels. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_MAX =
            key("taskmanager.network.request-backoff.max")
                    .intType()
                    .defaultValue(10000)
                    .withDeprecatedKeys("taskmanager.net.request-backoff.max")
                    .withDescription(
                            "Maximum backoff in milliseconds for partition requests of local input channels.");

    /** The timeout for partition request listener in result partition manager. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Duration> NETWORK_PARTITION_REQUEST_TIMEOUT =
            key("taskmanager.network.partition-request-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Timeout for an individual partition request of remote input channels. "
                                                    + "The partition request will finally fail if the total wait time exceeds "
                                                    + "twice the value of %s.",
                                            code(NETWORK_REQUEST_BACKOFF_MAX.key()))
                                    .build());

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private NettyShuffleEnvironmentOptions() {}
}
