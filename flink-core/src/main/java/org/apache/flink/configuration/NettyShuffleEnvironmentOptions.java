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

import static org.apache.flink.configuration.ConfigOptions.key;

/** The set of configuration options relating to network stack. */
@PublicEvolving
public class NettyShuffleEnvironmentOptions {

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
    public static final ConfigOption<Integer> DATA_BIND_PORT =
            key("taskmanager.data.bind-port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The task manager's bind port used for data exchange operations. If not configured, '"
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

    /**
     * Boolean flag indicating whether the shuffle data will be compressed for batch shuffle mode.
     *
     * <p>Note: Data is compressed per buffer and compression can incur extra CPU overhead so it is
     * more effective for IO bounded scenario when data compression ratio is high.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Boolean> BATCH_SHUFFLE_COMPRESSION_ENABLED =
            key("taskmanager.network.batch-shuffle.compression.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("taskmanager.network.blocking-shuffle.compression.enabled")
                    .withDescription(
                            "Boolean flag indicating whether the shuffle data will be compressed "
                                    + "for batch shuffle mode. Note that data is compressed per "
                                    + "buffer and compression can incur extra CPU overhead, so it "
                                    + "is more effective for IO bounded scenario when compression "
                                    + "ratio is high.");

    /** The codec to be used when compressing shuffle data. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    @Experimental
    public static final ConfigOption<String> SHUFFLE_COMPRESSION_CODEC =
            key("taskmanager.network.compression.codec")
                    .stringType()
                    .defaultValue("LZ4")
                    .withDescription(
                            "The codec to be used when compressing shuffle data, only \"LZ4\", \"LZO\" "
                                    + "and \"ZSTD\" are supported now. Through tpc-ds test of these "
                                    + "three algorithms, the results show that \"LZ4\" algorithm has "
                                    + "the highest compression and decompression speed, but the "
                                    + "compression ratio is the lowest. \"ZSTD\" has the highest "
                                    + "compression ratio, but the compression and decompression "
                                    + "speed is the slowest, and LZO is between the two. Also note "
                                    + "that this option is experimental and might be changed in the future.");

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
     * Number of buffers used in the network stack. This defines the number of possible tasks and
     * shuffles.
     *
     * @deprecated use {@link TaskManagerOptions#NETWORK_MEMORY_FRACTION}, {@link
     *     TaskManagerOptions#NETWORK_MEMORY_MIN}, and {@link TaskManagerOptions#NETWORK_MEMORY_MAX}
     *     instead
     */
    @Deprecated
    public static final ConfigOption<Integer> NETWORK_NUM_BUFFERS =
            key("taskmanager.network.numberOfBuffers").intType().defaultValue(2048);

    /**
     * Fraction of JVM memory to use for network buffers.
     *
     * @deprecated use {@link TaskManagerOptions#NETWORK_MEMORY_FRACTION} instead
     */
    @Deprecated
    public static final ConfigOption<Float> NETWORK_BUFFERS_MEMORY_FRACTION =
            key("taskmanager.network.memory.fraction")
                    .floatType()
                    .defaultValue(0.1f)
                    .withDescription(
                            "Fraction of JVM memory to use for network buffers. This determines how many streaming"
                                    + " data exchange channels a TaskManager can have at the same time and how well buffered the channels"
                                    + " are. If a job is rejected or you get a warning that the system has not enough buffers available,"
                                    + " increase this value or the min/max values below. Also note, that \"taskmanager.network.memory.min\""
                                    + "` and \"taskmanager.network.memory.max\" may override this fraction.");

    /**
     * Minimum memory size for network buffers.
     *
     * @deprecated use {@link TaskManagerOptions#NETWORK_MEMORY_MIN} instead
     */
    @Deprecated
    public static final ConfigOption<String> NETWORK_BUFFERS_MEMORY_MIN =
            key("taskmanager.network.memory.min")
                    .stringType()
                    .defaultValue("64mb")
                    .withDescription("Minimum memory size for network buffers.");

    /**
     * Maximum memory size for network buffers.
     *
     * @deprecated use {@link TaskManagerOptions#NETWORK_MEMORY_MAX} instead
     */
    @Deprecated
    public static final ConfigOption<String> NETWORK_BUFFERS_MEMORY_MAX =
            key("taskmanager.network.memory.max")
                    .stringType()
                    .defaultValue("1gb")
                    .withDescription("Maximum memory size for network buffers.");

    /** The maximum number of tpc connections between taskmanagers for data communication. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> MAX_NUM_TCP_CONNECTIONS =
            key("taskmanager.network.max-num-tcp-connections")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The maximum number of tpc connections between taskmanagers for data communication.");

    /**
     * Number of network buffers to use for each outgoing/incoming channel (subpartition/input
     * channel). The minimum valid value that can be configured is 0. When 0 buffers-per-channel is
     * configured, the exclusive network buffers used per downstream incoming channel will be 0, but
     * for each upstream outgoing channel, max(1, configured value) will be used. In other words we
     * ensure that, for performance reasons, there is at least one buffer per outgoing channel
     * regardless of the configuration.
     *
     * <p>Reasoning: 1 buffer for in-flight data in the subpartition + 1 buffer for parallel
     * serialization.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_BUFFERS_PER_CHANNEL =
            key("taskmanager.network.memory.buffers-per-channel")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "Number of exclusive network buffers to use for each outgoing/incoming "
                                    + "channel (subpartition/input channel) in the credit-based flow"
                                    + " control model. It should be configured at least 2 for good "
                                    + "performance. 1 buffer is for receiving in-flight data in the"
                                    + " subpartition and 1 buffer is for parallel serialization. The"
                                    + " minimum valid value that can be configured is 0. When 0 "
                                    + "buffers-per-channel is configured, the exclusive network "
                                    + "buffers used per downstream incoming channel will be 0, but "
                                    + "for each upstream outgoing channel, max(1, configured value)"
                                    + " will be used. In other words we ensure that, for performance"
                                    + " reasons, there is at least one buffer per outgoing channel "
                                    + "regardless of the configuration.");

    /**
     * Number of extra network buffers to use for each outgoing/incoming gate (result
     * partition/input gate).
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_EXTRA_BUFFERS_PER_GATE =
            key("taskmanager.network.memory.floating-buffers-per-gate")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            "Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate)."
                                    + " In credit-based flow control mode, this indicates how many floating credits are shared among all the input channels."
                                    + " The floating buffers are distributed based on backlog (real-time output buffers in the subpartition) feedback, and can"
                                    + " help relieve back-pressure caused by unbalanced data distribution among the subpartitions. This value should be"
                                    + " increased in case of higher round trip times between nodes and/or larger number of machines in the cluster.");

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
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
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

    /** Number of max buffers can be used for each output subpartition. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
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

    /** Number of max overdraft network buffers to use for each ResultPartition. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_MAX_OVERDRAFT_BUFFERS_PER_GATE =
            key("taskmanager.network.memory.max-overdraft-buffers-per-gate")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "Number of max overdraft network buffers to use for each ResultPartition. The overdraft buffers"
                                    + " will be used when the subtask cannot apply to the normal buffers  due to back pressure,"
                                    + " while subtask is performing an action that can not be interrupted in the middle,  like"
                                    + " serializing a large record, flatMap operator producing multiple records for one single"
                                    + " input record or processing time timer producing large output. In situations like that"
                                    + " system will allow subtask to request overdraft buffers, so that the subtask can finish"
                                    + " such uninterruptible action, without blocking unaligned checkpoints for long period of"
                                    + " time. Overdraft buffers are provided on best effort basis only if the system has some"
                                    + " unused buffers available. Subtask that has used overdraft buffers won't be allowed to"
                                    + " process any more records until the overdraft buffers are returned to the pool.");

    /** The timeout for requesting exclusive buffers for each channel. */
    @Documentation.ExcludeFromDocumentation(
            "This option is purely implementation related, and may be removed as the implementation changes.")
    public static final ConfigOption<Long> NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS =
            key("taskmanager.network.memory.exclusive-buffers-request-timeout-ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "The timeout for requesting exclusive buffers for each channel. Since the number of maximum buffers and "
                                    + "the number of required buffers is not the same for local buffer pools, there may be deadlock cases that the upstream"
                                    + "tasks have occupied all the buffers and the downstream tasks are waiting for the exclusive buffers. The timeout breaks"
                                    + "the tie by failing the request of exclusive buffers and ask users to increase the number of total buffers.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
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
     *
     * <p>Note: To avoid connection leak, you must set {@link #MAX_NUM_TCP_CONNECTIONS} to a smaller
     * value before you enable tcp connection reuse.
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
                                    + "to release idle connections. Note that to avoid connection leak, you must set "
                                    + MAX_NUM_TCP_CONNECTIONS.key()
                                    + " to a smaller value before you "
                                    + "enable tcp connection reuse.");

    // ------------------------------------------------------------------------
    //  Netty Options
    // ------------------------------------------------------------------------

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NUM_ARENAS =
            key("taskmanager.network.netty.num-arenas")
                    .intType()
                    .defaultValue(-1)
                    .withDeprecatedKeys("taskmanager.net.num-arenas")
                    .withDescription("The number of Netty arenas.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NUM_THREADS_SERVER =
            key("taskmanager.network.netty.server.numThreads")
                    .intType()
                    .defaultValue(-1)
                    .withDeprecatedKeys("taskmanager.net.server.numThreads")
                    .withDescription("The number of Netty server threads.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NUM_THREADS_CLIENT =
            key("taskmanager.network.netty.client.numThreads")
                    .intType()
                    .defaultValue(-1)
                    .withDeprecatedKeys("taskmanager.net.client.numThreads")
                    .withDescription("The number of Netty client threads.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> CONNECT_BACKLOG =
            key("taskmanager.network.netty.server.backlog")
                    .intType()
                    .defaultValue(0) // default: 0 => Netty's default
                    .withDeprecatedKeys("taskmanager.net.server.backlog")
                    .withDescription("The netty server connection backlog.");

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
    public static final ConfigOption<Integer> SEND_RECEIVE_BUFFER_SIZE =
            key("taskmanager.network.netty.sendReceiveBufferSize")
                    .intType()
                    .defaultValue(0) // default: 0 => Netty's default
                    .withDeprecatedKeys("taskmanager.net.sendReceiveBufferSize")
                    .withDescription(
                            "The Netty send and receive buffer size. This defaults to the system buffer size"
                                    + " (cat /proc/sys/net/ipv4/tcp_[rw]mem) and is 4 MiB in modern Linux.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<String> TRANSPORT_TYPE =
            key("taskmanager.network.netty.transport")
                    .stringType()
                    .defaultValue("auto")
                    .withDeprecatedKeys("taskmanager.net.transport")
                    .withDescription(
                            "The Netty transport type, either \"nio\" or \"epoll\". The \"auto\" means selecting the property mode automatically"
                                    + " based on the platform. Note that the \"epoll\" mode can get better performance, less GC and have more advanced features which are"
                                    + " only available on modern Linux.");

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
                            "Minimum backoff in milliseconds for partition requests of input channels.");

    /** Maximum backoff for partition requests of input channels. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_MAX =
            key("taskmanager.network.request-backoff.max")
                    .intType()
                    .defaultValue(10000)
                    .withDeprecatedKeys("taskmanager.net.request-backoff.max")
                    .withDescription(
                            "Maximum backoff in milliseconds for partition requests of input channels.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private NettyShuffleEnvironmentOptions() {}
}
