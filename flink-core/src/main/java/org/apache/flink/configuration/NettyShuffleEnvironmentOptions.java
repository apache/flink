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

    private static final String HYBRID_SHUFFLE_NEW_MODE_OPTION_NAME =
            "taskmanager.network.hybrid-shuffle.enable-new-mode";

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
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
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
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
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

    /** Region group size of hybrid spilled file data index. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> HYBRID_SHUFFLE_SPILLED_INDEX_REGION_GROUP_SIZE =
            key("taskmanager.network.hybrid-shuffle.spill-index-region-group-size")
                    .intType()
                    .defaultValue(1024)
                    .withDeprecatedKeys(
                            "taskmanager.network.hybrid-shuffle.spill-index-segment-size")
                    .withDescription(
                            "Controls the region group size(in bytes) of hybrid spilled file data index. "
                                    + "Note: This option will be ignored if "
                                    + HYBRID_SHUFFLE_NEW_MODE_OPTION_NAME
                                    + " is set true.");

    /** Max number of hybrid retained regions in memory. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Long> HYBRID_SHUFFLE_NUM_RETAINED_IN_MEMORY_REGIONS_MAX =
            key("taskmanager.network.hybrid-shuffle.num-retained-in-memory-regions-max")
                    .longType()
                    .defaultValue(1024 * 1024L)
                    .withDescription(
                            "Controls the max number of hybrid retained regions in memory. "
                                    + "Note: This option will be ignored if "
                                    + HYBRID_SHUFFLE_NEW_MODE_OPTION_NAME
                                    + " is set true. ");

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
                                    + " process any more records until the overdraft buffers are returned to the pool."
                                    + " It should be noted that this config option only takes effect for Pipelined Shuffle.");

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

    /** The option to enable the new mode of hybrid shuffle. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    @Experimental
    public static final ConfigOption<Boolean> NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE =
            ConfigOptions.key(HYBRID_SHUFFLE_NEW_MODE_OPTION_NAME)
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "The option is used to enable the new mode of hybrid shuffle, which has resolved existing issues in the legacy mode. First, the new mode "
                                    + "uses less required network memory. Second, the new mode can store shuffle data in remote storage when the disk space is not "
                                    + "enough, which could avoid insufficient disk space errors and is only supported when "
                                    + HYBRID_SHUFFLE_REMOTE_STORAGE_BASE_PATH_OPTION_NAME
                                    + " is configured. The new mode is currently in an experimental phase. It can be set to false to fallback to the legacy mode "
                                    + " if something unexpected. Once the new mode reaches a stable state, the legacy mode as well as the option will be removed.");

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
                                    + "Note: If the option is configured and "
                                    + HYBRID_SHUFFLE_NEW_MODE_OPTION_NAME
                                    + " is false, this option will be ignored. "
                                    + "If the option is not configured and "
                                    + HYBRID_SHUFFLE_NEW_MODE_OPTION_NAME
                                    + " is true, the remote storage will be disabled.");

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
