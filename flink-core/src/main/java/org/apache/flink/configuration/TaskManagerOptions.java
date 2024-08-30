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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** The set of configuration options relating to TaskManager and Task settings. */
@PublicEvolving
@ConfigGroups(groups = @ConfigGroup(name = "TaskManagerMemory", keyPrefix = "taskmanager.memory"))
public class TaskManagerOptions {

    public static final String MANAGED_MEMORY_CONSUMER_NAME_OPERATOR = "OPERATOR";
    public static final String MANAGED_MEMORY_CONSUMER_NAME_STATE_BACKEND = "STATE_BACKEND";
    public static final String MANAGED_MEMORY_CONSUMER_NAME_PYTHON = "PYTHON";

    // ------------------------------------------------------------------------
    //  General TaskManager Options
    // ------------------------------------------------------------------------

    /** Whether to kill the TaskManager when the task thread throws an OutOfMemoryError. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Boolean> KILL_ON_OUT_OF_MEMORY =
            key("taskmanager.jvm-exit-on-oom")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.");

    /**
     * The external address of the network interface where the TaskManager is exposed. Overrides
     * {@link #HOST_BIND_POLICY} automatic address binding.
     */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_TASK_MANAGER
    })
    public static final ConfigOption<String> HOST =
            key("taskmanager.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The external address of the network interface where the TaskManager is exposed."
                                    + " Because different TaskManagers need different values for this option, usually it is specified in an"
                                    + " additional non-shared TaskManager-specific config file.");

    /** The local address of the network interface that the task manager binds to. */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_TASK_MANAGER
    })
    public static final ConfigOption<String> BIND_HOST =
            key("taskmanager.bind-host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The local address of the network interface that the task manager binds to. If not"
                                    + " configured, '0.0.0.0' will be used.");

    /**
     * The default network port range the task manager expects incoming IPC connections. The {@code
     * "0"} means that the TaskManager searches for a free port.
     */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_TASK_MANAGER
    })
    public static final ConfigOption<String> RPC_PORT =
            key("taskmanager.rpc.port")
                    .stringType()
                    .defaultValue("0")
                    .withDescription(
                            "The external RPC port where the TaskManager is exposed. Accepts a list of ports"
                                    + " (“50100,50101”), ranges (“50100-50200”) or a combination of both. It is recommended to set a"
                                    + " range of ports to avoid collisions when multiple TaskManagers are running on the same machine.");

    /** The local port that the task manager binds to. */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_TASK_MANAGER
    })
    public static final ConfigOption<Integer> RPC_BIND_PORT =
            key("taskmanager.rpc.bind-port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The local RPC port that the TaskManager binds to. If not configured, the external port"
                                    + " (configured by '"
                                    + RPC_PORT.key()
                                    + "') will be used.");

    /** The default port that <code>CollectSinkFunction$ServerThread</code> is using. */
    @Documentation.Section({
        Documentation.Sections.COMMON_HOST_PORT,
        Documentation.Sections.ALL_TASK_MANAGER
    })
    public static final ConfigOption<Integer> COLLECT_PORT =
            key("taskmanager.collect-sink.port")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The port used for the client to retrieve query results from the TaskManager. "
                                    + "The default value is 0, which corresponds to a random port assignment.");

    /**
     * Defines the timeout it can take for the TaskManager registration. If the duration is exceeded
     * without a successful registration, then the TaskManager terminates.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Duration> REGISTRATION_TIMEOUT =
            key("taskmanager.registration.timeout")
                    .durationType()
                    .defaultValue(TimeUtils.parseDuration("5 min"))
                    .withDeprecatedKeys("taskmanager.maxRegistrationDuration")
                    .withDescription(
                            "Defines the timeout for the TaskManager registration. If the duration is"
                                    + " exceeded without a successful registration, then the TaskManager terminates.");

    /** The config parameter defining the number of task slots of a task manager. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Integer> NUM_TASK_SLOTS =
            key("taskmanager.numberOfTaskSlots")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of parallel operator or user function instances that a single TaskManager can"
                                    + " run. If this value is larger than 1, a single TaskManager takes multiple instances of a function or"
                                    + " operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the"
                                    + " available memory is divided between the different operator or function instances. This value"
                                    + " is typically proportional to the number of physical CPU cores that the TaskManager's machine has"
                                    + " (e.g., equal to the number of cores, or half the number of cores).");

    /** Timeout for identifying inactive slots. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Duration> SLOT_TIMEOUT =
            key("taskmanager.slot.timeout")
                    .durationType()
                    .defaultValue(RpcOptions.ASK_TIMEOUT_DURATION.defaultValue())
                    .withFallbackKeys(RpcOptions.ASK_TIMEOUT_DURATION.key())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Timeout used for identifying inactive slots. The TaskManager will free the slot if it does not become active "
                                                    + "within the given amount of time. Inactive slots can be caused by an out-dated slot request. If no "
                                                    + "value is configured, then it will fall back to %s.",
                                            code(RpcOptions.ASK_TIMEOUT_DURATION.key()))
                                    .build());

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Boolean> DEBUG_MEMORY_LOG =
            key("taskmanager.debug.memory.log")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("taskmanager.debug.memory.startLogThread")
                    .withDescription(
                            "Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Duration> DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS =
            key("taskmanager.debug.memory.log-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(5000L))
                    .withDeprecatedKeys("taskmanager.debug.memory.logIntervalMs")
                    .withDescription(
                            "The interval for the log thread to log the current memory usage.");

    // ------------------------------------------------------------------------
    //  Managed Memory Options
    // ------------------------------------------------------------------------

    /** Size of memory buffers used by the network stack and the memory manager. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<MemorySize> MEMORY_SEGMENT_SIZE =
            key("taskmanager.memory.segment-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("32kb"))
                    .withDescription(
                            "Size of memory buffers used by the network stack and the memory manager.");

    /** Minimum possible size of memory buffers used by the network stack and the memory manager. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<MemorySize> MIN_MEMORY_SEGMENT_SIZE =
            key("taskmanager.memory.min-segment-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256"))
                    .withDescription(
                            "Minimum possible size of memory buffers used by the network stack and the memory manager. "
                                    + "ex. can be used for automatic buffer size adjustment.");

    /** Starting size of memory buffers used by the network stack and the memory manager. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<MemorySize> STARTING_MEMORY_SEGMENT_SIZE =
            key("taskmanager.memory.starting-segment-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1024"))
                    .withDescription(
                            "Starting size of memory buffers used by the network stack and the memory manager, "
                                    + "when using automatic buffer size adjustment.");

    /**
     * The config parameter for automatically defining the TaskManager's binding address, if {@link
     * #HOST} configuration option is not set.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<String> HOST_BIND_POLICY =
            key("taskmanager.network.bind-policy")
                    .stringType()
                    .defaultValue("ip")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The automatic address binding policy used by the TaskManager if \""
                                                    + HOST.key()
                                                    + "\" is not set."
                                                    + " The value should be one of the following:\n")
                                    .list(
                                            text("\"name\" - uses hostname as binding address"),
                                            text(
                                                    "\"ip\" - uses host's ip address as binding address"))
                                    .build());

    /**
     * The TaskManager's ResourceID. If not configured, the ResourceID will be generated with the
     * RpcAddress:RpcPort and a 6-character random string. Notice that this option is not valid in
     * Yarn and Native Kubernetes mode.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<String> TASK_MANAGER_RESOURCE_ID =
            key("taskmanager.resource-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The TaskManager's ResourceID. If not configured, the ResourceID will be generated with the "
                                    + "\"RpcAddress:RpcPort\" and a 6-character random string. Notice that this option is not valid in Yarn and Native Kubernetes mode.");

    // ------------------------------------------------------------------------
    //  Resource Options
    // ------------------------------------------------------------------------

    /**
     * This config option describes number of cpu cores of task executors. In case of Yarn /
     * Kubernetes, it is used to launch a container for the task executor.
     *
     * <p>DO NOT USE THIS CONFIG OPTION. This config option is currently only used internally, for
     * passing cpu cores into task executors for dynamic fine grained slot resource management. The
     * feature is not completed at the moment, and the config option is experimental and might be
     * changed / removed in the future. Thus, we do not expose this config option to users.
     *
     * <p>For configuring the cpu cores of container on Yarn / Kubernetes, please use {@link
     * YarnConfigOptions#VCORES} and {@link KubernetesConfigOptions#TASK_MANAGER_CPU}.
     */
    @Documentation.ExcludeFromDocumentation
    public static final ConfigOption<Double> CPU_CORES =
            key("taskmanager.cpu.cores")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription(
                            "CPU cores for the TaskExecutors. In case of Yarn setups, this value will be rounded to "
                                    + "the closest positive integer. If not explicitly configured, legacy config options "
                                    + "'yarn.containers.vcores' and 'kubernetes.taskmanager.cpu' will be "
                                    + "used for Yarn / Kubernetes setups, and '"
                                    + NUM_TASK_SLOTS.key()
                                    + "' will be used for "
                                    + "standalone setups (approximate number of slots).");

    /** Total Process Memory size for the TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> TOTAL_PROCESS_MEMORY =
            key("taskmanager.memory.process.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Total Process Memory size for the TaskExecutors. This includes all the memory that a "
                                    + "TaskExecutor consumes, consisting of Total Flink Memory, JVM Metaspace, and JVM Overhead. On "
                                    + "containerized setups, this should be set to the container memory. See also "
                                    + "'taskmanager.memory.flink.size' for total Flink memory size configuration.");

    /** Total Flink Memory size for the TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> TOTAL_FLINK_MEMORY =
            key("taskmanager.memory.flink.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            String.format(
                                    "Total Flink Memory size for the TaskExecutors. This includes all the "
                                            + "memory that a TaskExecutor consumes, except for JVM Metaspace and JVM Overhead. It consists of "
                                            + "Framework Heap Memory, Task Heap Memory, Task Off-Heap Memory, Managed Memory, and Network "
                                            + "Memory. See also '%s' for total process memory size configuration.",
                                    TOTAL_PROCESS_MEMORY.key()));

    /** Framework Heap Memory size for TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> FRAMEWORK_HEAP_MEMORY =
            key("taskmanager.memory.framework.heap.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128m"))
                    .withDescription(
                            "Framework Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved"
                                    + " for TaskExecutor framework, which will not be allocated to task slots.");

    /** Framework Off-Heap Memory size for TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> FRAMEWORK_OFF_HEAP_MEMORY =
            key("taskmanager.memory.framework.off-heap.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128m"))
                    .withDescription(
                            "Framework Off-Heap Memory size for TaskExecutors. This is the size of off-heap memory"
                                    + " (JVM direct memory and native memory) reserved for TaskExecutor framework, which will not be"
                                    + " allocated to task slots. The configured value will be fully counted when Flink calculates the JVM"
                                    + " max direct memory size parameter.");

    /** Task Heap Memory size for TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> TASK_HEAP_MEMORY =
            key("taskmanager.memory.task.heap.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Task Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved for"
                                    + " tasks. If not specified, it will be derived as Total Flink Memory minus Framework Heap Memory,"
                                    + " Framework Off-Heap Memory, Task Off-Heap Memory, Managed Memory and Network Memory.");

    /** Task Off-Heap Memory size for TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> TASK_OFF_HEAP_MEMORY =
            key("taskmanager.memory.task.off-heap.size")
                    .memoryType()
                    .defaultValue(MemorySize.ZERO)
                    .withDescription(
                            "Task Off-Heap Memory size for TaskExecutors. This is the size of off heap memory (JVM"
                                    + " direct memory and native memory) reserved for tasks. The configured value will be fully counted"
                                    + " when Flink calculates the JVM max direct memory size parameter.");

    /** Managed Memory size for TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> MANAGED_MEMORY_SIZE =
            key("taskmanager.memory.managed.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDeprecatedKeys("taskmanager.memory.size")
                    .withDescription(
                            "Managed Memory size for TaskExecutors. This is the size of off-heap memory managed by the"
                                    + " memory manager, reserved for sorting, hash tables, caching of intermediate results and RocksDB state"
                                    + " backend. Memory consumers can either allocate memory from the memory manager in the form of"
                                    + " MemorySegments, or reserve bytes from the memory manager and keep their memory usage within that"
                                    + " boundary. If unspecified, it will be derived to make up the configured fraction of the Total Flink"
                                    + " Memory.");

    /**
     * Fraction of Total Flink Memory to be used as Managed Memory, if {@link #MANAGED_MEMORY_SIZE}
     * is not specified.
     */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<Float> MANAGED_MEMORY_FRACTION =
            key("taskmanager.memory.managed.fraction")
                    .floatType()
                    .defaultValue(0.4f)
                    .withDescription(
                            "Fraction of Total Flink Memory to be used as Managed Memory, if Managed Memory size is not"
                                    + " explicitly specified.");

    /** Weights of managed memory consumers. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<Map<String, String>> MANAGED_MEMORY_CONSUMER_WEIGHTS =
            key("taskmanager.memory.managed.consumer-weights")
                    .mapType()
                    .defaultValue(
                            new HashMap<String, String>() {
                                {
                                    put(MANAGED_MEMORY_CONSUMER_NAME_OPERATOR, "70");
                                    put(MANAGED_MEMORY_CONSUMER_NAME_STATE_BACKEND, "70");
                                    put(MANAGED_MEMORY_CONSUMER_NAME_PYTHON, "30");
                                }
                            })
                    .withDescription(
                            "Managed memory weights for different kinds of consumers. A slot’s"
                                    + " managed memory is shared by all kinds of consumers it"
                                    + " contains, proportionally to the kinds’ weights and"
                                    + " regardless of the number of consumers from each kind."
                                    + " Currently supported kinds of consumers are "
                                    + MANAGED_MEMORY_CONSUMER_NAME_OPERATOR
                                    + " (for built-in algorithms), "
                                    + MANAGED_MEMORY_CONSUMER_NAME_STATE_BACKEND
                                    + " (for RocksDB state backend) and "
                                    + MANAGED_MEMORY_CONSUMER_NAME_PYTHON
                                    + " (for Python processes).");

    /** Min Network Memory size for TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> NETWORK_MEMORY_MIN =
            key("taskmanager.memory.network.min")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64m"))
                    .withDeprecatedKeys("taskmanager.network.memory.min")
                    .withDescription(
                            "Min Network Memory size for TaskExecutors. Network Memory is off-heap memory reserved for"
                                    + " ShuffleEnvironment (e.g., network buffers). Network Memory size is derived to make up the configured"
                                    + " fraction of the Total Flink Memory. If the derived size is less/greater than the configured min/max"
                                    + " size, the min/max size will be used. The exact size of Network Memory can be explicitly specified by"
                                    + " setting the min/max to the same value.");

    /** Max Network Memory size for TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<MemorySize> NETWORK_MEMORY_MAX =
            key("taskmanager.memory.network.max")
                    .memoryType()
                    .defaultValue(MemorySize.MAX_VALUE)
                    .withDeprecatedKeys("taskmanager.network.memory.max")
                    .withDescription(
                            "Max Network Memory size for TaskExecutors. Network Memory is off-heap memory reserved for"
                                    + " ShuffleEnvironment (e.g., network buffers). Network Memory size is derived to make up the configured"
                                    + " fraction of the Total Flink Memory. If the derived size is less/greater than the configured min/max"
                                    + " size, the min/max size will be used. By default, the max limit of Network Memory is Long.MAX_VALUE."
                                    + " The exact size of Network Memory can be explicitly specified by setting the min/max to the same value.");

    /** Fraction of Total Flink Memory to be used as Network Memory. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<Float> NETWORK_MEMORY_FRACTION =
            key("taskmanager.memory.network.fraction")
                    .floatType()
                    .defaultValue(0.1f)
                    .withDeprecatedKeys("taskmanager.network.memory.fraction")
                    .withDescription(
                            "Fraction of Total Flink Memory to be used as Network Memory. Network Memory is off-heap"
                                    + " memory reserved for ShuffleEnvironment (e.g., network buffers). Network Memory size is derived to"
                                    + " make up the configured fraction of the Total Flink Memory. If the derived size is less/greater than"
                                    + " the configured min/max size, the min/max size will be used. The exact size of Network Memory can be"
                                    + " explicitly specified by setting the min/max size to the same value.");

    /** The period between recalculation the relevant size of the buffer. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Duration> BUFFER_DEBLOAT_PERIOD =
            ConfigOptions.key("taskmanager.network.memory.buffer-debloat.period")
                    .durationType()
                    .defaultValue(Duration.ofMillis(200))
                    .withDescription(
                            "The minimum period of time after which the buffer size will be debloated if required. "
                                    + "The low value provides a fast reaction to the load fluctuation but can influence the performance.");

    /** The number of samples requires for the buffer size adjustment. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> BUFFER_DEBLOAT_SAMPLES =
            ConfigOptions.key("taskmanager.network.memory.buffer-debloat.samples")
                    .intType()
                    .defaultValue(20)
                    .withDescription(
                            "The number of the last buffer size values that will be taken for the correct calculation of the new one.");

    /** The total time for which automated adjusted buffers should be fully consumed. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Duration> BUFFER_DEBLOAT_TARGET =
            ConfigOptions.key("taskmanager.network.memory.buffer-debloat.target")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "The target total time after which buffered in-flight data should be fully consumed. "
                                    + "This configuration option will be used, in combination with the measured throughput, to adjust the amount of in-flight data.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Boolean> BUFFER_DEBLOAT_ENABLED =
            ConfigOptions.key("taskmanager.network.memory.buffer-debloat.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "The switch of the automatic buffered debloating feature. "
                                    + "If enabled the amount of in-flight data will be adjusted automatically accordingly to the measured throughput.");

    /**
     * Difference between the new and the old buffer size for applying the new value(in percent).
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
    public static final ConfigOption<Integer> BUFFER_DEBLOAT_THRESHOLD_PERCENTAGES =
            ConfigOptions.key("taskmanager.network.memory.buffer-debloat.threshold-percentages")
                    .intType()
                    .defaultValue(25)
                    .withDescription(
                            "The minimum difference in percentage between the newly calculated buffer size and the old one to announce the new value. "
                                    + "Can be used to avoid constant back and forth small adjustments.");

    /**
     * Size of direct memory used by batch shuffle for shuffle data read (currently only used by
     * sort-shuffle and hybrid shuffle).
     */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> NETWORK_BATCH_SHUFFLE_READ_MEMORY =
            key("taskmanager.memory.framework.off-heap.batch-shuffle.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64m"))
                    .withDescription(
                            String.format(
                                    "Size of memory used by batch shuffle for shuffle data read "
                                            + "(currently only used by sort-shuffle and hybrid shuffle)."
                                            + " Notes: 1) The memory is cut from '%s' so must be smaller than"
                                            + " that, which means you may also need to increase '%s' "
                                            + "after you increase this config value; 2) This memory"
                                            + " size can influence the shuffle performance and you "
                                            + "can increase this config value for large-scale batch"
                                            + " jobs (for example, to 128M or 256M).",
                                    FRAMEWORK_OFF_HEAP_MEMORY.key(),
                                    FRAMEWORK_OFF_HEAP_MEMORY.key()));

    /** JVM Metaspace Size for the TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> JVM_METASPACE =
            key("taskmanager.memory.jvm-metaspace.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256m"))
                    .withDescription("JVM Metaspace Size for the TaskExecutors.");

    /** Min JVM Overhead size for the TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> JVM_OVERHEAD_MIN =
            key("taskmanager.memory.jvm-overhead.min")
                    .memoryType()
                    .defaultValue(MemorySize.parse("192m"))
                    .withDescription(
                            "Min JVM Overhead size for the TaskExecutors. This is off-heap memory reserved for JVM"
                                    + " overhead, such as thread stack space, compile cache, etc. This includes native memory but not direct"
                                    + " memory, and will not be counted when Flink calculates JVM max direct memory size parameter. The size"
                                    + " of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If the"
                                    + " derived size is less/greater than the configured min/max size, the min/max size will be used. The"
                                    + " exact size of JVM Overhead can be explicitly specified by setting the min/max size to the same value.");

    /** Max JVM Overhead size for the TaskExecutors. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<MemorySize> JVM_OVERHEAD_MAX =
            key("taskmanager.memory.jvm-overhead.max")
                    .memoryType()
                    .defaultValue(MemorySize.parse("1g"))
                    .withDescription(
                            "Max JVM Overhead size for the TaskExecutors. This is off-heap memory reserved for JVM"
                                    + " overhead, such as thread stack space, compile cache, etc. This includes native memory but not direct"
                                    + " memory, and will not be counted when Flink calculates JVM max direct memory size parameter. The size"
                                    + " of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If the"
                                    + " derived size is less/greater than the configured min/max size, the min/max size will be used. The"
                                    + " exact size of JVM Overhead can be explicitly specified by setting the min/max size to the same value.");

    /** Fraction of Total Process Memory to be reserved for JVM Overhead. */
    @Documentation.Section(Documentation.Sections.COMMON_MEMORY)
    public static final ConfigOption<Float> JVM_OVERHEAD_FRACTION =
            key("taskmanager.memory.jvm-overhead.fraction")
                    .floatType()
                    .defaultValue(0.1f)
                    .withDescription(
                            "Fraction of Total Process Memory to be reserved for JVM Overhead. This is off-heap memory"
                                    + " reserved for JVM overhead, such as thread stack space, compile cache, etc. This includes native"
                                    + " memory but not direct memory, and will not be counted when Flink calculates JVM max direct memory"
                                    + " size parameter. The size of JVM Overhead is derived to make up the configured fraction of the Total"
                                    + " Process Memory. If the derived size is less/greater than the configured min/max size, the min/max"
                                    + " size will be used. The exact size of JVM Overhead can be explicitly specified by setting the min/max"
                                    + " size to the same value.");

    // ------------------------------------------------------------------------
    //  Task Options
    // ------------------------------------------------------------------------

    /** Time interval in milliseconds between two successive task cancellation attempts. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Duration> TASK_CANCELLATION_INTERVAL =
            key("task.cancellation.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(30000L))
                    .withDeprecatedKeys("task.cancellation-interval")
                    .withDescription(
                            "Time interval between two successive task cancellation attempts.");

    /**
     * Timeout in milliseconds after which a task cancellation times out and leads to a fatal
     * TaskManager error. A value of <code>0</code> deactivates the watch dog. Notice that a task
     * cancellation is different from both a task failure and a clean shutdown. Task cancellation
     * timeout only applies to task cancellation and does not apply to task closing/clean-up caused
     * by a task failure or a clean shutdown.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Duration> TASK_CANCELLATION_TIMEOUT =
            key("task.cancellation.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(180000L))
                    .withDescription(
                            "Timeout after which a task cancellation times out and"
                                    + " leads to a fatal TaskManager error. A value of 0 deactivates"
                                    + " the watch dog. Notice that a task cancellation is different from"
                                    + " both a task failure and a clean shutdown. "
                                    + " Task cancellation timeout only applies to task cancellation and does not apply to"
                                    + " task closing/clean-up caused by a task failure or a clean shutdown.");
    /**
     * This configures how long we wait for the timers in milliseconds to finish all pending timer
     * threads when the stream task is cancelled.
     */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Duration> TASK_CANCELLATION_TIMEOUT_TIMERS =
            ConfigOptions.key("task.cancellation.timers.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(7500L))
                    .withDeprecatedKeys("timerservice.exceptional.shutdown.timeout")
                    .withDescription(
                            "Time we wait for the timers to finish all pending timer threads"
                                    + " when the stream task is cancelled.");

    /** This configures how to redirect the {@link System#out} and {@link System#err}. */
    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<SystemOutMode> TASK_MANAGER_SYSTEM_OUT_MODE =
            ConfigOptions.key("taskmanager.system-out.mode")
                    .enumType(SystemOutMode.class)
                    .defaultValue(SystemOutMode.DEFAULT)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Redirection mode of %s and %s for all %s.",
                                            code("System.out"),
                                            code("System.err"),
                                            code("TaskManagers"))
                                    .list(
                                            text(
                                                    "%s: %s don't redirect the %s and %s, it's the default value.",
                                                    code(SystemOutMode.DEFAULT.name()),
                                                    code("TaskManagers"),
                                                    code("System.out"),
                                                    code("System.err")),
                                            text(
                                                    "%s: %s redirect %s and %s to LOG.info and LOG.error.",
                                                    code(SystemOutMode.LOG.name()),
                                                    code("TaskManagers"),
                                                    code("System.out"),
                                                    code("System.err")),
                                            text(
                                                    "%s: %s ignore %s and %s directly.",
                                                    code(SystemOutMode.IGNORE.name()),
                                                    code("TaskManagers"),
                                                    code("System.out"),
                                                    code("System.err")))
                                    .build());

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Boolean> TASK_MANAGER_SYSTEM_OUT_LOG_THREAD_NAME =
            ConfigOptions.key("taskmanager.system-out.log.thread-name.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to log the thread name when %s is LOG.",
                                            code(TASK_MANAGER_SYSTEM_OUT_MODE.key()))
                                    .build());

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<MemorySize> TASK_MANAGER_SYSTEM_OUT_LOG_CACHE_SIZE =
            ConfigOptions.key("taskmanager.system-out.log.cache-upper-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("100 kb"))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The cache upper size when Flink caches current line context "
                                                    + "of %s or %s when %s is LOG.",
                                            code("System.out"),
                                            code("System.err"),
                                            code(TASK_MANAGER_SYSTEM_OUT_MODE.key()))
                                    .build());

    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_TASK_MANAGER
    })
    public static final ConfigOption<TaskManagerLoadBalanceMode> TASK_MANAGER_LOAD_BALANCE_MODE =
            ConfigOptions.key("taskmanager.load-balance.mode")
                    .enumType(TaskManagerLoadBalanceMode.class)
                    .defaultValue(TaskManagerLoadBalanceMode.NONE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Mode for the load-balance allocation strategy across all available %s.",
                                            code("TaskManagers"))
                                    .list(
                                            text(
                                                    "The %s mode tries to spread out the slots evenly across all available %s.",
                                                    code(TaskManagerLoadBalanceMode.SLOTS.name()),
                                                    code("TaskManagers")),
                                            text(
                                                    "The %s mode is the default mode without any specified strategy.",
                                                    code(TaskManagerLoadBalanceMode.NONE.name())))
                                    .build());

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    @Documentation.OverrideDefault("System.getProperty(\"log.file\")")
    public static final ConfigOption<String> TASK_MANAGER_LOG_PATH =
            ConfigOptions.key("taskmanager.log.path")
                    .stringType()
                    .defaultValue(System.getProperty("log.file"))
                    .withDescription("The path to the log file of the task manager.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Duration> FS_STREAM_OPENING_TIME_OUT =
            ConfigOptions.key("taskmanager.runtime.fs-timeout")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDeprecatedKeys("taskmanager.runtime.fs_timeout")
                    .withDescription(
                            "The timeout for filesystem stream opening. A value of 0 indicates infinite waiting.");

    @Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
    public static final ConfigOption<Integer> MINI_CLUSTER_NUM_TASK_MANAGERS =
            ConfigOptions.key("minicluster.number-of-taskmanagers")
                    .intType()
                    .defaultValue(1)
                    .withDeprecatedKeys("local.number-taskmanager")
                    .withDescription("The number of task managers of MiniCluster.");

    /** Type of redirection of {@link System#out} and {@link System#err}. */
    public enum SystemOutMode {

        /** Don't change the {@link System#out} and {@link System#err}, it's the default value. */
        DEFAULT,

        /** Redirect all {@link System#out} and {@link System#err} to LOG. */
        LOG,

        /** Ignore all {@link System#out} and {@link System#err} directly. */
        IGNORE
    }

    /** Type of {@link TaskManagerOptions#TASK_MANAGER_LOAD_BALANCE_MODE}. */
    public enum TaskManagerLoadBalanceMode {
        NONE,
        SLOTS
    }

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private TaskManagerOptions() {}
}
