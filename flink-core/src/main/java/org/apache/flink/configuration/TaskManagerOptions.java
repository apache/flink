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

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * The set of configuration options relating to TaskManager and Task settings.
 */
@PublicEvolving
@ConfigGroups(groups = @ConfigGroup(name = "TaskManagerMemory", keyPrefix = "taskmanager.memory"))
public class TaskManagerOptions {

	// ------------------------------------------------------------------------
	//  General TaskManager Options
	// ------------------------------------------------------------------------

	/**
	 * JVM heap size for the TaskManagers with memory size.
	 *
	 * @deprecated use {@link #TOTAL_FLINK_MEMORY} for standalone setups and {@link #TOTAL_PROCESS_MEMORY} for containerized setups.
	 */
	@Deprecated
	public static final ConfigOption<MemorySize> TASK_MANAGER_HEAP_MEMORY =
		key("taskmanager.heap.size")
			.memoryType()
			.noDefaultValue()
			.withDescription("JVM heap size for the TaskManagers, which are the parallel workers of" +
				" the system. On YARN setups, this value is automatically configured to the size of the TaskManager's" +
				" YARN container, minus a certain tolerance value.");

	/**
	 * JVM heap size (in megabytes) for the TaskManagers.
	 *
	 * @deprecated use {@link #TOTAL_FLINK_MEMORY} for standalone setups and {@link #TOTAL_PROCESS_MEMORY} for containerized setups.
	 */
	@Deprecated
	public static final ConfigOption<Integer> TASK_MANAGER_HEAP_MEMORY_MB =
			key("taskmanager.heap.mb")
			.intType()
			.noDefaultValue()
			.withDescription("JVM heap size (in megabytes) for the TaskManagers, which are the parallel workers of" +
				" the system. On YARN setups, this value is automatically configured to the size of the TaskManager's" +
				" YARN container, minus a certain tolerance value.");

	/**
	 * Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.
	 */
	public static final ConfigOption<Boolean> KILL_ON_OUT_OF_MEMORY =
			key("taskmanager.jvm-exit-on-oom")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.");

	/**
	 * Whether the quarantine monitor for task managers shall be started. The quarantine monitor
	 * shuts down the actor system if it detects that it has quarantined another actor system
	 * or if it has been quarantined by another actor system.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> EXIT_ON_FATAL_AKKA_ERROR =
			key("taskmanager.exit-on-fatal-akka-error")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether the quarantine monitor for task managers shall be started. The quarantine monitor" +
				" shuts down the actor system if it detects that it has quarantined another actor system" +
				" or if it has been quarantined by another actor system.");

	/**
	 * The config parameter defining the task manager's hostname.
	 * Overrides {@link #HOST_BIND_POLICY} automatic address binding.
	 */
	public static final ConfigOption<String> HOST =
		key("taskmanager.host")
			.stringType()
			.noDefaultValue()
			.withDescription("The address of the network interface that the TaskManager binds to." +
				" This option can be used to define explicitly a binding address. Because" +
				" different TaskManagers need different values for this option, usually it is specified in an" +
				" additional non-shared TaskManager-specific config file.");

	/**
	 * The default network port range the task manager expects incoming IPC connections. The {@code "0"} means that
	 * the TaskManager searches for a free port.
	 */
	public static final ConfigOption<String> RPC_PORT =
		key("taskmanager.rpc.port")
			.stringType()
			.defaultValue("0")
			.withDescription("The task manager’s IPC port. Accepts a list of ports (“50100,50101”), ranges" +
				" (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid" +
				" collisions when multiple TaskManagers are running on the same machine.");

	/**
	 * The initial registration backoff between two consecutive registration attempts. The backoff
	 * is doubled for each new registration attempt until it reaches the maximum registration backoff.
	 */
	public static final ConfigOption<Duration> INITIAL_REGISTRATION_BACKOFF =
		key("taskmanager.registration.initial-backoff")
			.durationType()
			.defaultValue(TimeUtils.parseDuration("500 ms"))
			.withDeprecatedKeys("taskmanager.initial-registration-pause")
			.withDescription("The initial registration backoff between two consecutive registration attempts. The backoff" +
				" is doubled for each new registration attempt until it reaches the maximum registration backoff.");

	/**
	 * The maximum registration backoff between two consecutive registration attempts.
	 */
	public static final ConfigOption<Duration> REGISTRATION_MAX_BACKOFF =
		key("taskmanager.registration.max-backoff")
			.durationType()
			.defaultValue(TimeUtils.parseDuration("30 s"))
			.withDeprecatedKeys("taskmanager.max-registration-pause")
			.withDescription("The maximum registration backoff between two consecutive registration attempts. The max" +
				" registration backoff requires a time unit specifier (ms/s/min/h/d).");

	/**
	 * The backoff after a registration has been refused by the job manager before retrying to connect.
	 */
	public static final ConfigOption<Duration> REFUSED_REGISTRATION_BACKOFF =
		key("taskmanager.registration.refused-backoff")
			.durationType()
			.defaultValue(TimeUtils.parseDuration("10 s"))
			.withDeprecatedKeys("taskmanager.refused-registration-pause")
			.withDescription("The backoff after a registration has been refused by the job manager before retrying to connect.");

	/**
	 * Defines the timeout it can take for the TaskManager registration. If the duration is
	 * exceeded without a successful registration, then the TaskManager terminates.
	 */
	public static final ConfigOption<Duration> REGISTRATION_TIMEOUT =
		key("taskmanager.registration.timeout")
			.durationType()
			.defaultValue(TimeUtils.parseDuration("5 min"))
			.withDeprecatedKeys("taskmanager.maxRegistrationDuration")
			.withDescription("Defines the timeout for the TaskManager registration. If the duration is" +
				" exceeded without a successful registration, then the TaskManager terminates.");

	/**
	 * The config parameter defining the number of task slots of a task manager.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_PARALLELISM_SLOTS)
	public static final ConfigOption<Integer> NUM_TASK_SLOTS =
		key("taskmanager.numberOfTaskSlots")
			.intType()
			.defaultValue(1)
			.withDescription("The number of parallel operator or user function instances that a single TaskManager can" +
				" run. If this value is larger than 1, a single TaskManager takes multiple instances of a function or" +
				" operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the" +
				" available memory is divided between the different operator or function instances. This value" +
				" is typically proportional to the number of physical CPU cores that the TaskManager's machine has" +
				" (e.g., equal to the number of cores, or half the number of cores).");

	public static final ConfigOption<Boolean> DEBUG_MEMORY_LOG =
		key("taskmanager.debug.memory.log")
			.booleanType()
			.defaultValue(false)
			.withDeprecatedKeys("taskmanager.debug.memory.startLogThread")
			.withDescription("Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.");

	public static final ConfigOption<Long> DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS =
		key("taskmanager.debug.memory.log-interval")
			.longType()
			.defaultValue(5000L)
			.withDeprecatedKeys("taskmanager.debug.memory.logIntervalMs")
			.withDescription("The interval (in ms) for the log thread to log the current memory usage.");

	// ------------------------------------------------------------------------
	//  Managed Memory Options
	// ------------------------------------------------------------------------

	/**
	 * Size of memory buffers used by the network stack and the memory manager.
	 */
	public static final ConfigOption<MemorySize> MEMORY_SEGMENT_SIZE =
			key("taskmanager.memory.segment-size")
			.memoryType()
			.defaultValue(MemorySize.parse("32kb"))
			.withDescription("Size of memory buffers used by the network stack and the memory manager.");

	/**
	 * The config parameter for automatically defining the TaskManager's binding address,
	 * if {@link #HOST} configuration option is not set.
	 */
	public static final ConfigOption<String> HOST_BIND_POLICY =
		key("taskmanager.network.bind-policy")
			.stringType()
			.defaultValue("ip")
			.withDescription(Description.builder()
				.text("The automatic address binding policy used by the TaskManager if \"" + HOST.key() + "\" is not set." +
					" The value should be one of the following:\n")
				.list(
					text("\"name\" - uses hostname as binding address"),
					text("\"ip\" - uses host's ip address as binding address"))
				.build());

	// ------------------------------------------------------------------------
	//  Resource Options
	// ------------------------------------------------------------------------

	/**
	 * This config option describes number of cpu cores of task executors.
	 * In case of Yarn / Mesos / Kubernetes, it is used to launch a container for the task executor.
	 *
	 * <p>DO NOT USE THIS CONFIG OPTION.
	 * This config option is currently only used internally, for passing cpu cores into task executors
	 * for dynamic fine grained slot resource management. The feature is not completed at the moment,
	 * and the config option is experimental and might be changed / removed in the future. Thus, we do
	 * not expose this config option to users.
	 *
	 * <p>For configuring the cpu cores of container on Yarn / Mesos / Kubernetes, please use
	 * {@link YarnConfigOptions#VCORES}, {@link MesosTaskManagerParameters#MESOS_RM_TASKS_CPUS} and
	 * {@link KubernetesConfigOptions#TASK_MANAGER_CPU}.
	 */
	@Documentation.ExcludeFromDocumentation
	public static final ConfigOption<Double> CPU_CORES =
		key("taskmanager.cpu.cores")
			.doubleType()
			.noDefaultValue()
			.withDescription("CPU cores for the TaskExecutors. In case of Yarn setups, this value will be rounded to "
				+ "the closest positive integer. If not explicitly configured, legacy config options "
				+ "'yarn.containers.vcores', 'mesos.resourcemanager.tasks.cpus' and 'kubernetes.taskmanager.cpu' will be "
				+ "used for Yarn / Mesos / Kubernetes setups, and '" + NUM_TASK_SLOTS.key() + "' will be used for "
				+ "standalone setups (approximate number of slots).");

	/**
	 * Total Process Memory size for the TaskExecutors.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_MEMORY)
	public static final ConfigOption<MemorySize> TOTAL_PROCESS_MEMORY =
		key("taskmanager.memory.process.size")
			.memoryType()
			.noDefaultValue()
			.withDescription("Total Process Memory size for the TaskExecutors. This includes all the memory that a"
				+ " TaskExecutor consumes, consisting of Total Flink Memory, JVM Metaspace, and JVM Overhead. On"
				+ " containerized setups, this should be set to the container memory.");

	/**
	 * Total Flink Memory size for the TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> TOTAL_FLINK_MEMORY =
		key("taskmanager.memory.flink.size")
		.memoryType()
		.noDefaultValue()
		.withDescription("Total Flink Memory size for the TaskExecutors. This includes all the memory that a"
			+ " TaskExecutor consumes, except for JVM Metaspace and JVM Overhead. It consists of Framework Heap Memory,"
			+ " Task Heap Memory, Task Off-Heap Memory, Managed Memory, and Shuffle Memory.");

	/**
	 * Framework Heap Memory size for TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> FRAMEWORK_HEAP_MEMORY =
		key("taskmanager.memory.framework.heap.size")
			.memoryType()
			.defaultValue(MemorySize.parse("128m"))
			.withDescription("Framework Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved"
				+ " for TaskExecutor framework, which will not be allocated to task slots.");

	/**
	 * Framework Off-Heap Memory size for TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> FRAMEWORK_OFF_HEAP_MEMORY =
		key("taskmanager.memory.framework.off-heap.size")
			.memoryType()
			.defaultValue(MemorySize.parse("128m"))
			.withDescription("Framework Off-Heap Memory size for TaskExecutors. This is the size of off-heap memory"
				+ " (JVM direct memory and native memory) reserved for TaskExecutor framework, which will not be"
				+ " allocated to task slots. The configured value will be fully counted when Flink calculates the JVM"
				+ " max direct memory size parameter.");

	/**
	 * Task Heap Memory size for TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> TASK_HEAP_MEMORY =
		key("taskmanager.memory.task.heap.size")
			.memoryType()
			.noDefaultValue()
			.withDescription("Task Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved for"
				+ " tasks. If not specified, it will be derived as Total Flink Memory minus Framework Heap Memory,"
				+ " Task Off-Heap Memory, Managed Memory and Shuffle Memory.");

	/**
	 * Task Off-Heap Memory size for TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> TASK_OFF_HEAP_MEMORY =
		key("taskmanager.memory.task.off-heap.size")
			.memoryType()
			.defaultValue(MemorySize.ZERO)
			.withDescription("Task Off-Heap Memory size for TaskExecutors. This is the size of off heap memory (JVM"
				+ " direct memory and native memory) reserved for tasks. The configured value will be fully counted"
				+ " when Flink calculates the JVM max direct memory size parameter.");

	/**
	 * Managed Memory size for TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> MANAGED_MEMORY_SIZE =
		key("taskmanager.memory.managed.size")
			.memoryType()
			.noDefaultValue()
			.withDeprecatedKeys("taskmanager.memory.size")
			.withDescription("Managed Memory size for TaskExecutors. This is the size of off-heap memory managed by the"
				+ " memory manager, reserved for sorting, hash tables, caching of intermediate results and RocksDB state"
				+ " backend. Memory consumers can either allocate memory from the memory manager in the form of"
				+ " MemorySegments, or reserve bytes from the memory manager and keep their memory usage within that"
				+ " boundary. If unspecified, it will be derived to make up the configured fraction of the Total Flink"
				+ " Memory.");

	/**
	 * Fraction of Total Flink Memory to be used as Managed Memory, if {@link #MANAGED_MEMORY_SIZE} is not specified.
	 */
	public static final ConfigOption<Float> MANAGED_MEMORY_FRACTION =
		key("taskmanager.memory.managed.fraction")
			.floatType()
			.defaultValue(0.4f)
			.withDescription("Fraction of Total Flink Memory to be used as Managed Memory, if Managed Memory size is not"
				+ " explicitly specified.");

	/**
	 * Min Shuffle Memory size for TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> SHUFFLE_MEMORY_MIN =
		key("taskmanager.memory.shuffle.min")
			.memoryType()
			.defaultValue(MemorySize.parse("64m"))
			.withDeprecatedKeys(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.key())
			.withDescription("Min Shuffle Memory size for TaskExecutors. Shuffle Memory is off-heap memory reserved for"
				+ " ShuffleEnvironment (e.g., network buffers). Shuffle Memory size is derived to make up the configured"
				+ " fraction of the Total Flink Memory. If the derived size is less/greater than the configured min/max"
				+ " size, the min/max size will be used. The exact size of Shuffle Memory can be explicitly specified by"
				+ " setting the min/max to the same value.");

	/**
	 * Max Shuffle Memory size for TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> SHUFFLE_MEMORY_MAX =
		key("taskmanager.memory.shuffle.max")
			.memoryType()
			.defaultValue(MemorySize.parse("1g"))
			.withDeprecatedKeys(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key())
			.withDescription("Max Shuffle Memory size for TaskExecutors. Shuffle Memory is off-heap memory reserved for"
				+ " ShuffleEnvironment (e.g., network buffers). Shuffle Memory size is derived to make up the configured"
				+ " fraction of the Total Flink Memory. If the derived size is less/greater than the configured min/max"
				+ " size, the min/max size will be used. The exact size of Shuffle Memory can be explicitly specified by"
				+ " setting the min/max to the same value.");

	/**
	 * Fraction of Total Flink Memory to be used as Shuffle Memory.
	 */
	public static final ConfigOption<Float> SHUFFLE_MEMORY_FRACTION =
		key("taskmanager.memory.shuffle.fraction")
			.floatType()
			.defaultValue(0.1f)
			.withDeprecatedKeys(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key())
			.withDescription("Fraction of Total Flink Memory to be used as Shuffle Memory. Shuffle Memory is off-heap"
				+ " memory reserved for ShuffleEnvironment (e.g., network buffers). Shuffle Memory size is derived to"
				+ " make up the configured fraction of the Total Flink Memory. If the derived size is less/greater than"
				+ " the configured min/max size, the min/max size will be used. The exact size of Shuffle Memory can be"
				+ " explicitly specified by setting the min/max size to the same value.");

	/**
	 * JVM Metaspace Size for the TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> JVM_METASPACE =
		key("taskmanager.memory.jvm-metaspace.size")
			.memoryType()
			.defaultValue(MemorySize.parse("128m"))
			.withDescription("JVM Metaspace Size for the TaskExecutors.");

	/**
	 * Min JVM Overhead size for the TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> JVM_OVERHEAD_MIN =
		key("taskmanager.memory.jvm-overhead.min")
			.memoryType()
			.defaultValue(MemorySize.parse("128m"))
			.withDescription("Min JVM Overhead size for the TaskExecutors. This is off-heap memory reserved for JVM"
				+ " overhead, such as thread stack space, compile cache, etc. This includes native memory but not direct"
				+ " memory, and will not be counted when Flink calculates JVM max direct memory size parameter. The size"
				+ " of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If the"
				+ " derived size is less/greater than the configured min/max size, the min/max size will be used. The"
				+ " exact size of JVM Overhead can be explicitly specified by setting the min/max size to the same value.");

	/**
	 * Max JVM Overhead size for the TaskExecutors.
	 */
	public static final ConfigOption<MemorySize> JVM_OVERHEAD_MAX =
		key("taskmanager.memory.jvm-overhead.max")
			.memoryType()
			.defaultValue(MemorySize.parse("1g"))
			.withDescription("Max JVM Overhead size for the TaskExecutors. This is off-heap memory reserved for JVM"
				+ " overhead, such as thread stack space, compile cache, etc. This includes native memory but not direct"
				+ " memory, and will not be counted when Flink calculates JVM max direct memory size parameter. The size"
				+ " of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If the"
				+ " derived size is less/greater than the configured min/max size, the min/max size will be used. The"
				+ " exact size of JVM Overhead can be explicitly specified by setting the min/max size to the same value.");

	/**
	 * Fraction of Total Process Memory to be reserved for JVM Overhead.
	 */
	public static final ConfigOption<Float> JVM_OVERHEAD_FRACTION =
		key("taskmanager.memory.jvm-overhead.fraction")
			.floatType()
			.defaultValue(0.1f)
			.withDescription("Fraction of Total Process Memory to be reserved for JVM Overhead. This is off-heap memory"
				+ " reserved for JVM overhead, such as thread stack space, compile cache, etc. This includes native"
				+ " memory but not direct memory, and will not be counted when Flink calculates JVM max direct memory"
				+ " size parameter. The size of JVM Overhead is derived to make up the configured fraction of the Total"
				+ " Process Memory. If the derived size is less/greater than the configured min/max size, the min/max"
				+ " size will be used. The exact size of JVM Overhead can be explicitly specified by setting the min/max"
				+ " size to the same value.");

	// ------------------------------------------------------------------------
	//  Task Options
	// ------------------------------------------------------------------------

	/**
	 * Time interval in milliseconds between two successive task cancellation
	 * attempts.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_INTERVAL =
			key("task.cancellation.interval")
			.longType()
			.defaultValue(30000L)
			.withDeprecatedKeys("task.cancellation-interval")
			.withDescription("Time interval between two successive task cancellation attempts in milliseconds.");

	/**
	 * Timeout in milliseconds after which a task cancellation times out and
	 * leads to a fatal TaskManager error. A value of <code>0</code> deactivates
	 * the watch dog.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT =
			key("task.cancellation.timeout")
			.longType()
			.defaultValue(180000L)
			.withDescription("Timeout in milliseconds after which a task cancellation times out and" +
				" leads to a fatal TaskManager error. A value of 0 deactivates" +
				" the watch dog.");
	/**
	 * This configures how long we wait for the timers in milliseconds to finish all pending timer threads
	 * when the stream task is cancelled.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT_TIMERS = ConfigOptions
			.key("task.cancellation.timers.timeout")
			.longType()
			.defaultValue(7500L)
			.withDeprecatedKeys("timerservice.exceptional.shutdown.timeout")
			.withDescription("Time we wait for the timers in milliseconds to finish all pending timer threads" +
				" when the stream task is cancelled.");

	/**
	 * The maximum number of bytes that a checkpoint alignment may buffer.
	 * If the checkpoint alignment buffers more than the configured amount of
	 * data, the checkpoint is aborted (skipped).
	 *
	 * <p>The default value of {@code -1} indicates that there is no limit.
	 */
	public static final ConfigOption<Long> TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT =
			key("task.checkpoint.alignment.max-size")
			.longType()
			.defaultValue(-1L)
			.withDescription("The maximum number of bytes that a checkpoint alignment may buffer. If the checkpoint" +
				" alignment buffers more than the configured amount of data, the checkpoint is aborted (skipped)." +
				" A value of -1 indicates that there is no limit.");

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private TaskManagerOptions() {}
}
