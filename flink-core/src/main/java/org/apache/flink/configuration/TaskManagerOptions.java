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

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
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
	 */
	@Deprecated
	public static final ConfigOption<String> TASK_MANAGER_HEAP_MEMORY =
			key("taskmanager.heap.size")
			.defaultValue("1024m")
			.withDescription("JVM heap size for the TaskManagers, which are the parallel workers of" +
					" the system. On YARN setups, this value is automatically configured to the size of the TaskManager's" +
					" YARN container, minus a certain tolerance value.");

	/**
	 * JVM heap size (in megabytes) for the TaskManagers.
	 *
	 * @deprecated use {@link #TASK_MANAGER_HEAP_MEMORY}
	 */
	@Deprecated
	public static final ConfigOption<Integer> TASK_MANAGER_HEAP_MEMORY_MB =
			key("taskmanager.heap.mb")
			.defaultValue(1024)
			.withDescription("JVM heap size (in megabytes) for the TaskManagers, which are the parallel workers of" +
				" the system. On YARN setups, this value is automatically configured to the size of the TaskManager's" +
				" YARN container, minus a certain tolerance value.");

	/**
	 * Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.
	 */
	public static final ConfigOption<Boolean> KILL_ON_OUT_OF_MEMORY =
			key("taskmanager.jvm-exit-on-oom")
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
			.defaultValue("0")
			.withDescription("The task manager’s IPC port. Accepts a list of ports (“50100,50101”), ranges" +
				" (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid" +
				" collisions when multiple TaskManagers are running on the same machine.");

	/**
	 * The initial registration backoff between two consecutive registration attempts. The backoff
	 * is doubled for each new registration attempt until it reaches the maximum registration backoff.
	 */
	public static final ConfigOption<String> INITIAL_REGISTRATION_BACKOFF =
		key("taskmanager.registration.initial-backoff")
			.defaultValue("500 ms")
			.withDeprecatedKeys("taskmanager.initial-registration-pause")
			.withDescription("The initial registration backoff between two consecutive registration attempts. The backoff" +
				" is doubled for each new registration attempt until it reaches the maximum registration backoff.");

	/**
	 * The maximum registration backoff between two consecutive registration attempts.
	 */
	public static final ConfigOption<String> REGISTRATION_MAX_BACKOFF =
		key("taskmanager.registration.max-backoff")
			.defaultValue("30 s")
			.withDeprecatedKeys("taskmanager.max-registration-pause")
			.withDescription("The maximum registration backoff between two consecutive registration attempts. The max" +
				" registration backoff requires a time unit specifier (ms/s/min/h/d).");

	/**
	 * The backoff after a registration has been refused by the job manager before retrying to connect.
	 */
	public static final ConfigOption<String> REFUSED_REGISTRATION_BACKOFF =
		key("taskmanager.registration.refused-backoff")
			.defaultValue("10 s")
			.withDeprecatedKeys("taskmanager.refused-registration-pause")
			.withDescription("The backoff after a registration has been refused by the job manager before retrying to connect.");

	/**
	 * Defines the timeout it can take for the TaskManager registration. If the duration is
	 * exceeded without a successful registration, then the TaskManager terminates.
	 */
	public static final ConfigOption<String> REGISTRATION_TIMEOUT =
		key("taskmanager.registration.timeout")
			.defaultValue("5 min")
			.withDeprecatedKeys("taskmanager.maxRegistrationDuration")
			.withDescription("Defines the timeout for the TaskManager registration. If the duration is" +
				" exceeded without a successful registration, then the TaskManager terminates.");

	/**
	 * The config parameter defining the number of task slots of a task manager.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_PARALLELISM_SLOTS)
	public static final ConfigOption<Integer> NUM_TASK_SLOTS =
		key("taskmanager.numberOfTaskSlots")
			.defaultValue(1)
			.withDescription("The number of parallel operator or user function instances that a single TaskManager can" +
				" run. If this value is larger than 1, a single TaskManager takes multiple instances of a function or" +
				" operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the" +
				" available memory is divided between the different operator or function instances. This value" +
				" is typically proportional to the number of physical CPU cores that the TaskManager's machine has" +
				" (e.g., equal to the number of cores, or half the number of cores).");

	public static final ConfigOption<Boolean> DEBUG_MEMORY_LOG =
		key("taskmanager.debug.memory.log")
			.defaultValue(false)
			.withDeprecatedKeys("taskmanager.debug.memory.startLogThread")
			.withDescription("Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.");

	public static final ConfigOption<Long> DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS =
		key("taskmanager.debug.memory.log-interval")
			.defaultValue(5000L)
			.withDeprecatedKeys("taskmanager.debug.memory.logIntervalMs")
			.withDescription("The interval (in ms) for the log thread to log the current memory usage.");

	// ------------------------------------------------------------------------
	//  Managed Memory Options
	// ------------------------------------------------------------------------

	/**
	 * Size of memory buffers used by the network stack and the memory manager.
	 */
	public static final ConfigOption<String> MEMORY_SEGMENT_SIZE =
			key("taskmanager.memory.segment-size")
			.defaultValue("32kb")
			.withDescription("Size of memory buffers used by the network stack and the memory manager.");

	/**
	 * Amount of memory to be allocated by the task manager's memory manager. If not
	 * set, a relative fraction will be allocated, as defined by {@link #LEGACY_MANAGED_MEMORY_FRACTION}.
	 */
	@Deprecated
	public static final ConfigOption<String> LEGACY_MANAGED_MEMORY_SIZE =
			key("taskmanager.memory.size")
			.defaultValue("0")
			.withDescription("The amount of memory (in megabytes) that the task manager reserves on-heap or off-heap" +
				" (depending on taskmanager.memory.off-heap) for sorting, hash tables, and caching of intermediate" +
				" results. If unspecified, the memory manager will take a fixed ratio with respect to the size of" +
				" the task manager JVM as specified by taskmanager.memory.fraction.");

	/**
	 * Fraction of free memory allocated by the memory manager if {@link #LEGACY_MANAGED_MEMORY_SIZE} is
	 * not set.
	 */
	@Deprecated
	public static final ConfigOption<Float> LEGACY_MANAGED_MEMORY_FRACTION =
			key("taskmanager.memory.fraction")
			.defaultValue(0.7f)
			.withDescription(new Description.DescriptionBuilder()
				.text("The relative amount of memory (after subtracting the amount of memory used by network" +
					" buffers) that the task manager reserves for sorting, hash tables, and caching of intermediate results." +
					" For example, a value of %s means that a task manager reserves 80% of its memory" +
					" (on-heap or off-heap depending on taskmanager.memory.off-heap)" +
					" for internal data buffers, leaving 20% of free memory for the task manager's heap for objects" +
					" created by user-defined functions. This parameter is only evaluated, if " +
					LEGACY_MANAGED_MEMORY_SIZE.key() + " is not set.", code("0.8"))
				.build());

	/**
	 * Memory allocation method (JVM heap or off-heap), used for managed memory of the TaskManager
	 * as well as the network buffers.
	 **/
	@Deprecated
	public static final ConfigOption<Boolean> MEMORY_OFF_HEAP =
			key("taskmanager.memory.off-heap")
			.defaultValue(true)
				.withDescription("Memory allocation method (JVM heap or off-heap), used for managed memory of the" +
						" TaskManager. For setups with larger quantities of memory, this can" +
						" improve the efficiency of the operations performed on the memory.");

	/**
	 * The config parameter for automatically defining the TaskManager's binding address,
	 * if {@link #HOST} configuration option is not set.
	 */
	public static final ConfigOption<String> HOST_BIND_POLICY =
		key("taskmanager.network.bind-policy")
			.defaultValue("ip")
			.withDescription(Description.builder()
				.text("The automatic address binding policy used by the TaskManager if \"" + HOST.key() + "\" is not set." +
					" The value should be one of the following:\n")
				.list(
					text("\"name\" - uses hostname as binding address"),
					text("\"ip\" - uses host's ip address as binding address"))
				.build());

	// ------------------------------------------------------------------------
	//  Memory Options
	// ------------------------------------------------------------------------

	/**
	 * Total Process Memory size for the TaskExecutors.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_MEMORY)
	public static final ConfigOption<String> TOTAL_PROCESS_MEMORY =
		key("taskmanager.memory.total-process.size")
			.noDefaultValue()
			.withDeprecatedKeys(TASK_MANAGER_HEAP_MEMORY.key())
			.withDescription("Total Process Memory size for the TaskExecutors. This includes all the memory that a"
				+ " TaskExecutor consumes, consisting of Total Flink Memory, JVM Metaspace, and JVM Overhead. On"
				+ " containerized setups, this should be set to the container memory.");

	/**
	 * Total Flink Memory size for the TaskExecutors.
	 */
	public static final ConfigOption<String> TOTAL_FLINK_MEMORY =
		key("taskmanager.memory.total-flink.size")
		.noDefaultValue()
		.withDescription("Total Flink Memory size for the TaskExecutors. This includes all the memory that a"
			+ " TaskExecutor consumes, except for JVM Metaspace and JVM Overhead. It consists of Framework Heap Memory,"
			+ " Task Heap Memory, Task Off-Heap Memory, Managed Memory, and Shuffle Memory.");

	/**
	 * Framework Heap Memory size for TaskExecutors.
	 */
	public static final ConfigOption<String> FRAMEWORK_HEAP_MEMORY =
		key("taskmanager.memory.framework.heap.size")
			.defaultValue("128m")
			.withDescription("Framework Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved"
				+ " for TaskExecutor framework, which will not be allocated to task slots.");

	/**
	 * Framework Off-Heap Memory size for TaskExecutors.
	 */
	public static final ConfigOption<String> FRAMEWORK_OFF_HEAP_MEMORY =
		key("taskmanager.memory.framework.off-heap.size")
			.defaultValue("128m")
			.withDescription("Framework Off-Heap Memory size for TaskExecutors. This is the size of off-heap memory"
				+ " (JVM direct memory or native memory) reserved for TaskExecutor framework, which will not be"
				+ " allocated to task slots. It will be accounted as part of the JVM max direct memory size limit.");

	/**
	 * Task Heap Memory size for TaskExecutors.
	 */
	public static final ConfigOption<String> TASK_HEAP_MEMORY =
		key("taskmanager.memory.task.heap.size")
			.noDefaultValue()
			.withDescription("Task Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved for"
				+ " user code. If not specified, it will be derived as Total Flink Memory minus Framework Heap Memory,"
				+ " Task Off-Heap Memory, (On-Heap and Off-Heap) Managed Memory and Shuffle Memory.");

	/**
	 * Task Off-Heap Memory size for TaskExecutors.
	 */
	public static final ConfigOption<String> TASK_OFF_HEAP_MEMORY =
		key("taskmanager.memory.task.off-heap.size")
			.defaultValue("0b")
			.withDescription("Task Heap Memory size for TaskExecutors. This is the size of off heap memory (JVM direct"
				+ " memory or native memory) reserved for user code.");

	/**
	 * Managed Memory size for TaskExecutors.
	 */
	public static final ConfigOption<String> MANAGED_MEMORY_SIZE =
		key("taskmanager.memory.managed.size")
			.noDefaultValue()
			.withDeprecatedKeys(LEGACY_MANAGED_MEMORY_SIZE.key())
			.withDescription("Managed Memory size for TaskExecutors. This is the size of memory managed by the memory"
				+ " manager, including both On-Heap Managed Memory and Off-Heap Managed Memory, reserved for sorting,"
				+ " hash tables, caching of intermediate results and state backends. Memory consumers can either"
				+ " allocate memory from the memory manager in the form of MemorySegments, or reserve bytes from the"
				+ " memory manager and keep their memory usage within that boundary. If unspecified, it will be derived"
				+ " to make up the configured fraction of the Total Flink Memory.");

	/**
	 * Fraction of Total Flink Memory to be used as Managed Memory, if {@link #MANAGED_MEMORY_SIZE} is not specified.
	 */
	public static final ConfigOption<Float> MANAGED_MEMORY_FRACTION =
		key("taskmanager.memory.managed.fraction")
			.defaultValue(0.4f)
			.withDescription("Fraction of Total Flink Memory to be used as Managed Memory, if Managed Memory size is not"
				+ " explicitly specified.");

	/**
	 * Off-Heap Managed Memory size for TaskExecutors.
	 */
	public static final ConfigOption<String> MANAGED_MEMORY_OFFHEAP_SIZE =
		key("taskmanager.memory.managed.off-heap.size")
			.noDefaultValue()
			.withDescription("Off-Heap Managed Memory size for TaskExecutors. This is the part of Managed Memory that is"
				+ " off-heap, while the remaining is on-heap. If unspecified, it will be derived to make up the"
				+ " configured fraction of the Managed Memory size.");

	/**
	 * Fraction of Managed Memory that Off-Heap Managed Memory takes.
	 */
	public static final ConfigOption<Float> MANAGED_MEMORY_OFFHEAP_FRACTION =
		key("taskmanager.memory.managed.off-heap.fraction")
			.defaultValue(-1.0f)
			.withDescription("Fraction of Managed Memory that Off-Heap Managed Memory takes, if Off-Heap Managed Memory"
				+ " size is not explicitly specified. If the fraction is not explicitly specified (or configured with"
				+ " negative values), it will be derived from the legacy config option '"
				+ TaskManagerOptions.MEMORY_OFF_HEAP.key() + "', to use either all on-heap memory or all off-heap memory"
				+ " for Managed Memory.");

	/**
	 * Min Shuffle Memory size for TaskExecutors.
	 */
	public static final ConfigOption<String> SHUFFLE_MEMORY_MIN =
		key("taskmanager.memory.shuffle.min")
			.defaultValue("64m")
			.withDeprecatedKeys(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.key())
			.withDescription("Min Shuffle Memory size for TaskExecutors. Shuffle Memory is off-heap memory reserved for"
				+ " ShuffleEnvironment (e.g., network buffers). Shuffle Memory size is derived to make up the configured"
				+ " fraction of the Total Flink Memory. If the derived size is less/greater than the configured min/max"
				+ " size, the min/max size will be used. The exact size of Shuffle Memory can be explicitly specified by"
				+ " setting the min/max to the same value.");

	/**
	 * Max Shuffle Memory size for TaskExecutors.
	 */
	public static final ConfigOption<String> SHUFFLE_MEMORY_MAX =
		key("taskmanager.memory.shuffle.max")
			.defaultValue("1g")
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
	public static final ConfigOption<String> JVM_METASPACE =
		key("taskmanager.memory.jvm-metaspace.size")
			.defaultValue("192m")
			.withDescription("JVM Metaspace Size for the TaskExecutors.");

	/**
	 * Min JVM Overhead size for the TaskExecutors.
	 */
	public static final ConfigOption<String> JVM_OVERHEAD_MIN =
		key("taskmanager.memory.jvm-overhead.min")
			.defaultValue("128m")
			.withDescription("Min JVM Overhead size for the TaskExecutors. This is off-heap memory reserved for JVM"
				+ " overhead, such as thread stack space, I/O direct memory, compile cache, etc. The size of JVM"
				+ " Overhead is derived to make up the configured fraction of the Total Process Memory. If the derived"
				+ " size is less/greater than the configured min/max size, the min/max size will be used. The exact size"
				+ " of JVM Overhead can be explicitly specified by setting the min/max size to the same value.");

	/**
	 * Max JVM Overhead size for the TaskExecutors.
	 */
	public static final ConfigOption<String> JVM_OVERHEAD_MAX =
		key("taskmanager.memory.jvm-overhead.max")
			.defaultValue("1g")
			.withDescription("Max JVM Overhead size for the TaskExecutors. This is off-heap memory reserved for JVM"
				+ " overhead, such as thread stack space, I/O direct memory, compile cache, etc. The size of JVM"
				+ " Overhead is derived to make up the configured fraction of the Total Process Memory. If the derived"
				+ " size is less/greater than the configured min/max size, the min/max size will be used. The exact size"
				+ " of JVM Overhead can be explicitly specified by setting the min/max size to the same value.");

	/**
	 * Fraction of Total Process Memory to be reserved for JVM Overhead.
	 */
	public static final ConfigOption<Float> JVM_OVERHEAD_FRACTION =
		key("taskmanager.memory.jvm-overhead.fraction")
			.defaultValue(0.1f)
			.withDescription("Fraction of Total Process Memory to be reserved for JVM Overhead. This is off-heap memory"
				+ " reserved for JVM overhead, such as thread stack space, I/O direct memory, compile cache, etc. The"
				+ " size of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If"
				+ " the derived size is less/greater than the configured min/max size, the min/max size will be used."
				+ " The exact size of JVM Overhead can be explicitly specified by setting the min/max size to the same"
				+ " value.");

	// ------------------------------------------------------------------------
	//  Task Options
	// ------------------------------------------------------------------------

	/**
	 * Time interval in milliseconds between two successive task cancellation
	 * attempts.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_INTERVAL =
			key("task.cancellation.interval")
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
			.defaultValue(-1L)
			.withDescription("The maximum number of bytes that a checkpoint alignment may buffer. If the checkpoint" +
				" alignment buffers more than the configured amount of data, the checkpoint is aborted (skipped)." +
				" A value of -1 indicates that there is no limit.");

	// ------------------------------------------------------------------------

	/**
	 * Toggle to switch between FLIP-49 and current task manager memory configurations.
	 */
	@Documentation.ExcludeFromDocumentation("FLIP-49 is still in development.")
	public static final ConfigOption<Boolean> ENABLE_FLIP_49_CONFIG =
			key("taskmanager.enable-flip-49")
			.defaultValue(false)
			.withDescription("Toggle to switch between FLIP-49 and current task manager memory configurations.");

	/** Not intended to be instantiated. */
	private TaskManagerOptions() {}
}
