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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class contains all constants for the configuration. That includes the configuration keys and
 * the default values.
 */
@Public
@SuppressWarnings("unused")
public final class ConfigConstants {

	// ------------------------------------------------------------------------
	//                            Configuration Keys
	// ------------------------------------------------------------------------

	// ---------------------------- Parallelism -------------------------------

	/**
	 * The config parameter defining the default parallelism for jobs.
	 *
	 * @deprecated use {@link CoreOptions#DEFAULT_PARALLELISM} instead
	 */
	@Deprecated
	public static final String DEFAULT_PARALLELISM_KEY = "parallelism.default";

	// ---------------------------- Restart strategies ------------------------

	/**
	 * Defines the restart strategy to be used. It can be "off", "none", "disable" to be disabled or
	 * it can be "fixeddelay", "fixed-delay" to use the FixedDelayRestartStrategy or it can
	 * be "failurerate", "failure-rate" to use FailureRateRestartStrategy. You can also
	 * specify a class name which implements the RestartStrategy interface and has a static
	 * create method which takes a Configuration object.
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY = "restart-strategy";

	/**
	 * Maximum number of attempts the fixed delay restart strategy will try before failing a job.
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS = "restart-strategy.fixed-delay.attempts";

	/**
	 * Delay between two consecutive restart attempts in FixedDelayRestartStrategy. It can be specified using Scala's
	 * FiniteDuration notation: "1 min", "20 s"
	 */
	@PublicEvolving
	public static final ConfigOption<String> RESTART_STRATEGY_FIXED_DELAY_DELAY =
		key("restart-strategy.fixed-delay.delay").defaultValue("0 s");

	/**
	 * Maximum number of restarts in given time interval {@link #RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL} before failing a job
	 * in FailureRateRestartStrategy.
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL = "restart-strategy.failure-rate.max-failures-per-interval";

	/**
	 * Time interval in which greater amount of failures than {@link #RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL} causes
	 * job fail in FailureRateRestartStrategy. It can be specified using Scala's FiniteDuration notation: "1 min", "20 s"
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL = "restart-strategy.failure-rate.failure-rate-interval";

	/**
	 * Delay between two consecutive restart attempts in FailureRateRestartStrategy.
	 * It can be specified using Scala's FiniteDuration notation: "1 min", "20 s".
	 */
	@PublicEvolving
	public static final String RESTART_STRATEGY_FAILURE_RATE_DELAY = "restart-strategy.failure-rate.delay";

	/**
	 * Config parameter for the number of re-tries for failed tasks. Setting this
	 * value to 0 effectively disables fault tolerance.
	 *
	 * @deprecated The configuration value will be replaced by {@link #RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS}
	 * and the corresponding FixedDelayRestartStrategy.
	 */
	@Deprecated
	@PublicEvolving
	public static final String EXECUTION_RETRIES_KEY = "execution-retries.default";

	/**
	 * Config parameter for the delay between execution retries. The value must be specified in the
	 * notation "10 s" or "1 min" (style of Scala Finite Durations)
	 *
	 * @deprecated The configuration value will be replaced by {@link #RESTART_STRATEGY_FIXED_DELAY_DELAY}
	 * and the corresponding FixedDelayRestartStrategy.
	 */
	@Deprecated
	@PublicEvolving
	public static final String EXECUTION_RETRY_DELAY_KEY = "execution-retries.delay";

	// -------------------------------- Runtime -------------------------------

	/**
	 * The config parameter defining the network address to connect to
	 * for communication with the job manager.
	 *
	 * @deprecated Use {@link JobManagerOptions#ADDRESS} instead
	 */
	@Deprecated
	public static final String JOB_MANAGER_IPC_ADDRESS_KEY = "jobmanager.rpc.address";

	/**
	 * The config parameter defining the network port to connect to
	 * for communication with the job manager.
	 *
	 * @deprecated Use {@link JobManagerOptions#PORT} instead
	 */
	@Deprecated
	public static final String JOB_MANAGER_IPC_PORT_KEY = "jobmanager.rpc.port";

	/**
	 * The config parameter defining the network port to connect to
	 * for communication with the resource manager.
	 * @deprecated Use {@link ResourceManagerOptions#IPC_PORT} instead.
	 */
	@Deprecated
	public static final String RESOURCE_MANAGER_IPC_PORT_KEY = "resourcemanager.rpc.port";

	/**
	 * @deprecated use {@link BlobServerOptions#STORAGE_DIRECTORY} instead
	 */
	@Deprecated
	public static final String BLOB_STORAGE_DIRECTORY_KEY = "blob.storage.directory";

	/**
	 * @deprecated use {@link BlobServerOptions#FETCH_RETRIES} instead
	 */
	@Deprecated
	public static final String BLOB_FETCH_RETRIES_KEY = "blob.fetch.retries";

	/**
	 * @deprecated use {@link BlobServerOptions#FETCH_CONCURRENT} instead
	 */
	@Deprecated
	public static final String BLOB_FETCH_CONCURRENT_KEY = "blob.fetch.num-concurrent";

	/**
	 * @deprecated use {@link BlobServerOptions#FETCH_BACKLOG} instead
	 */
	@Deprecated
	public static final String BLOB_FETCH_BACKLOG_KEY = "blob.fetch.backlog";

	/**
	 * @deprecated use {@link BlobServerOptions#PORT} instead
	 */
	@Deprecated
	public static final String BLOB_SERVER_PORT = "blob.server.port";

	/**
	 * @deprecated use {@link BlobServerOptions#SSL_ENABLED} instead
	 */
	@Deprecated
	public static final String BLOB_SERVICE_SSL_ENABLED = "blob.service.ssl.enabled";

	/**
	 * The config parameter defining the cleanup interval of the library cache manager.
	 *
	 * @deprecated use {@link BlobServerOptions#CLEANUP_INTERVAL} instead
	 */
	@Deprecated
	public static final String LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL = "library-cache-manager.cleanup.interval";

	/**
	 * The config parameter defining the task manager's hostname.
	 *
	 * @deprecated use {@link TaskManagerOptions#HOST} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_HOSTNAME_KEY = "taskmanager.hostname";

	/**
	 * @deprecated use {@link TaskManagerOptions#RPC_PORT} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_IPC_PORT_KEY = "taskmanager.rpc.port";

	/**
	 * @deprecated use {@link NettyShuffleEnvironmentOptions#DATA_PORT} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_DATA_PORT_KEY = "taskmanager.data.port";

	/**
	 * Config parameter to override SSL support for taskmanager's data transport.
	 *
	 * @deprecated use {@link NettyShuffleEnvironmentOptions#DATA_SSL_ENABLED} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_DATA_SSL_ENABLED = "taskmanager.data.ssl.enabled";

	/**
	 * The config parameter defining the directories for temporary files, separated by
	 * ",", "|", or the system's {@link java.io.File#pathSeparator}.
	 *
	 * @deprecated Use {@link CoreOptions#TMP_DIRS} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_TMP_DIR_KEY = "taskmanager.tmp.dirs";

	/**
	 * The config parameter defining the taskmanager log file location.
	 */
	public static final String TASK_MANAGER_LOG_PATH_KEY = "taskmanager.log.path";

	/**
	 * The config parameter defining the amount of memory to be allocated by the task manager's
	 * memory manager (in megabytes). If not set, a relative fraction will be allocated, as defined
	 * by {@link #TASK_MANAGER_MEMORY_FRACTION_KEY}.
	 *
	 * @deprecated Use {@link TaskManagerOptions#MANAGED_MEMORY_SIZE} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_MEMORY_SIZE_KEY = "taskmanager.memory.size";

	/**
	 * The config parameter defining the fraction of free memory allocated by the memory manager.
	 *
	 * @deprecated Use {@link TaskManagerOptions#MANAGED_MEMORY_FRACTION} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_MEMORY_FRACTION_KEY = "taskmanager.memory.fraction";

	/**
	 * The config parameter defining the memory allocation method (JVM heap or off-heap).
	 *
	 * @deprecated Use {@link TaskManagerOptions#MEMORY_OFF_HEAP} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_MEMORY_OFF_HEAP_KEY = "taskmanager.memory.off-heap";

	/**
	 * The config parameter for specifying whether TaskManager managed memory should be preallocated
	 * when the TaskManager is starting. (default is false)
	 *
	 * @deprecated Use {@link TaskManagerOptions#MANAGED_MEMORY_PRE_ALLOCATE} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_MEMORY_PRE_ALLOCATE_KEY = "taskmanager.memory.preallocate";

	/**
	 * The config parameter defining the number of buffers used in the network stack. This defines the
	 * number of possible tasks and shuffles.
	 *
	 * @deprecated Use {@link NettyShuffleEnvironmentOptions#NETWORK_NUM_BUFFERS} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY = "taskmanager.network.numberOfBuffers";

	/**
	 * Config parameter defining the size of memory buffers used by the network stack and the memory manager.
	 *
	 * @deprecated Use {@link TaskManagerOptions#MEMORY_SEGMENT_SIZE} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY = "taskmanager.memory.segment-size";

	/**
	 * @deprecated Not used anymore
	 */
	@Deprecated
	public static final String TASK_MANAGER_NETWORK_DEFAULT_IO_MODE = "taskmanager.network.defaultIOMode";

	/**
	 * The config parameter defining the number of task slots of a task manager.
	 *
	 * @deprecated use {@link TaskManagerOptions#NUM_TASK_SLOTS} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_NUM_TASK_SLOTS = "taskmanager.numberOfTaskSlots";

	/**
	 * Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.
	 *
	 * @deprecated use {@link TaskManagerOptions#DEBUG_MEMORY_LOG} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD = "taskmanager.debug.memory.startLogThread";

	/**
	 * The interval (in ms) for the log thread to log the current memory usage.
	 *
	 * @deprecated use {@link TaskManagerOptions#DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS = "taskmanager.debug.memory.logIntervalMs";

	/**
	 * Defines the maximum time it can take for the TaskManager registration. If the duration is
	 * exceeded without a successful registration, then the TaskManager terminates.
	 *
	 * @deprecated use {@link TaskManagerOptions#REGISTRATION_TIMEOUT} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_MAX_REGISTRATION_DURATION = "taskmanager.maxRegistrationDuration";

	/**
	 * The initial registration pause between two consecutive registration attempts. The pause
	 * is doubled for each new registration attempt until it reaches the maximum registration pause.
	 *
	 * @deprecated use {@link TaskManagerOptions#INITIAL_REGISTRATION_BACKOFF} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_INITIAL_REGISTRATION_PAUSE = "taskmanager.initial-registration-pause";

	/**
	 * The maximum registration pause between two consecutive registration attempts.
	 *
	 * @deprecated use {@link TaskManagerOptions#REGISTRATION_MAX_BACKOFF} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_MAX_REGISTARTION_PAUSE = "taskmanager.max-registration-pause";

	/**
	 * The pause after a registration has been refused by the job manager before retrying to connect.
	 *
	 * @deprecated use {@link TaskManagerOptions#REFUSED_REGISTRATION_BACKOFF} instead
	 */
	@Deprecated
	public static final String TASK_MANAGER_REFUSED_REGISTRATION_PAUSE = "taskmanager.refused-registration-pause";

	/**
	 * @deprecated Deprecated. Please use {@link TaskManagerOptions#TASK_CANCELLATION_INTERVAL}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String TASK_CANCELLATION_INTERVAL_MILLIS = "task.cancellation-interval";

	// --------------------------- Runtime Algorithms -------------------------------

	/**
	 * @deprecated use {@link AlgorithmOptions#SPILLING_MAX_FAN} instead
	 */
	@Deprecated
	public static final String DEFAULT_SPILLING_MAX_FAN_KEY = "taskmanager.runtime.max-fan";

	/**
	 * @deprecated use {@link AlgorithmOptions#SORT_SPILLING_THRESHOLD instead}
	 */
	@Deprecated
	public static final String DEFAULT_SORT_SPILLING_THRESHOLD_KEY = "taskmanager.runtime.sort-spilling-threshold";

	/**
	 * @deprecated use {@link AlgorithmOptions#HASH_JOIN_BLOOM_FILTERS} instead
	 */
	@Deprecated
	public static final String RUNTIME_HASH_JOIN_BLOOM_FILTERS_KEY = "taskmanager.runtime.hashjoin-bloom-filters";

	/**
	 * The config parameter defining the timeout for filesystem stream opening.
	 * A value of 0 indicates infinite waiting.
	 */
	public static final String FS_STREAM_OPENING_TIMEOUT_KEY = "taskmanager.runtime.fs_timeout";

	/**
	 * Whether to use the LargeRecordHandler when spilling.
	 */
	public static final String USE_LARGE_RECORD_HANDLER_KEY = "taskmanager.runtime.large-record-handler";


	// -------- Common Resource Framework Configuration (YARN & Mesos) --------

	/**
	 * Percentage of heap space to remove from containers (YARN / Mesos), to compensate
	 * for other JVM memory usage.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_HEAP_CUTOFF_RATIO} instead.
	 */
	@Deprecated
	public static final String CONTAINERIZED_HEAP_CUTOFF_RATIO = "containerized.heap-cutoff-ratio";

	/**
	 * Minimum amount of heap memory to remove in containers, as a safety margin.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_HEAP_CUTOFF_MIN} instead.
	 */
	@Deprecated
	public static final String CONTAINERIZED_HEAP_CUTOFF_MIN = "containerized.heap-cutoff-min";

	/**
	 * Prefix for passing custom environment variables to Flink's master process.
	 * For example for passing LD_LIBRARY_PATH as an env variable to the AppMaster, set:
	 * containerized.master.env.LD_LIBRARY_PATH: "/usr/lib/native"
	 * in the flink-conf.yaml.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_MASTER_ENV_PREFIX} instead.
	 */
	@Deprecated
	public static final String CONTAINERIZED_MASTER_ENV_PREFIX = "containerized.master.env.";

	/**
	 * Similar to the {@see CONTAINERIZED_MASTER_ENV_PREFIX}, this configuration prefix allows
	 * setting custom environment variables for the workers (TaskManagers).
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_TASK_MANAGER_ENV_PREFIX} instead.
	 */
	@Deprecated
	public static final String CONTAINERIZED_TASK_MANAGER_ENV_PREFIX = "containerized.taskmanager.env.";


	// ------------------------ YARN Configuration ------------------------

	/**
	 * The vcores exposed by YARN.
	 * @deprecated in favor of {@code YarnConfigOptions#VCORES}.
	 */
	@Deprecated
	public static final String YARN_VCORES = "yarn.containers.vcores";

	/**
	 * Percentage of heap space to remove from containers started by YARN.
	 * @deprecated in favor of {@code #CONTAINERIZED_HEAP_CUTOFF_RATIO}
	 */
	@Deprecated
	public static final String YARN_HEAP_CUTOFF_RATIO = "yarn.heap-cutoff-ratio";

	/**
	 * Minimum amount of memory to remove from the heap space as a safety margin.
	 * @deprecated in favor of {@code #CONTAINERIZED_HEAP_CUTOFF_MIN}
	 */
	@Deprecated
	public static final String YARN_HEAP_CUTOFF_MIN = "yarn.heap-cutoff-min";

	/**
	 * Reallocate failed YARN containers.
	 *
	 * @deprecated Not used anymore
	 */
	@Deprecated
	public static final String YARN_REALLOCATE_FAILED_CONTAINERS = "yarn.reallocate-failed";

	/**
	 * The maximum number of failed YARN containers before entirely stopping
	 * the YARN session / job on YARN.
	 *
	 * <p>By default, we take the number of initially requested containers.
	 *
	 * @deprecated in favor of {@code YarnConfigOptions#MAX_FAILED_CONTAINERS}.
	 */
	@Deprecated
	public static final String YARN_MAX_FAILED_CONTAINERS = "yarn.maximum-failed-containers";

	/**
	 * Set the number of retries for failed YARN ApplicationMasters/JobManagers in high
	 * availability mode. This value is usually limited by YARN.
	 *
	 * <p>By default, it's 1 in the standalone case and 2 in the high availability case.
	 *
	 * @deprecated in favor of {@code YarnConfigOptions#APPLICATION_ATTEMPTS}.
	 */
	@Deprecated
	public static final String YARN_APPLICATION_ATTEMPTS = "yarn.application-attempts";

	/**
	 * The heartbeat interval between the Application Master and the YARN Resource Manager.
	 * The default value is 5 (seconds).
	 * @deprecated in favor of {@code YarnConfigOptions#HEARTBEAT_DELAY_SECONDS}.
	 */
	@Deprecated
	public static final String YARN_HEARTBEAT_DELAY_SECONDS = "yarn.heartbeat-delay";

	/**
	 * When a Flink job is submitted to YARN, the JobManager's host and the number of available
	 * processing slots is written into a properties file, so that the Flink client is able
	 * to pick those details up.
	 * This configuration parameter allows changing the default location of that file (for example
	 * for environments sharing a Flink installation between users).
	 * @deprecated in favor of {@code YarnConfigOptions#PROPERTIES_FILE_LOCATION}.
	 */
	@Deprecated
	public static final String YARN_PROPERTIES_FILE_LOCATION = "yarn.properties-file.location";

	/**
	 * Prefix for passing custom environment variables to Flink's ApplicationMaster (JobManager).
	 * For example for passing LD_LIBRARY_PATH as an env variable to the AppMaster, set:
	 * 	yarn.application-master.env.LD_LIBRARY_PATH: "/usr/lib/native"
	 * in the flink-conf.yaml.
	 * @deprecated Please use {@code CONTAINERIZED_MASTER_ENV_PREFIX}.
	 */
	@Deprecated
	public static final String YARN_APPLICATION_MASTER_ENV_PREFIX = "yarn.application-master.env.";

	/** @deprecated Not used anymore, but remain here until Flink 2.0 */
	@Deprecated
	public static final String DEFAULT_YARN_APPLICATION_MASTER_PORT = "deprecated";

	/** @deprecated Not used anymore, but remain here until Flink 2.0 */
	@Deprecated
	public static final int DEFAULT_YARN_MIN_HEAP_CUTOFF = -1;

	/**
	 * Similar to the {@see YARN_APPLICATION_MASTER_ENV_PREFIX}, this configuration prefix allows
	 * setting custom environment variables.
	 * @deprecated Please use {@code CONTAINERIZED_TASK_MANAGER_ENV_PREFIX}.
	 */
	@Deprecated
	public static final String YARN_TASK_MANAGER_ENV_PREFIX = "yarn.taskmanager.env.";

	/**
	 * Template for the YARN container start invocation.
	 */
	public static final String YARN_CONTAINER_START_COMMAND_TEMPLATE =
		"yarn.container-start-command-template";

	 /**
	 * The config parameter defining the Akka actor system port for the ApplicationMaster and
	 * JobManager
	 *
	 * <p>The port can either be a port, such as "9123",
	 * a range of ports: "50100-50200"
	 * or a list of ranges and or points: "50100-50200,50300-50400,51234"
	 *
	 * <p>Setting the port to 0 will let the OS choose an available port.
	 *
	 * @deprecated in favor of {@code YarnConfigOptions#APPLICATION_MASTER_PORT}.
	 */
	@Deprecated
	public static final String YARN_APPLICATION_MASTER_PORT = "yarn.application-master.port";

	/**
	 * A comma-separated list of strings to use as YARN application tags.
	 * @deprecated in favor of {@code YarnConfigOptions#APPLICATION_TAGS}.
	 */
	@Deprecated
	public static final String YARN_APPLICATION_TAGS = "yarn.tags";


	// ------------------------ Mesos Configuration ------------------------

	/**
	 * The initial number of Mesos tasks to allocate.
	 * @deprecated in favor of {@code MesosOptions#INITIAL_TASKS}.
	 */
	@Deprecated
	public static final String MESOS_INITIAL_TASKS = "mesos.initial-tasks";

	/**
	 * The maximum number of failed Mesos tasks before entirely stopping
	 * the Mesos session / job on Mesos.
	 *
	 * <p>By default, we take the number of initially requested tasks.
	 *
	 * @deprecated in favor of {@code MesosOptions#MAX_FAILED_TASKS}.
	 */
	@Deprecated
	public static final String MESOS_MAX_FAILED_TASKS = "mesos.maximum-failed-tasks";

	/**
	 * The Mesos master URL.
	 *
	 * <p>The value should be in one of the following forms:
	 * <pre>
	 * {@code
	 *     host:port
	 *     zk://host1:port1,host2:port2,.../path
	 *     zk://username:password@host1:port1,host2:port2,.../path
	 *     file:///path/to/file (where file contains one of the above)
	 * }
	 * </pre>
	 * @deprecated in favor of {@code MesosOptions#MASTER_URL}.
	 */
	@Deprecated
	public static final String MESOS_MASTER_URL = "mesos.master";

	/**
	 * The failover timeout for the Mesos scheduler, after which running tasks are automatically shut down.
	 *
	 * <p>The default value is 600 (seconds).
	 *
	 * @deprecated in favor of {@code MesosOptions#FAILOVER_TIMEOUT_SECONDS}.
	 */
	@Deprecated
	public static final String MESOS_FAILOVER_TIMEOUT_SECONDS = "mesos.failover-timeout";

	/**
	 * The config parameter defining the Mesos artifact server port to use.
	 * Setting the port to 0 will let the OS choose an available port.
	 * @deprecated in favor of {@code MesosOptions#ARTIFACT_SERVER_PORT_KEY}.
	 */
	@Deprecated
	public static final String MESOS_ARTIFACT_SERVER_PORT_KEY = "mesos.resourcemanager.artifactserver.port";

	/** @deprecated in favor of {@code MesosOptions#RESOURCEMANAGER_FRAMEWORK_NAME}. */
	@Deprecated
	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_NAME = "mesos.resourcemanager.framework.name";

	/** @deprecated in favor of {@code MesosOptions#RESOURCEMANAGER_FRAMEWORK_ROLE}. */
	@Deprecated
	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_ROLE = "mesos.resourcemanager.framework.role";

	/** @deprecated in favor of {@code MesosOptions#RESOURCEMANAGER_FRAMEWORK_PRINCIPAL}. */
	@Deprecated
	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_PRINCIPAL = "mesos.resourcemanager.framework.principal";

	/** @deprecated in favor of {@code MesosOptions#RESOURCEMANAGER_FRAMEWORK_SECRET}. */
	@Deprecated
	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_SECRET = "mesos.resourcemanager.framework.secret";

	/** @deprecated in favor of {@code MesosOptions#RESOURCEMANAGER_FRAMEWORK_USER}. */
	@Deprecated
	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_USER = "mesos.resourcemanager.framework.user";

	/**
	 * Config parameter to override SSL support for the Artifact Server.
	 * @deprecated in favor of {@code MesosOptions#ARTIFACT_SERVER_SSL_ENABLED}.
	 */
	@Deprecated
	public static final String MESOS_ARTIFACT_SERVER_SSL_ENABLED = "mesos.resourcemanager.artifactserver.ssl.enabled";

	// ------------------------ Hadoop Configuration ------------------------

	/**
	 * Path to hdfs-default.xml file.
	 *
	 * @deprecated Use environment variable HADOOP_CONF_DIR instead.
	 */
	@Deprecated
	public static final String HDFS_DEFAULT_CONFIG = "fs.hdfs.hdfsdefault";

	/**
	 * Path to hdfs-site.xml file.
	 *
	 * @deprecated Use environment variable HADOOP_CONF_DIR instead.
	 */
	@Deprecated
	public static final String HDFS_SITE_CONFIG = "fs.hdfs.hdfssite";

	/**
	 * Path to Hadoop configuration.
	 *
	 * @deprecated Use environment variable HADOOP_CONF_DIR instead.
	 */
	@Deprecated
	public static final String PATH_HADOOP_CONFIG = "fs.hdfs.hadoopconf";

	// ------------------------ File System Behavior ------------------------

	/**
	 * Key to specify the default filesystem to be used by a job. In the case of
	 * <code>file:///</code>, which is the default (see {@link ConfigConstants#DEFAULT_FILESYSTEM_SCHEME}),
	 * the local filesystem is going to be used to resolve URIs without an explicit scheme.
	 *
	 * @deprecated Use {@link CoreOptions#DEFAULT_FILESYSTEM_SCHEME} instead.
	 */
	@Deprecated
	public static final String FILESYSTEM_SCHEME = "fs.default-scheme";

	/**
	 * Key to specify whether the file systems should simply overwrite existing files.
	 *
	 * @deprecated Use {@link CoreOptions#FILESYTEM_DEFAULT_OVERRIDE} instead.
	 */
	@Deprecated
	public static final String FILESYSTEM_DEFAULT_OVERWRITE_KEY = "fs.overwrite-files";

	/**
	 * Key to specify whether the file systems should always create a directory for the output, even with a parallelism of one.
	 *
	 * @deprecated Use {@link CoreOptions#FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY} instead.
	 */
	@Deprecated
	public static final String FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY = "fs.output.always-create-directory";

	// ---------------------------- Compiler -------------------------------

	/**
	 * The maximum number of line samples to be taken by the delimited input format, if no
	 * other value is specified for the data source.
	 *
	 * @deprecated use {@link OptimizerOptions#DELIMITED_FORMAT_MAX_LINE_SAMPLES} instead
	 */
	@Deprecated
	public static final String DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY = "compiler.delimited-informat.max-line-samples";

	/**
	 * The minimum number of line samples to be taken by the delimited input format, if no
	 * other value is specified for the data source.
	 *
	 * @deprecated use {@link OptimizerOptions#DELIMITED_FORMAT_MIN_LINE_SAMPLES} instead
	 */
	@Deprecated
	public static final String DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY = "compiler.delimited-informat.min-line-samples";

	/**
	 * The maximum length of a single sampled record before the sampling is aborted.
	 *
	 * @deprecated use {@link OptimizerOptions#DELIMITED_FORMAT_MAX_SAMPLE_LEN} instead
	 */
	@Deprecated
	public static final String DELIMITED_FORMAT_MAX_SAMPLE_LENGTH_KEY = "compiler.delimited-informat.max-sample-len";


	// ------------------------- JobManager Web Frontend ----------------------

	/**
	 * The port for the runtime monitor web-frontend server.
	 *
	 * @deprecated Use {@link WebOptions#PORT} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_PORT_KEY = "jobmanager.web.port";

	/**
	 * Config parameter to override SSL support for the JobManager Web UI.
	 *
	 * @deprecated Use {@link WebOptions#SSL_ENABLED} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_SSL_ENABLED = "jobmanager.web.ssl.enabled";

	/**
	 * The config parameter defining the flink web directory to be used by the webmonitor.
	 *
	 * @deprecated Use {@link WebOptions#TMP_DIR} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_TMPDIR_KEY = "jobmanager.web.tmpdir";

	/**
	 * The config parameter defining the directory for uploading the job jars. If not specified a dynamic directory
	 * will be used under the directory specified by JOB_MANAGER_WEB_TMPDIR_KEY.
	 *
	 * @deprecated Use {@link WebOptions#UPLOAD_DIR} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_UPLOAD_DIR_KEY = "jobmanager.web.upload.dir";

	/**
	 * The config parameter defining the number of archived jobs for the jobmanager.
	 *
	 * @deprecated Use {@link WebOptions#ARCHIVE_COUNT} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_ARCHIVE_COUNT = "jobmanager.web.history";

	/**
	 * The log file location (may be in /log for standalone but under log directory when using YARN).
	 *
	 * @deprecated Use {@link WebOptions#LOG_PATH} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_LOG_PATH_KEY = "jobmanager.web.log.path";

	/**
	 * Config parameter indicating whether jobs can be uploaded and run from the web-frontend.
	 *
	 * @deprecated Use {@link WebOptions#SUBMIT_ENABLE} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_SUBMIT_ENABLED_KEY = "jobmanager.web.submit.enable";

	/**
	 * Flag to disable checkpoint stats.
	 *
	 * @deprecated Not possible to disable any longer. Use history size of 0.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_CHECKPOINTS_DISABLE = "jobmanager.web.checkpoints.disable";

	/**
	 * Config parameter defining the number of checkpoints to remember for recent history.
	 *
	 * @deprecated Use {@link WebOptions#CHECKPOINTS_HISTORY_SIZE} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE = "jobmanager.web.checkpoints.history";

	/**
	 * Time after which cached stats are cleaned up if not accessed.
	 *
	 * @deprecated Use {@link WebOptions#BACKPRESSURE_CLEANUP_INTERVAL} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_BACK_PRESSURE_CLEAN_UP_INTERVAL = "jobmanager.web.backpressure.cleanup-interval";

	/**
	 * Time after which available stats are deprecated and need to be refreshed (by resampling).
	 *
	 * @deprecated Use {@link WebOptions#BACKPRESSURE_REFRESH_INTERVAL} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_BACK_PRESSURE_REFRESH_INTERVAL = "jobmanager.web.backpressure.refresh-interval";

	/**
	 * Number of stack trace samples to take to determine back pressure.
	 *
	 * @deprecated Use {@link WebOptions#BACKPRESSURE_NUM_SAMPLES} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_BACK_PRESSURE_NUM_SAMPLES = "jobmanager.web.backpressure.num-samples";

	/**
	 * Delay between stack trace samples to determine back pressure.
	 *
	 * @deprecated Use {@link WebOptions#BACKPRESSURE_DELAY} instead.
	 */
	@Deprecated
	public static final String JOB_MANAGER_WEB_BACK_PRESSURE_DELAY = "jobmanager.web.backpressure.delay-between-samples";

	// ------------------------------ AKKA ------------------------------------

	/**
	 * Timeout for the startup of the actor system.
	 *
	 * @deprecated Use {@link AkkaOptions#STARTUP_TIMEOUT} instead.
	 */
	@Deprecated
	public static final String AKKA_STARTUP_TIMEOUT = "akka.startup-timeout";

	/**
	 * Heartbeat interval of the transport failure detector.
	 *
	 * @deprecated Use {@link AkkaOptions#TRANSPORT_HEARTBEAT_INTERVAL} instead.
	 */
	@Deprecated
	public static final String AKKA_TRANSPORT_HEARTBEAT_INTERVAL = "akka.transport.heartbeat.interval";

	/**
	 * Allowed heartbeat pause for the transport failure detector.
	 *
	 * @deprecated Use {@link AkkaOptions#TRANSPORT_HEARTBEAT_PAUSE} instead.
	 */
	@Deprecated
	public static final String AKKA_TRANSPORT_HEARTBEAT_PAUSE = "akka.transport.heartbeat.pause";

	/**
	 * Detection threshold of transport failure detector.
	 *
	 * @deprecated Use {@link AkkaOptions#TRANSPORT_THRESHOLD} instead.
	 */
	@Deprecated
	public static final String AKKA_TRANSPORT_THRESHOLD = "akka.transport.threshold";

	/**
	 * Heartbeat interval of watch failure detector.
	 *
	 * @deprecated Use {@link AkkaOptions#WATCH_HEARTBEAT_INTERVAL} instead.
	 */
	@Deprecated
	public static final String AKKA_WATCH_HEARTBEAT_INTERVAL = "akka.watch.heartbeat.interval";

	/**
	 * Allowed heartbeat pause for the watch failure detector.
	 *
	 * @deprecated Use {@link AkkaOptions#WATCH_HEARTBEAT_PAUSE} instead.
	 */
	@Deprecated
	public static final String AKKA_WATCH_HEARTBEAT_PAUSE = "akka.watch.heartbeat.pause";

	/**
	 * Detection threshold for the phi accrual watch failure detector.
	 *
	 * @deprecated Use {@link AkkaOptions#WATCH_THRESHOLD} instead.
	 */
	@Deprecated
	public static final String AKKA_WATCH_THRESHOLD = "akka.watch.threshold";

	/**
	 * Akka TCP timeout.
	 *
	 * @deprecated Use {@link AkkaOptions#TCP_TIMEOUT} instead.
	 */
	@Deprecated
	public static final String AKKA_TCP_TIMEOUT = "akka.tcp.timeout";

	/**
	 * Override SSL support for the Akka transport.
	 *
	 * @deprecated Use {@link AkkaOptions#SSL_ENABLED} instead.
	 */
	@Deprecated
	public static final String AKKA_SSL_ENABLED = "akka.ssl.enabled";

	/**
	 * Maximum framesize of akka messages.
	 *
	 * @deprecated Use {@link AkkaOptions#FRAMESIZE} instead.
	 */
	@Deprecated
	public static final String AKKA_FRAMESIZE = "akka.framesize";

	/**
	 * Maximum number of messages until another actor is executed by the same thread.
	 *
	 * @deprecated Use {@link AkkaOptions#DISPATCHER_THROUGHPUT} instead.
	 */
	@Deprecated
	public static final String AKKA_DISPATCHER_THROUGHPUT = "akka.throughput";

	/**
	 * Log lifecycle events.
	 *
	 * @deprecated Use {@link AkkaOptions#LOG_LIFECYCLE_EVENTS} instead.
	 */
	@Deprecated
	public static final String AKKA_LOG_LIFECYCLE_EVENTS = "akka.log.lifecycle.events";

	/**
	 * Timeout for all blocking calls on the cluster side.
	 *
	 * @deprecated Use {@link AkkaOptions#ASK_TIMEOUT} instead.
	 */
	@Deprecated
	public static final String AKKA_ASK_TIMEOUT = "akka.ask.timeout";

	/**
	 * Timeout for all blocking calls that look up remote actors.
	 *
	 * @deprecated Use {@link AkkaOptions#LOOKUP_TIMEOUT} instead.
	 */
	@Deprecated
	public static final String AKKA_LOOKUP_TIMEOUT = "akka.lookup.timeout";

	/**
	 * Timeout for all blocking calls on the client side.
	 *
	 * @deprecated Use {@link AkkaOptions#CLIENT_TIMEOUT} instead.
	 */
	@Deprecated
	public static final String AKKA_CLIENT_TIMEOUT = "akka.client.timeout";

	/**
	 * Exit JVM on fatal Akka errors.
	 *
	 * @deprecated Use {@link AkkaOptions#JVM_EXIT_ON_FATAL_ERROR} instead.
	 */
	@Deprecated
	public static final String AKKA_JVM_EXIT_ON_FATAL_ERROR = "akka.jvm-exit-on-fatal-error";

	// ----------------------------- Transport SSL Settings--------------------

	/**
	 * @deprecated use {@link SecurityOptions#SSL_ENABLED} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_ENABLED = "security.ssl.enabled";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_KEYSTORE} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_KEYSTORE = "security.ssl.keystore";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_KEYSTORE_PASSWORD} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_KEYSTORE_PASSWORD = "security.ssl.keystore-password";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_KEY_PASSWORD} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_KEY_PASSWORD = "security.ssl.key-password";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_TRUSTSTORE} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_TRUSTSTORE = "security.ssl.truststore";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_TRUSTSTORE_PASSWORD} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_TRUSTSTORE_PASSWORD = "security.ssl.truststore-password";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_PROTOCOL} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_PROTOCOL = "security.ssl.protocol";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_ALGORITHMS} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_ALGORITHMS = "security.ssl.algorithms";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_VERIFY_HOSTNAME} instead
	 */
	@Deprecated
	public static final String SECURITY_SSL_VERIFY_HOSTNAME = "security.ssl.verify-hostname";

	// ----------------------------- Streaming --------------------------------

	/**
	 * State backend for checkpoints.
	 *
	 * @deprecated Use {@link CheckpointingOptions#STATE_BACKEND} instead.
	 */
	@Deprecated
	public static final String STATE_BACKEND = "state.backend";

	// ----------------------------- Miscellaneous ----------------------------

	/**
	 * The key to the Flink base directory path. Was initially used for configurations of the
	 * web UI, but outdated now.
	 *
	 * @deprecated This parameter should not be used any more. A running Flink cluster should
	 *             make no assumption about its location.
	 */
	@Deprecated
	public static final String FLINK_BASE_DIR_PATH_KEY = "flink.base.dir.path";

	/** @deprecated Use {@link CoreOptions#FLINK_JVM_OPTIONS} instead. */
	@Deprecated
	public static final String FLINK_JVM_OPTIONS = "env.java.opts";

	// --------------------------- High Availability --------------------------

	/** @deprecated Deprecated in favour of {@link HighAvailabilityOptions#HA_MODE} */
	@PublicEvolving
	@Deprecated
	public static final String HA_MODE = "high-availability";

	/** Ports used by the job manager if not in 'none' recovery mode. */
	@PublicEvolving
	public static final String HA_JOB_MANAGER_PORT = "high-availability.jobmanager.port";

	/** @deprecated Deprecated in favour of {@link HighAvailabilityOptions#HA_JOB_DELAY}. */
	@PublicEvolving
	@Deprecated
	public static final String HA_JOB_DELAY = "high-availability.job.delay";

	/** @deprecated Deprecated in favour of {@link #HA_MODE}. */
	@Deprecated
	public static final String RECOVERY_MODE = "recovery.mode";

	/** @deprecated Deprecated in favour of {@link #HA_JOB_MANAGER_PORT}. */
	@Deprecated
	public static final String RECOVERY_JOB_MANAGER_PORT = "recovery.jobmanager.port";

	/** @deprecated Deprecated in favour of {@link #HA_JOB_DELAY}. */
	@Deprecated
	public static final String RECOVERY_JOB_DELAY = "recovery.job.delay";

	// --------------------------- ZooKeeper ----------------------------------

	/**
	 * ZooKeeper servers.
	 * @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_QUORUM}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_QUORUM_KEY = "high-availability.zookeeper.quorum";

	/**
	 * File system state backend base path for recoverable state handles. Recovery state is written
	 * to this path and the file state handles are persisted for recovery.
	 * @deprecated in favor of {@link HighAvailabilityOptions#HA_STORAGE_PATH}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_STORAGE_PATH = "high-availability.zookeeper.storageDir";

	/**
	 * ZooKeeper root path.
	 * @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_ROOT}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_DIR_KEY = "high-availability.zookeeper.path.root";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_CLUSTER_ID}. */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_NAMESPACE_KEY = "high-availability.zookeeper.path.namespace";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_LATCH_PATH}. */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_LATCH_PATH = "high-availability.zookeeper.path.latch";

	/**
	 * ZooKeeper root path (ZNode) for job graphs.
	 * @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_JOBGRAPHS_PATH}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_JOBGRAPHS_PATH = "high-availability.zookeeper.path.jobgraphs";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_LEADER_PATH}. */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_LEADER_PATH = "high-availability.zookeeper.path.leader";

	/**
	 * ZooKeeper root path (ZNode) for completed checkpoints.
	 * @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_CHECKPOINTS_PATH}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_CHECKPOINTS_PATH = "high-availability.zookeeper.path.checkpoints";

	/**
	 * ZooKeeper root path (ZNode) for checkpoint counters.
	 * @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH = "high-availability.zookeeper.path.checkpoint-counter";

	/**
	 * ZooKeeper root path (ZNode) for Mesos workers.
	 * @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_MESOS_WORKERS_PATH}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_MESOS_WORKERS_PATH = "high-availability.zookeeper.path.mesos-workers";

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_SESSION_TIMEOUT}. */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_SESSION_TIMEOUT = "high-availability.zookeeper.client.session-timeout";

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_CONNECTION_TIMEOUT}. */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_CONNECTION_TIMEOUT = "high-availability.zookeeper.client.connection-timeout";

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_RETRY_WAIT} */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_RETRY_WAIT = "high-availability.zookeeper.client.retry-wait";

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_MAX_RETRY_ATTEMPTS}. */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_MAX_RETRY_ATTEMPTS = "high-availability.zookeeper.client.max-retry-attempts";

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_CLIENT_ACL}. */
	@PublicEvolving
	@Deprecated
	public static final String HA_ZOOKEEPER_CLIENT_ACL = "high-availability.zookeeper.client.acl";

	/** @deprecated in favor of {@link SecurityOptions#ZOOKEEPER_SASL_DISABLE}. */
	@PublicEvolving
	@Deprecated
	public static final String ZOOKEEPER_SASL_DISABLE = "zookeeper.sasl.disable";

	/** @deprecated in favor of {@link SecurityOptions#ZOOKEEPER_SASL_SERVICE_NAME}. */
	@PublicEvolving
	@Deprecated
	public static final String ZOOKEEPER_SASL_SERVICE_NAME = "zookeeper.sasl.service-name";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_QUORUM_KEY}. */
	@Deprecated
	public static final String ZOOKEEPER_QUORUM_KEY = "recovery.zookeeper.quorum";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_STORAGE_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_RECOVERY_PATH = "recovery.zookeeper.storageDir";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_DIR_KEY}. */
	@Deprecated
	public static final String ZOOKEEPER_DIR_KEY = "recovery.zookeeper.path.root";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_NAMESPACE_KEY}. */
	@Deprecated
	public static final String ZOOKEEPER_NAMESPACE_KEY = "recovery.zookeeper.path.namespace";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_LATCH_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_LATCH_PATH = "recovery.zookeeper.path.latch";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_LEADER_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_LEADER_PATH = "recovery.zookeeper.path.leader";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_JOBGRAPHS_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_JOBGRAPHS_PATH = "recovery.zookeeper.path.jobgraphs";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_CHECKPOINTS_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_CHECKPOINTS_PATH = "recovery.zookeeper.path.checkpoints";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_CHECKPOINT_COUNTER_PATH = "recovery.zookeeper.path.checkpoint-counter";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_MESOS_WORKERS_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_MESOS_WORKERS_PATH = "recovery.zookeeper.path.mesos-workers";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_SESSION_TIMEOUT}. */
	@Deprecated
	public static final String ZOOKEEPER_SESSION_TIMEOUT = "recovery.zookeeper.client.session-timeout";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_CONNECTION_TIMEOUT}. */
	@Deprecated
	public static final String ZOOKEEPER_CONNECTION_TIMEOUT = "recovery.zookeeper.client.connection-timeout";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_RETRY_WAIT}. */
	@Deprecated
	public static final String ZOOKEEPER_RETRY_WAIT = "recovery.zookeeper.client.retry-wait";

	/** @deprecated Deprecated in favour of {@link #HA_ZOOKEEPER_MAX_RETRY_ATTEMPTS}. */
	@Deprecated
	public static final String ZOOKEEPER_MAX_RETRY_ATTEMPTS = "recovery.zookeeper.client.max-retry-attempts";

	// ---------------------------- Metrics -----------------------------------

	/** @deprecated Use {@link MetricOptions#REPORTERS_LIST} instead. */
	@Deprecated
	public static final String METRICS_REPORTERS_LIST = "metrics.reporters";

	/**
	 * The prefix for per-reporter configs. Has to be combined with a reporter name and
	 * the configs mentioned below.
	 */
	public static final String METRICS_REPORTER_PREFIX = "metrics.reporter.";

	/** The class of the reporter to use. This is used as a suffix in an actual reporter config */
	public static final String METRICS_REPORTER_CLASS_SUFFIX = "class";

	/** The class of the reporter factory to use. This is used as a suffix in an actual reporter config */
	public static final String METRICS_REPORTER_FACTORY_CLASS_SUFFIX = "factory.class";

	/** The interval between reports. This is used as a suffix in an actual reporter config */
	public static final String METRICS_REPORTER_INTERVAL_SUFFIX = "interval";

	/**	The delimiter used to assemble the metric identifier. This is used as a suffix in an actual reporter config. */
	public static final String METRICS_REPORTER_SCOPE_DELIMITER = "scope.delimiter";

	/** @deprecated Use {@link MetricOptions#SCOPE_DELIMITER} instead. */
	@Deprecated
	public static final String METRICS_SCOPE_DELIMITER = "metrics.scope.delimiter";

	/** @deprecated Use {@link MetricOptions#SCOPE_NAMING_JM} instead. */
	@Deprecated
	public static final String METRICS_SCOPE_NAMING_JM = "metrics.scope.jm";

	/** @deprecated Use {@link MetricOptions#SCOPE_NAMING_TM} instead. */
	@Deprecated
	public static final String METRICS_SCOPE_NAMING_TM = "metrics.scope.tm";

	/** @deprecated Use {@link MetricOptions#SCOPE_NAMING_JM_JOB} instead. */
	@Deprecated
	public static final String METRICS_SCOPE_NAMING_JM_JOB = "metrics.scope.jm.job";

	/** @deprecated Use {@link MetricOptions#SCOPE_NAMING_TM_JOB} instead. */
	@Deprecated
	public static final String METRICS_SCOPE_NAMING_TM_JOB = "metrics.scope.tm.job";

	/** @deprecated Use {@link MetricOptions#SCOPE_NAMING_TASK} instead. */
	@Deprecated
	public static final String METRICS_SCOPE_NAMING_TASK = "metrics.scope.task";

	/** @deprecated Use {@link MetricOptions#SCOPE_NAMING_OPERATOR} instead. */
	@Deprecated
	public static final String METRICS_SCOPE_NAMING_OPERATOR = "metrics.scope.operator";

	/** @deprecated Use {@link MetricOptions#LATENCY_HISTORY_SIZE} instead. */
	@Deprecated
	public static final String METRICS_LATENCY_HISTORY_SIZE = "metrics.latency.history-size";


	// ---------------------------- Checkpoints -------------------------------

	/**
	 * The default directory for savepoints.
	 * @deprecated Use {@link CheckpointingOptions#SAVEPOINT_DIRECTORY} instead.
	 */
	@PublicEvolving
	@Deprecated
	public static final String SAVEPOINT_DIRECTORY_KEY = "state.savepoints.dir";

	/**
	 * The default directory used for persistent checkpoints.
	 * @deprecated Use {@link CheckpointingOptions#CHECKPOINTS_DIRECTORY} instead.
	 */
	@PublicEvolving
	@Deprecated
	public static final String CHECKPOINTS_DIRECTORY_KEY = "state.checkpoints.dir";

	/**
	 * @deprecated This key was used in Flink versions <= 1.1.X with the savepoint backend
	 * configuration. We now always use the FileSystem for savepoints. For this,
	 * the only relevant config key is {@link CheckpointingOptions#SAVEPOINT_DIRECTORY}.
	 */
	@Deprecated
	public static final String SAVEPOINT_FS_DIRECTORY_KEY = "savepoints.state.backend.fs.dir";

	// ------------------------------------------------------------------------
	//                            Default Values
	// ------------------------------------------------------------------------

	// ---------------------------- Parallelism -------------------------------

	/**
	 * The default parallelism for operations.
	 *
	 * @deprecated use {@link CoreOptions#DEFAULT_PARALLELISM} instead
	 */
	@Deprecated
	public static final int DEFAULT_PARALLELISM = 1;

	/**
	 * The default number of execution retries.
	 */
	public static final int DEFAULT_EXECUTION_RETRIES = 0;

	// ------------------------------ Runtime ---------------------------------

	/**
	 * The default library cache manager cleanup interval in seconds.
	 *
	 * @deprecated use {@link BlobServerOptions#CLEANUP_INTERVAL} instead
	 */
	@Deprecated
	public static final long DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL =
		BlobServerOptions.CLEANUP_INTERVAL.defaultValue();

	/**
	 * @deprecated use {@link JobManagerOptions#PORT} instead
	 */
	@Deprecated
	public static final int DEFAULT_JOB_MANAGER_IPC_PORT = 6123;

	/**
	 * The default network port of the resource manager.
	 * @deprecated Use {@link ResourceManagerOptions#IPC_PORT} instead.
	 */
	@Deprecated
	public static final int DEFAULT_RESOURCE_MANAGER_IPC_PORT = 0;

	/**
	 * @deprecated use {@link BlobServerOptions#SSL_ENABLED} instead
	 */
	@Deprecated
	public static final boolean DEFAULT_BLOB_SERVICE_SSL_ENABLED = true;

	/**
	 * @deprecated use {@link BlobServerOptions#FETCH_RETRIES} instead
	 */
	@Deprecated
	public static final int DEFAULT_BLOB_FETCH_RETRIES = 5;

	/**
	 * @deprecated use {@link BlobServerOptions#FETCH_CONCURRENT} instead
	 */
	@Deprecated
	public static final int DEFAULT_BLOB_FETCH_CONCURRENT = 50;

	/**
	 * @deprecated use {@link BlobServerOptions#FETCH_BACKLOG} instead
	 */
	@Deprecated
	public static final int DEFAULT_BLOB_FETCH_BACKLOG = 1000;

	/**
	 * @deprecated use {@link BlobServerOptions#PORT} instead
	 */
	@Deprecated
	public static final String DEFAULT_BLOB_SERVER_PORT = "0";

	/**
	 * @deprecated use {@link TaskManagerOptions#RPC_PORT} instead
	 */
	@Deprecated
	public static final int DEFAULT_TASK_MANAGER_IPC_PORT = 0;

	/**
	 * The default network port the task manager expects to receive transfer envelopes on. The {@code 0} means that
	 * the TaskManager searches for a free port.
	 *
	 * @deprecated use {@link NettyShuffleEnvironmentOptions#DATA_PORT} instead
	 */
	@Deprecated
	public static final int DEFAULT_TASK_MANAGER_DATA_PORT = 0;

	/**
	 * The default value to override ssl support for task manager's data transport.
	 *
	 * @deprecated use {@link NettyShuffleEnvironmentOptions#DATA_SSL_ENABLED} instead
	 */
	@Deprecated
	public static final boolean DEFAULT_TASK_MANAGER_DATA_SSL_ENABLED = true;

	/**
	 * The default directory for temporary files of the task manager.
	 *
	 * @deprecated {@link CoreOptions#TMP_DIRS} provides the default value now
	 */
	@Deprecated
	public static final String DEFAULT_TASK_MANAGER_TMP_PATH = System.getProperty("java.io.tmpdir");

	/**
	 * Config key has been deprecated. Therefore, no default value required.
	 *
	 * @deprecated {@link TaskManagerOptions#MANAGED_MEMORY_FRACTION} provides the default value now
	 */
	@Deprecated
	public static final float DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION = 0.7f;

	/**
	 * Config key has been deprecated. Therefore, no default value required.
	 *
	 * @deprecated {@link NettyShuffleEnvironmentOptions#NETWORK_NUM_BUFFERS} provides the default value now
	 */
	@Deprecated
	public static final int DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS = 2048;

	/**
	 * Config key has been deprecated. Therefore, no default value required.
	 *
	 * @deprecated {@link TaskManagerOptions#MEMORY_SEGMENT_SIZE} provides the default value now
	 */
	@Deprecated
	public static final int DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE = 32768;

	/**
	 * @deprecated Not used anymore
	 */
	@Deprecated
	public static final String DEFAULT_TASK_MANAGER_NETWORK_DEFAULT_IO_MODE = "sync";

	/**
	 * Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.
	 *
	 * @deprecated use {@link TaskManagerOptions#DEBUG_MEMORY_LOG} instead
	 */
	@Deprecated
	public static final boolean DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD = false;

	/**
	 * The interval (in ms) for the log thread to log the current memory usage.
	 *
	 * @deprecated use {@link TaskManagerOptions#DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS} instead
	 */
	@Deprecated
	public static final long DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS = 5000L;

	/**
	 * The default task manager's maximum registration duration.
	 *
	 * @deprecated use {@link TaskManagerOptions#REGISTRATION_TIMEOUT} instead
	 */
	@Deprecated
	public static final String DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION = "Inf";

	/**
	 * The default task manager's initial registration pause.
	 *
	 * @deprecated use {@link TaskManagerOptions#INITIAL_REGISTRATION_BACKOFF} instead
	 */
	@Deprecated
	public static final String DEFAULT_TASK_MANAGER_INITIAL_REGISTRATION_PAUSE = "500 ms";

	/**
	 * The default task manager's maximum registration pause.
	 *
	 * @deprecated use {@link TaskManagerOptions#REGISTRATION_MAX_BACKOFF} instead
	 */
	@Deprecated
	public static final String DEFAULT_TASK_MANAGER_MAX_REGISTRATION_PAUSE = "30 s";

	/**
	 * The default task manager's refused registration pause.
	 *
	 * @deprecated use {@link TaskManagerOptions#REFUSED_REGISTRATION_BACKOFF} instead
	 */
	@Deprecated
	public static final String DEFAULT_TASK_MANAGER_REFUSED_REGISTRATION_PAUSE = "10 s";

	/**
	 * Config key has been deprecated. Therefore, no default value required.
	 *
	 * @deprecated {@link TaskManagerOptions#MANAGED_MEMORY_PRE_ALLOCATE} provides the default value now
	 */
	@Deprecated
	public static final boolean DEFAULT_TASK_MANAGER_MEMORY_PRE_ALLOCATE = false;

	/** @deprecated Please use {@link TaskManagerOptions#TASK_CANCELLATION_INTERVAL}. */
	@Deprecated
	public static final long DEFAULT_TASK_CANCELLATION_INTERVAL_MILLIS = 30000;

	// ------------------------ Runtime Algorithms ------------------------

	/**
	 * @deprecated use {@link AlgorithmOptions#HASH_JOIN_BLOOM_FILTERS} instead
	 */
	@Deprecated
	public static final boolean DEFAULT_RUNTIME_HASH_JOIN_BLOOM_FILTERS = false;

	/**
	 * @deprecated use {@link AlgorithmOptions#SPILLING_MAX_FAN} insstead
	 */
	@Deprecated
	public static final int DEFAULT_SPILLING_MAX_FAN = 128;

	/**
	 * @deprecated use {@link AlgorithmOptions#SORT_SPILLING_THRESHOLD} instead
	 */
	@Deprecated
	public static final float DEFAULT_SORT_SPILLING_THRESHOLD = 0.8f;

	/**
	 * The default timeout for filesystem stream opening: infinite (means max long milliseconds).
	 */
	public static final int DEFAULT_FS_STREAM_OPENING_TIMEOUT = 0;

	/**
	 * Whether to use the LargeRecordHandler when spilling.
	 */
	public static final boolean DEFAULT_USE_LARGE_RECORD_HANDLER = false;


	// ------ Common Resource Framework Configuration (YARN & Mesos) ------

	/**
	 * Minimum amount of memory to subtract from the process memory to get the TaskManager
	 * heap size. We came up with these values experimentally.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_HEAP_CUTOFF_MIN} instead.
	 */
	@Deprecated
	public static final int DEFAULT_YARN_HEAP_CUTOFF = 600;

	/**
	 * Relative amount of memory to subtract from Java process memory to get the TaskManager
	 * heap size.
	 * @deprecated Use {@link ResourceManagerOptions#CONTAINERIZED_HEAP_CUTOFF_RATIO} instead.
	 */
	@Deprecated
	public static final float DEFAULT_YARN_HEAP_CUTOFF_RATIO = 0.25f;

	/**
	 * Start command template for Flink on YARN containers.
	 */
	public static final String DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE =
		"%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%";

	/**
	 * Default port for the application master is 0, which means
	 * the operating system assigns an ephemeral port.
	 * @deprecated in favor of {@code YarnConfigOptions#APPLICATION_MASTER_PORT}.
	 */
	@Deprecated
	public static final String DEFAULT_YARN_JOB_MANAGER_PORT = "0";

	// ------ Mesos-Specific Configuration ------
	// For more configuration entries please see {@code MesosTaskManagerParameters}.

	/**
	 * The default failover timeout provided to Mesos (10 mins).
	 * @deprecated in favor of {@code MesosOptions#FAILOVER_TIMEOUT_SECONDS}.
	 */
	@Deprecated
	public static final int DEFAULT_MESOS_FAILOVER_TIMEOUT_SECS = 10 * 60;

	/**
	 * The default network port to listen on for the Mesos artifact server.
	 * @deprecated in favor of {@code MesosOptions#ARTIFACT_SERVER_PORT_KEY}.
	 */
	@Deprecated
	public static final int DEFAULT_MESOS_ARTIFACT_SERVER_PORT = 0;

	/**
	 * The default Mesos framework name for the ResourceManager to use.
	 * @deprecated in favor of {@code MesosOptions#RESOURCEMANAGER_FRAMEWORK_NAME}.
	 */
	@Deprecated
	public static final String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_NAME = "Flink";

	/** @deprecated in favor of {@code MesosOptions#RESOURCEMANAGER_FRAMEWORK_ROLE}. */
	@Deprecated
	public static final String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_ROLE = "*";

	/** @deprecated in favor of {@code MesosOptions#RESOURCEMANAGER_FRAMEWORK_USER}. */
	@Deprecated
	public static final String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_USER = "";

	/**
	 * Default value to override SSL support for the Artifact Server.
	 * @deprecated in favor of {@code MesosOptions#ARTIFACT_SERVER_SSL_ENABLED}.
	 */
	@Deprecated
	public static final boolean DEFAULT_MESOS_ARTIFACT_SERVER_SSL_ENABLED = true;

	// ------------------------ File System Behavior ------------------------

	/**
	 * The default filesystem to be used, if no other scheme is specified in the
	 * user-provided URI (= local filesystem).
	 */
	public static final String DEFAULT_FILESYSTEM_SCHEME = "file:///";

	/**
	 * The default behavior with respect to overwriting existing files (= not overwrite).
	 */
	public static final boolean DEFAULT_FILESYSTEM_OVERWRITE = false;

	/**
	 * The default behavior for output directory creating (create only directory when parallelism &gt; 1).
	 *
	 * @deprecated Use {@link CoreOptions#FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY} instead.
	 */
	@Deprecated
	public static final boolean DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY = false;


	// ---------------------------- Compiler -------------------------------

	/**
	 * The default maximum number of line samples taken by the delimited input format.
	 *
	 * @deprecated use {@link OptimizerOptions#DELIMITED_FORMAT_MAX_LINE_SAMPLES} instead
	 */
	@Deprecated
	public static final int DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES = 10;

	/**
	 * The default minimum number of line samples taken by the delimited input format.
	 *
	 * @deprecated use {@link OptimizerOptions#DELIMITED_FORMAT_MIN_LINE_SAMPLES} instead
	 */
	@Deprecated
	public static final int DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES = 2;

	/**
	 * The default maximum sample length before sampling is aborted (2 MiBytes).
	 *
	 * @deprecated use {@link OptimizerOptions#DELIMITED_FORMAT_MAX_SAMPLE_LEN} instead
	 */
	@Deprecated
	public static final int DEFAULT_DELIMITED_FORMAT_MAX_SAMPLE_LEN = 2 * 1024 * 1024;


	// ------------------------- JobManager Web Frontend ----------------------

	/**
	 * The config key for the address of the JobManager web frontend.
	 *
	 * @deprecated use {@link WebOptions#ADDRESS} instead
	 */
	@Deprecated
	public static final ConfigOption<String> DEFAULT_JOB_MANAGER_WEB_FRONTEND_ADDRESS =
		key("jobmanager.web.address")
			.noDefaultValue();

	/**
	 * The config key for the port of the JobManager web frontend.
	 * Setting this value to {@code -1} disables the web frontend.
	 *
	 * @deprecated use {@link WebOptions#PORT} instead
	 */
	@Deprecated
	public static final int DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT = 8081;

	/**
	 * Default value to override SSL support for the JobManager web UI.
	 *
	 * @deprecated use {@link WebOptions#SSL_ENABLED} instead
	 */
	@Deprecated
	public static final boolean DEFAULT_JOB_MANAGER_WEB_SSL_ENABLED = true;

	/**
	 * The default number of archived jobs for the jobmanager.
	 *
	 * @deprecated use {@link WebOptions#ARCHIVE_COUNT} instead
	 */
	@Deprecated
	public static final int DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT = 5;

	/**
	 * By default, submitting jobs from the web-frontend is allowed.
	 *
	 * @deprecated use {@link WebOptions#SUBMIT_ENABLE} instead
	 */
	@Deprecated
	public static final boolean DEFAULT_JOB_MANAGER_WEB_SUBMIT_ENABLED = true;

	/** @deprecated Config key has been deprecated. Therefore, no default value required. */
	@Deprecated
	public static final boolean DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_DISABLE = false;

	/**
	 * Default number of checkpoints to remember for recent history.
	 *
	 * @deprecated use {@link WebOptions#CHECKPOINTS_HISTORY_SIZE} instead
	 */
	@Deprecated
	public static final int DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE = 10;

	/**
	 * Time after which cached stats are cleaned up.
	 *
	 * @deprecated use {@link WebOptions#BACKPRESSURE_CLEANUP_INTERVAL} instead
	 */
	@Deprecated
	public static final int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_CLEAN_UP_INTERVAL = 10 * 60 * 1000;

	/**
	 * Time after which available stats are deprecated and need to be refreshed (by resampling).
	 *
	 * @deprecated use {@link WebOptions#BACKPRESSURE_REFRESH_INTERVAL} instead
	 */
	@Deprecated
	public static final int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_REFRESH_INTERVAL = 60 * 1000;

	/**
	 * Number of samples to take to determine back pressure.
	 *
	 * @deprecated use {@link WebOptions#BACKPRESSURE_NUM_SAMPLES} instead
	 */
	@Deprecated
	public static final int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_NUM_SAMPLES = 100;

	/**
	 * Delay between samples to determine back pressure.
	 *
	 * @deprecated use {@link WebOptions#BACKPRESSURE_DELAY} instead
	 */
	@Deprecated
	public static final int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_DELAY = 50;

	// ------------------------------ Akka Values ------------------------------

	/**
	 * @deprecated Use {@link AkkaOptions#TRANSPORT_HEARTBEAT_INTERVAL} instead.
	 */
	@Deprecated
	public static final String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL = "1000 s";

	/**
	 * @deprecated Use {@link AkkaOptions#TRANSPORT_HEARTBEAT_PAUSE} instead.
	 */
	@Deprecated
	public static final String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE = "6000 s";

	/**
	 * @deprecated Use {@link AkkaOptions#TRANSPORT_THRESHOLD} instead.
	 */
	@Deprecated
	public static final double DEFAULT_AKKA_TRANSPORT_THRESHOLD = 300.0;

	/**
	 * @deprecated Use {@link AkkaOptions#WATCH_THRESHOLD} instead.
	 */
	@Deprecated
	public static final double DEFAULT_AKKA_WATCH_THRESHOLD = 12;

	/**
	 * @deprecated Use {@link AkkaOptions#DISPATCHER_THROUGHPUT} instead.
	 */
	@Deprecated
	public static final int DEFAULT_AKKA_DISPATCHER_THROUGHPUT = 15;

	/**
	 * @deprecated Use {@link AkkaOptions#LOG_LIFECYCLE_EVENTS} instead.
	 */
	@Deprecated
	public static final boolean DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS = false;

	/**
	 * @deprecated Use {@link AkkaOptions#FRAMESIZE} instead.
	 */
	@Deprecated
	public static final String DEFAULT_AKKA_FRAMESIZE = "10485760b";

	/**
	 * @deprecated Use {@link AkkaOptions#ASK_TIMEOUT} instead.
	 */
	@Deprecated
	public static final String DEFAULT_AKKA_ASK_TIMEOUT = "10 s";

	/**
	 * @deprecated Use {@link AkkaOptions#LOOKUP_TIMEOUT} instead.
	 */
	@Deprecated
	public static final String DEFAULT_AKKA_LOOKUP_TIMEOUT = "10 s";

	/**
	 * @deprecated Use {@link AkkaOptions#CLIENT_TIMEOUT} instead.
	 */
	@Deprecated
	public static final String DEFAULT_AKKA_CLIENT_TIMEOUT = "60 s";

	/**
	 * @deprecated Use {@link AkkaOptions#SSL_ENABLED} instead.
	 */
	@Deprecated
	public static final boolean DEFAULT_AKKA_SSL_ENABLED = true;

	// ----------------------------- SSL Values --------------------------------

	/**
	 * @deprecated use {@link SecurityOptions#SSL_ENABLED} instead
	 */
	@Deprecated
	public static final boolean DEFAULT_SECURITY_SSL_ENABLED = false;

	/**
	 * @deprecated use {@link SecurityOptions#SSL_PROTOCOL} instead
	 */
	@Deprecated
	public static final String DEFAULT_SECURITY_SSL_PROTOCOL = "TLSv1.2";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_ALGORITHMS} instead
	 */
	@Deprecated
	public static final String DEFAULT_SECURITY_SSL_ALGORITHMS = "TLS_RSA_WITH_AES_128_CBC_SHA";

	/**
	 * @deprecated use {@link SecurityOptions#SSL_VERIFY_HOSTNAME} instead
	 */
	@Deprecated
	public static final boolean DEFAULT_SECURITY_SSL_VERIFY_HOSTNAME = true;

	// ----------------------------- Streaming Values --------------------------

	public static final String DEFAULT_STATE_BACKEND = "jobmanager";

	// ----------------------------- LocalExecution ----------------------------

	/**
	 * Sets the number of local task managers.
	 */
	public static final String LOCAL_NUMBER_TASK_MANAGER = "local.number-taskmanager";

	public static final int DEFAULT_LOCAL_NUMBER_TASK_MANAGER = 1;

	public static final String LOCAL_NUMBER_JOB_MANAGER = "local.number-jobmanager";

	public static final int DEFAULT_LOCAL_NUMBER_JOB_MANAGER = 1;

	/**
	 * @deprecated Use {@link ResourceManagerOptions#LOCAL_NUMBER_RESOURCE_MANAGER} instead.
	 */
	@Deprecated
	public static final String LOCAL_NUMBER_RESOURCE_MANAGER = "local.number-resourcemanager";

	/**
	 * @deprecated Use {@link ResourceManagerOptions#LOCAL_NUMBER_RESOURCE_MANAGER} instead.
	 */
	@Deprecated
	public static final int DEFAULT_LOCAL_NUMBER_RESOURCE_MANAGER = 1;

	public static final String LOCAL_START_WEBSERVER = "local.start-webserver";

	// --------------------------- High Availability ---------------------------------

	/** @deprecated Deprecated in favour of {@link HighAvailabilityOptions#HA_MODE} */
	@PublicEvolving
	@Deprecated
	public static final String DEFAULT_HA_MODE = "none";

	/** @deprecated Deprecated in favour of {@link #DEFAULT_HA_MODE} */
	@Deprecated
	public static final String DEFAULT_RECOVERY_MODE = "standalone";

	/**
	 * Default port used by the job manager if not in standalone recovery mode. If <code>0</code>
	 * the OS picks a random port port.
	 *
	 * @deprecated No longer used.
	 */
	@PublicEvolving
	@Deprecated
	public static final String DEFAULT_HA_JOB_MANAGER_PORT = "0";

	/** @deprecated Deprecated in favour of {@link #DEFAULT_HA_JOB_MANAGER_PORT} */
	@Deprecated
	public static final String DEFAULT_RECOVERY_JOB_MANAGER_PORT = "0";

	// --------------------------- ZooKeeper ----------------------------------

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_ROOT}. */
	@Deprecated
	public static final String DEFAULT_ZOOKEEPER_DIR_KEY = "/flink";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_CLUSTER_ID}. */
	@Deprecated
	public static final String DEFAULT_ZOOKEEPER_NAMESPACE_KEY = "/default";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_LATCH_PATH}. */
	@Deprecated
	public static final String DEFAULT_ZOOKEEPER_LATCH_PATH = "/leaderlatch";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_LEADER_PATH}. */
	@Deprecated
	public static final String DEFAULT_ZOOKEEPER_LEADER_PATH = "/leader";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_JOBGRAPHS_PATH}. */
	@Deprecated
	public static final String DEFAULT_ZOOKEEPER_JOBGRAPHS_PATH = "/jobgraphs";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_CHECKPOINTS_PATH}. */
	@Deprecated
	public static final String DEFAULT_ZOOKEEPER_CHECKPOINTS_PATH = "/checkpoints";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH} */
	@Deprecated
	public static final String DEFAULT_ZOOKEEPER_CHECKPOINT_COUNTER_PATH = "/checkpoint-counter";

	/** @deprecated in favor of {@link HighAvailabilityOptions#HA_ZOOKEEPER_MESOS_WORKERS_PATH}. */
	@Deprecated
	public static final String DEFAULT_ZOOKEEPER_MESOS_WORKERS_PATH = "/mesos-workers";

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_SESSION_TIMEOUT}. */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 60000;

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_CONNECTION_TIMEOUT}. */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT = 15000;

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_RETRY_WAIT}. */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_RETRY_WAIT = 5000;

	/** @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_MAX_RETRY_ATTEMPTS}. */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_MAX_RETRY_ATTEMPTS = 3;

	// - Defaults for required ZooKeeper configuration keys -------------------

	/**
	 * ZooKeeper default client port.
	 * @deprecated in favor of {@code FlinkZookeeperQuorumPeer#DEFAULT_ZOOKEEPER_CLIENT_PORT}.
	 */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;

	/**
	 * ZooKeeper default init limit.
	 * @deprecated in favor of {@code FlinkZookeeperQuorumPeer#DEFAULT_ZOOKEEPER_INIT_LIMIT}.
	 */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_INIT_LIMIT = 10;

	/**
	 * ZooKeeper default sync limit.
	 * @deprecated in favor of {@code FlinkZookeeperQuorumPeer#DEFAULT_ZOOKEEPER_SYNC_LIMIT}.
	 */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_SYNC_LIMIT = 5;

	/**
	 * ZooKeeper default peer port.
	 * @deprecated in favor of {@code FlinkZookeeperQuorumPeer#DEFAULT_ZOOKEEPER_PEER_PORT}.
	 */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_PEER_PORT = 2888;

	/**
	 * ZooKeeper default leader port.
	 * @deprecated in favor of {@code FlinkZookeeperQuorumPeer#DEFAULT_ZOOKEEPER_LEADER_PORT}.
	 */
	@Deprecated
	public static final int DEFAULT_ZOOKEEPER_LEADER_PORT = 3888;

	/**
	 * Defaults for ZK client security.
	 * @deprecated in favor of {@link SecurityOptions#ZOOKEEPER_SASL_DISABLE}.
	 */
	@Deprecated
	public static final boolean DEFAULT_ZOOKEEPER_SASL_DISABLE = true;

	/**
	 * ACL options supported "creator" or "open".
	 * @deprecated in favor of {@link HighAvailabilityOptions#ZOOKEEPER_CLIENT_ACL}.
	 */
	@Deprecated
	public static final String DEFAULT_HA_ZOOKEEPER_CLIENT_ACL = "open";

	// ----------------------------- Metrics ----------------------------

	/** @deprecated Use {@link MetricOptions#LATENCY_HISTORY_SIZE} instead. */
	@Deprecated
	public static final int DEFAULT_METRICS_LATENCY_HISTORY_SIZE = 128;

	// ----------------------------- Environment Variables ----------------------------

	/** The environment variable name which contains the location of the configuration directory. */
	public static final String ENV_FLINK_CONF_DIR = "FLINK_CONF_DIR";

	/** The environment variable name which contains the location of the lib folder. */
	public static final String ENV_FLINK_LIB_DIR = "FLINK_LIB_DIR";

	/** The environment variable name which contains the location of the opt directory. */
	public static final String ENV_FLINK_OPT_DIR = "FLINK_OPT_DIR";

	/** The environment variable name which contains the location of the plugins folder. */
	public static final String ENV_FLINK_PLUGINS_DIR = "FLINK_PLUGINS_DIR";

	/** The default Flink plugins directory if none has been specified via {@link #ENV_FLINK_PLUGINS_DIR}. */
	public static final String DEFAULT_FLINK_PLUGINS_DIRS = "plugins";

	/** The environment variable name which contains the location of the bin directory. */
	public static final String ENV_FLINK_BIN_DIR = "FLINK_BIN_DIR";

	/** The environment variable name which contains the Flink installation root directory. */
	public static final String ENV_FLINK_HOME_DIR = "FLINK_HOME";

	// ---------------------------- Encoding ------------------------------

	public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

	/**
	 * Not instantiable.
	 */
	private ConfigConstants() {}
}
