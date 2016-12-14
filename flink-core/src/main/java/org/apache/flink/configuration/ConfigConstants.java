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

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class contains all constants for the configuration. That includes the configuration keys and
 * the default values.
 */
@Public
public final class ConfigConstants {

	// ------------------------------------------------------------------------
	//                            Configuration Keys
	// ------------------------------------------------------------------------
	
	// ---------------------------- Parallelism -------------------------------

	/**
	 * The config parameter defining the default parallelism for jobs.
	 */
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
	public static final String RESTART_STRATEGY_FIXED_DELAY_DELAY = "restart-strategy.fixed-delay.delay";

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
	 */
	public static final String JOB_MANAGER_IPC_ADDRESS_KEY = "jobmanager.rpc.address";

	/**
	 * The config parameter defining the network port to connect to
	 * for communication with the job manager.
	 */
	public static final String JOB_MANAGER_IPC_PORT_KEY = "jobmanager.rpc.port";

	/**
	 * The config parameter defining the network port to connect to
	 * for communication with the resource manager.
	 */
	public static final String RESOURCE_MANAGER_IPC_PORT_KEY = "resourcemanager.rpc.port";

	/**
	 * The config parameter defining the storage directory to be used by the blob server.
	 */
	public static final String BLOB_STORAGE_DIRECTORY_KEY = "blob.storage.directory";

	/**
	 * The config parameter defining number of retires for failed BLOB fetches.
	 */
	public static final String BLOB_FETCH_RETRIES_KEY = "blob.fetch.retries";

	/**
	 * The config parameter defining the maximum number of concurrent BLOB fetches that the JobManager serves.
	 */
	public static final String BLOB_FETCH_CONCURRENT_KEY = "blob.fetch.num-concurrent";

	/**
	 * The config parameter defining the backlog of BLOB fetches on the JobManager
	 */
	public static final String BLOB_FETCH_BACKLOG_KEY = "blob.fetch.backlog";

	/**
	 * The config parameter defining the server port of the blob service.
	 * The port can either be a port, such as "9123",
	 * a range of ports: "50100-50200"
	 * or a list of ranges and or points: "50100-50200,50300-50400,51234"
	 *
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final String BLOB_SERVER_PORT = "blob.server.port";

	/** Flag to override ssl support for the blob service transport */
	public static final String BLOB_SERVICE_SSL_ENABLED = "blob.service.ssl.enabled";

	/**
	 * The config parameter defining the cleanup interval of the library cache manager.
	 */
	public static final String LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL = "library-cache-manager.cleanup.interval";

	/**
	 * The config parameter defining the task manager's hostname.
	 */
	public static final String TASK_MANAGER_HOSTNAME_KEY = "taskmanager.hostname";

	/**
	 * The config parameter defining the task manager's IPC port from the configuration.
	 */
	public static final String TASK_MANAGER_IPC_PORT_KEY = "taskmanager.rpc.port";

	/**
	 * The config parameter defining the task manager's data port from the configuration.
	 */
	public static final String TASK_MANAGER_DATA_PORT_KEY = "taskmanager.data.port";

	/**
	 * Config parameter to override SSL support for taskmanager's data transport
	 */
	public static final String TASK_MANAGER_DATA_SSL_ENABLED = "taskmanager.data.ssl.enabled";

	/**
	 * The config parameter defining the directories for temporary files.
	 */
	public static final String TASK_MANAGER_TMP_DIR_KEY = "taskmanager.tmp.dirs";

	/**
	 * The config parameter defining the taskmanager log file location
	 */
	public static final String TASK_MANAGER_LOG_PATH_KEY = "taskmanager.log.path";

	/**
	 * The config parameter defining the amount of memory to be allocated by the task manager's
	 * memory manager (in megabytes). If not set, a relative fraction will be allocated, as defined
	 * by {@link #TASK_MANAGER_MEMORY_FRACTION_KEY}.
	 */
	public static final String TASK_MANAGER_MEMORY_SIZE_KEY = "taskmanager.memory.size";
	
	/**
	 * The config parameter defining the fraction of free memory allocated by the memory manager.
	 */
	public static final String TASK_MANAGER_MEMORY_FRACTION_KEY = "taskmanager.memory.fraction";

	/**
	 * The config parameter defining the memory allocation method (JVM heap or off-heap).
	*/
	public static final String TASK_MANAGER_MEMORY_OFF_HEAP_KEY = "taskmanager.memory.off-heap";

	/**
	 * The config parameter for specifying whether TaskManager managed memory should be preallocated
	 * when the TaskManager is starting. (default is false)
	 */
	public static final String TASK_MANAGER_MEMORY_PRE_ALLOCATE_KEY = "taskmanager.memory.preallocate";

	/**
	 * The config parameter defining the number of buffers used in the network stack. This defines the
	 * number of possible tasks and shuffles.
	 */
	public static final String TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY = "taskmanager.network.numberOfBuffers";

	/**
	 * Config parameter defining the size of memory buffers used by the network stack and the memory manager.
	 */
	public static final String TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY = "taskmanager.memory.segment-size";
	
	/**
	 * The implementation to use for spillable/spilled intermediate results, which have both
	 * synchronous and asynchronous implementations: "sync" or "async".
	 */
	public static final String TASK_MANAGER_NETWORK_DEFAULT_IO_MODE = "taskmanager.network.defaultIOMode";

	/**
	 * The config parameter defining the number of task slots of a task manager.
	 */
	public static final String TASK_MANAGER_NUM_TASK_SLOTS = "taskmanager.numberOfTaskSlots";

	/**
	 * Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.
	 */
	public static final String TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD = "taskmanager.debug.memory.startLogThread";

	/**
	 * The interval (in ms) for the log thread to log the current memory usage.
	 */
	public static final String TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS = "taskmanager.debug.memory.logIntervalMs";

	/**
	 * Defines the maximum time it can take for the TaskManager registration. If the duration is
	 * exceeded without a successful registration, then the TaskManager terminates.
	 */
	public static final String TASK_MANAGER_MAX_REGISTRATION_DURATION = "taskmanager.maxRegistrationDuration";

	/**
	 * The initial registration pause between two consecutive registration attempts. The pause
	 * is doubled for each new registration attempt until it reaches the maximum registration pause.
	 */
	public static final String TASK_MANAGER_INITIAL_REGISTRATION_PAUSE = "taskmanager.initial-registration-pause";

	/**
	 * The maximum registration pause between two consecutive registration attempts.
	 */
	public static final String TASK_MANAGER_MAX_REGISTARTION_PAUSE = "taskmanager.max-registration-pause";

	/**
	 * The pause after a registration has been refused by the job manager before retrying to connect.
	 */
	public static final String TASK_MANAGER_REFUSED_REGISTRATION_PAUSE = "taskmanager.refused-registration-pause";

	/**
	 * Deprecated. Please use {@link TaskManagerOptions#TASK_CANCELLATION_INTERVAL}.
	 */
	@PublicEvolving
	@Deprecated
	public static final String TASK_CANCELLATION_INTERVAL_MILLIS = "task.cancellation-interval";

	// --------------------------- Runtime Algorithms -------------------------------
	
	/**
	 * Parameter for the maximum fan for out-of-core algorithms.
	 * Corresponds to the maximum fan-in for merge-sorts and the maximum fan-out
	 * for hybrid hash joins. 
	 */
	public static final String DEFAULT_SPILLING_MAX_FAN_KEY = "taskmanager.runtime.max-fan";
	
	/**
	 * Key for the default spilling threshold. When more than the threshold memory of the sort buffers is full, the
	 * sorter will start spilling to disk.
	 */
	public static final String DEFAULT_SORT_SPILLING_THRESHOLD_KEY = "taskmanager.runtime.sort-spilling-threshold";

	/**
	 * Parameter to switch hash join bloom filters for spilled partitions on and off.
	 */
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
	 */
	public static final String CONTAINERIZED_HEAP_CUTOFF_RATIO = "containerized.heap-cutoff-ratio";

	/**
	 * Minimum amount of heap memory to remove in containers, as a safety margin.
	 */
	public static final String CONTAINERIZED_HEAP_CUTOFF_MIN = "containerized.heap-cutoff-min";

	/**
	 * Prefix for passing custom environment variables to Flink's master process.
	 * For example for passing LD_LIBRARY_PATH as an env variable to the AppMaster, set:
	 * containerized.master.env.LD_LIBRARY_PATH: "/usr/lib/native"
	 * in the flink-conf.yaml.
	 */
	public static final String CONTAINERIZED_MASTER_ENV_PREFIX = "containerized.master.env.";

	/**
	 * Similar to the {@see CONTAINERIZED_MASTER_ENV_PREFIX}, this configuration prefix allows
	 * setting custom environment variables for the workers (TaskManagers)
	 */
	public static final String CONTAINERIZED_TASK_MANAGER_ENV_PREFIX = "containerized.taskmanager.env.";

	
	// ------------------------ YARN Configuration ------------------------

	/**
	 * The vcores exposed by YARN.
	 */
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
	 */
	@Deprecated
	public static final String YARN_REALLOCATE_FAILED_CONTAINERS = "yarn.reallocate-failed";

	/**
	 * The maximum number of failed YARN containers before entirely stopping
	 * the YARN session / job on YARN.
	 *
	 * By default, we take the number of of initially requested containers.
	 */
	public static final String YARN_MAX_FAILED_CONTAINERS = "yarn.maximum-failed-containers";

	/**
	 * Set the number of retries for failed YARN ApplicationMasters/JobManagers in high
	 * availability mode. This value is usually limited by YARN.
	 *
	 * By default, it's 1 in the standalone case and 2 in the high availability case.
	 */
	public static final String YARN_APPLICATION_ATTEMPTS = "yarn.application-attempts";

	/**
	 * The heartbeat interval between the Application Master and the YARN Resource Manager.
	 *
	 * The default value is 5 (seconds).
	 */
	public static final String YARN_HEARTBEAT_DELAY_SECONDS = "yarn.heartbeat-delay";

	/**
	 * When a Flink job is submitted to YARN, the JobManager's host and the number of available
	 * processing slots is written into a properties file, so that the Flink client is able
	 * to pick those details up.
	 * This configuration parameter allows changing the default location of that file (for example
	 * for environments sharing a Flink installation between users)
	 */
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

	// these default values are not used anymore, but remain here until Flink 2.0
	@Deprecated
	public static final String DEFAULT_YARN_APPLICATION_MASTER_PORT = "deprecated";
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
	 * The config parameter defining the Akka actor system port for the ApplicationMaster and
	 * JobManager
	 *
	 * The port can either be a port, such as "9123",
	 * a range of ports: "50100-50200"
	 * or a list of ranges and or points: "50100-50200,50300-50400,51234"
	 *
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final String YARN_APPLICATION_MASTER_PORT = "yarn.application-master.port";


	// ------------------------ Mesos Configuration ------------------------

	/**
	 * The initial number of Mesos tasks to allocate.
	 */
	public static final String MESOS_INITIAL_TASKS = "mesos.initial-tasks";

	/**
	 * The maximum number of failed Mesos tasks before entirely stopping
	 * the Mesos session / job on Mesos.
	 *
	 * By default, we take the number of of initially requested tasks.
	 */
	public static final String MESOS_MAX_FAILED_TASKS = "mesos.maximum-failed-tasks";

	/**
	 * The Mesos master URL.
	 *
	 * The value should be in one of the following forms:
	 * <pre>
	 * {@code
	 *     host:port
	 *     zk://host1:port1,host2:port2,.../path
	 *     zk://username:password@host1:port1,host2:port2,.../path
	 *     file:///path/to/file (where file contains one of the above)
	 * }
	 * </pre>
	 *
	 */
	public static final String MESOS_MASTER_URL = "mesos.master";

	/**
	 * The failover timeout for the Mesos scheduler, after which running tasks are automatically shut down.
	 *
	 * The default value is 600 (seconds).
	 */
	public static final String MESOS_FAILOVER_TIMEOUT_SECONDS = "mesos.failover-timeout";

	/**
	 * The config parameter defining the Mesos artifact server port to use.
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final String MESOS_ARTIFACT_SERVER_PORT_KEY = "mesos.resourcemanager.artifactserver.port";

	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_NAME = "mesos.resourcemanager.framework.name";

	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_ROLE = "mesos.resourcemanager.framework.role";

	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_PRINCIPAL = "mesos.resourcemanager.framework.principal";

	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_SECRET = "mesos.resourcemanager.framework.secret";

	public static final String MESOS_RESOURCEMANAGER_FRAMEWORK_USER = "mesos.resourcemanager.framework.user";

	/**
	 * Config parameter to override SSL support for the Artifact Server
	 */
	public static final String MESOS_ARTIFACT_SERVER_SSL_ENABLED = "mesos.resourcemanager.artifactserver.ssl.enabled";

	// ------------------------ Hadoop Configuration ------------------------

	/**
	 * Path to hdfs-defaul.xml file
	 */
	public static final String HDFS_DEFAULT_CONFIG = "fs.hdfs.hdfsdefault";
	
	/**
	 * Path to hdfs-site.xml file
	 */
	public static final String HDFS_SITE_CONFIG = "fs.hdfs.hdfssite";
	
	/**
	 * Path to Hadoop configuration
	 */
	public static final String PATH_HADOOP_CONFIG = "fs.hdfs.hadoopconf";
	
	// ------------------------ File System Behavior ------------------------

	/**
	 * Key to specify the default filesystem to be used by a job. In the case of
	 * <code>file:///</code>, which is the default (see {@link ConfigConstants#DEFAULT_FILESYSTEM_SCHEME}),
	 * the local filesystem is going to be used to resolve URIs without an explicit scheme.
	 * */
	public static final String FILESYSTEM_SCHEME = "fs.default-scheme";

	/**
	 * Key to specify whether the file systems should simply overwrite existing files.
	 */
	public static final String FILESYSTEM_DEFAULT_OVERWRITE_KEY = "fs.overwrite-files";

	/**
	 * Key to specify whether the file systems should always create a directory for the output, even with a parallelism of one.
	 */
	public static final String FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY = "fs.output.always-create-directory";

	// ---------------------------- Compiler -------------------------------

	/**
	 * The maximum number of line samples to be taken by the delimited input format, if no
	 * other value is specified for the data source.
	 */
	public static final String DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY = "compiler.delimited-informat.max-line-samples";

	/**
	 * The minimum number of line samples to be taken by the delimited input format, if no
	 * other value is specified for the data source.
	 */
	public static final String DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY = "compiler.delimited-informat.min-line-samples";

	/**
	 * The maximum length of a single sampled record before the sampling is aborted.
	 */
	public static final String DELIMITED_FORMAT_MAX_SAMPLE_LENGTH_KEY = "compiler.delimited-informat.max-sample-len";
	
	
	// ------------------------- JobManager Web Frontend ----------------------

	/**
	 * The port for the runtime monitor web-frontend server.
	 */
	public static final String JOB_MANAGER_WEB_PORT_KEY = "jobmanager.web.port";

	/**
	 * Config parameter to override SSL support for the JobManager Web UI
	 */
	public static final String JOB_MANAGER_WEB_SSL_ENABLED = "jobmanager.web.ssl.enabled";

	/**
	 * The config parameter defining the flink web directory to be used by the webmonitor.
	 */
	public static final String JOB_MANAGER_WEB_TMPDIR_KEY = "jobmanager.web.tmpdir";

	/**
	 * The config parameter defining the directory for uploading the job jars. If not specified a dynamic directory
	 * will be used under the directory specified by JOB_MANAGER_WEB_TMPDIR_KEY.
	 */
	public static final String JOB_MANAGER_WEB_UPLOAD_DIR_KEY = "jobmanager.web.upload.dir";

	/**
	 * The config parameter defining the number of archived jobs for the jobmanager
	 */
	public static final String JOB_MANAGER_WEB_ARCHIVE_COUNT = "jobmanager.web.history";

	/**
	 * The log file location (may be in /log for standalone but under log directory when using YARN)
	 */
	public static final String JOB_MANAGER_WEB_LOG_PATH_KEY = "jobmanager.web.log.path";

	/** Config parameter indicating whether jobs can be uploaded and run from the web-frontend. */
	public static final String JOB_MANAGER_WEB_SUBMIT_ENABLED_KEY = "jobmanager.web.submit.enable";

	/** Flag to disable checkpoint stats. */
	public static final String JOB_MANAGER_WEB_CHECKPOINTS_DISABLE = "jobmanager.web.checkpoints.disable";

	/** Config parameter defining the number of checkpoints to remember for recent history. */
	public static final String JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE = "jobmanager.web.checkpoints.history";

	/** Time after which cached stats are cleaned up if not accessed. */
	public static final String JOB_MANAGER_WEB_BACK_PRESSURE_CLEAN_UP_INTERVAL = "jobmanager.web.backpressure.cleanup-interval";

	/** Time after which available stats are deprecated and need to be refreshed (by resampling). */
	public static final String JOB_MANAGER_WEB_BACK_PRESSURE_REFRESH_INTERVAL = "jobmanager.web.backpressure.refresh-interval";

	/** Number of stack trace samples to take to determine back pressure. */
	public static final String JOB_MANAGER_WEB_BACK_PRESSURE_NUM_SAMPLES = "jobmanager.web.backpressure.num-samples";

	/** Delay between stack trace samples to determine back pressure. */
	public static final String JOB_MANAGER_WEB_BACK_PRESSURE_DELAY = "jobmanager.web.backpressure.delay-between-samples";

	// ------------------------------ AKKA ------------------------------------

	/**
	 * Timeout for the startup of the actor system
	 */
	public static final String AKKA_STARTUP_TIMEOUT = "akka.startup-timeout";

	/**
	 * Heartbeat interval of the transport failure detector
	 */
	public static final String AKKA_TRANSPORT_HEARTBEAT_INTERVAL = "akka.transport.heartbeat.interval";

	/**
	 * Allowed heartbeat pause for the transport failure detector
	 */
	public static final String AKKA_TRANSPORT_HEARTBEAT_PAUSE = "akka.transport.heartbeat.pause";

	/**
	 * Detection threshold of transport failure detector
	 */
	public static final String AKKA_TRANSPORT_THRESHOLD = "akka.transport.threshold";

	/**
	 * Heartbeat interval of watch failure detector
	 */
	public static final String AKKA_WATCH_HEARTBEAT_INTERVAL = "akka.watch.heartbeat.interval";

	/**
	 * Allowed heartbeat pause for the watch failure detector
	 */
	public static final String AKKA_WATCH_HEARTBEAT_PAUSE = "akka.watch.heartbeat.pause";

	/**
	 * Detection threshold for the phi accrual watch failure detector
	 */
	public static final String AKKA_WATCH_THRESHOLD = "akka.watch.threshold";

	/**
	 * Akka TCP timeout
	 */
	public static final String AKKA_TCP_TIMEOUT = "akka.tcp.timeout";

	/**
	 * Override SSL support for the Akka transport
	 */
	public static final String AKKA_SSL_ENABLED = "akka.ssl.enabled";

	/**
	 * Maximum framesize of akka messages
	 */
	public static final String AKKA_FRAMESIZE = "akka.framesize";

	/**
	 * Maximum number of messages until another actor is executed by the same thread
	 */
	public static final String AKKA_DISPATCHER_THROUGHPUT = "akka.throughput";

	/**
	 * Log lifecycle events
	 */
	public static final String AKKA_LOG_LIFECYCLE_EVENTS = "akka.log.lifecycle.events";

	/**
	 * Timeout for all blocking calls on the cluster side
	 */
	public static final String AKKA_ASK_TIMEOUT = "akka.ask.timeout";

	/**
	 * Timeout for all blocking calls that look up remote actors
	 */
	public static final String AKKA_LOOKUP_TIMEOUT = "akka.lookup.timeout";

	/**
	 * Timeout for all blocking calls on the client side
	 */
	public static final String AKKA_CLIENT_TIMEOUT = "akka.client.timeout";

	/**
	 * Exit JVM on fatal Akka errors
	 */
	public static final String AKKA_JVM_EXIT_ON_FATAL_ERROR = "akka.jvm-exit-on-fatal-error";
	
	// ----------------------------- Transport SSL Settings--------------------

	/**
	 * Enable SSL support
	 */
	public static final String SECURITY_SSL_ENABLED = "security.ssl.enabled";

	/** The Java keystore file containing the flink endpoint key and certificate */
	public static final String SECURITY_SSL_KEYSTORE = "security.ssl.keystore";

	/** secret to decrypt the keystore file */
	public static final String SECURITY_SSL_KEYSTORE_PASSWORD = "security.ssl.keystore-password";

	/** secret to decrypt the server key */
	public static final String SECURITY_SSL_KEY_PASSWORD = "security.ssl.key-password";

	/** The truststore file containing the public CA certificates to verify the ssl peers */
	public static final String SECURITY_SSL_TRUSTSTORE = "security.ssl.truststore";

	/** Secret to decrypt the truststore */
	public static final String SECURITY_SSL_TRUSTSTORE_PASSWORD = "security.ssl.truststore-password";

	/** SSL protocol version to be supported */
	public static final String SECURITY_SSL_PROTOCOL = "security.ssl.protocol";

	/**
	 * The standard SSL algorithms to be supported
	 * More options here - http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites
	 * */
	public static final String SECURITY_SSL_ALGORITHMS = "security.ssl.algorithms";

	/** Flag to enable/disable hostname verification for the ssl connections */
	public static final String SECURITY_SSL_VERIFY_HOSTNAME = "security.ssl.verify-hostname";

	// ----------------------------- Streaming --------------------------------
	
	/**
	 * State backend for checkpoints;
	 */
	public static final String STATE_BACKEND = "state.backend";
	
	// ----------------------------- Miscellaneous ----------------------------
	
	/**
	 * The key to the Flink base directory path
	 */
	public static final String FLINK_BASE_DIR_PATH_KEY = "flink.base.dir.path";
	
	public static final String FLINK_JVM_OPTIONS = "env.java.opts";

	// --------------------------- High Availability --------------------------

	/** Defines high availabilty mode used for the cluster execution ("NONE", "ZOOKEEPER") */
	@PublicEvolving
	public static final String HA_MODE = "high-availability";

	/** Ports used by the job manager if not in 'none' recovery mode */
	@PublicEvolving
	public static final String HA_JOB_MANAGER_PORT = "high-availability.jobmanager.port";

	/** The time before the JobManager recovers persisted jobs */
	@PublicEvolving
	public static final String HA_JOB_DELAY = "high-availability.job.delay";

	/** Deprecated in favour of {@link #HA_MODE}. */
	@Deprecated
	public static final String RECOVERY_MODE = "recovery.mode";

	/** Deprecated in favour of {@link #HA_JOB_MANAGER_PORT}. */
	@Deprecated
	public static final String RECOVERY_JOB_MANAGER_PORT = "recovery.jobmanager.port";

	/** Deprecated in favour of {@link #HA_JOB_DELAY}. */
	@Deprecated
	public static final String RECOVERY_JOB_DELAY = "recovery.job.delay";

	// --------------------------- ZooKeeper ----------------------------------

	/** ZooKeeper servers. */
	@PublicEvolving
	public static final String HA_ZOOKEEPER_QUORUM_KEY = "high-availability.zookeeper.quorum";

	/**
	 * File system state backend base path for recoverable state handles. Recovery state is written
	 * to this path and the file state handles are persisted for recovery.
	 */
	@PublicEvolving
	public static final String HA_ZOOKEEPER_STORAGE_PATH = "high-availability.zookeeper.storageDir";

	/** ZooKeeper root path. */
	@PublicEvolving
	public static final String HA_ZOOKEEPER_DIR_KEY = "high-availability.zookeeper.path.root";

	@PublicEvolving
	public static final String HA_ZOOKEEPER_NAMESPACE_KEY = "high-availability.zookeeper.path.namespace";

	@PublicEvolving
	public static final String HA_ZOOKEEPER_LATCH_PATH = "high-availability.zookeeper.path.latch";

	/** ZooKeeper root path (ZNode) for job graphs. */
	@PublicEvolving
	public static final String HA_ZOOKEEPER_JOBGRAPHS_PATH = "high-availability.zookeeper.path.jobgraphs";

	@PublicEvolving
	public static final String HA_ZOOKEEPER_LEADER_PATH = "high-availability.zookeeper.path.leader";

	/** ZooKeeper root path (ZNode) for completed checkpoints. */
	@PublicEvolving
	public static final String HA_ZOOKEEPER_CHECKPOINTS_PATH = "high-availability.zookeeper.path.checkpoints";

	/** ZooKeeper root path (ZNode) for checkpoint counters. */
	@PublicEvolving
	public static final String HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH = "high-availability.zookeeper.path.checkpoint-counter";

	/** ZooKeeper root path (ZNode) for Mesos workers. */
	@PublicEvolving
	public static final String HA_ZOOKEEPER_MESOS_WORKERS_PATH = "recovery.zookeeper.path.mesos-workers";

	@PublicEvolving
	public static final String HA_ZOOKEEPER_SESSION_TIMEOUT = "high-availability.zookeeper.client.session-timeout";

	@PublicEvolving
	public static final String HA_ZOOKEEPER_CONNECTION_TIMEOUT = "high-availability.zookeeper.client.connection-timeout";

	@PublicEvolving
	public static final String HA_ZOOKEEPER_RETRY_WAIT = "high-availability.zookeeper.client.retry-wait";

	@PublicEvolving
	public static final String HA_ZOOKEEPER_MAX_RETRY_ATTEMPTS = "high-availability.zookeeper.client.max-retry-attempts";

	@PublicEvolving
	public static final String HA_ZOOKEEPER_CLIENT_ACL = "high-availability.zookeeper.client.acl";

	@PublicEvolving
	public static final String ZOOKEEPER_SASL_DISABLE = "zookeeper.sasl.disable";

	@PublicEvolving
	public static final String ZOOKEEPER_SASL_SERVICE_NAME = "zookeeper.sasl.service-name";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_QUORUM_KEY}. */
	@Deprecated
	public static final String ZOOKEEPER_QUORUM_KEY = "recovery.zookeeper.quorum";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_STORAGE_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_RECOVERY_PATH = "recovery.zookeeper.storageDir";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_DIR_KEY}. */
	@Deprecated
	public static final String ZOOKEEPER_DIR_KEY = "recovery.zookeeper.path.root";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_NAMESPACE_KEY}. */
	@Deprecated
	public static final String ZOOKEEPER_NAMESPACE_KEY = "recovery.zookeeper.path.namespace";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_LATCH_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_LATCH_PATH = "recovery.zookeeper.path.latch";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_LEADER_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_LEADER_PATH = "recovery.zookeeper.path.leader";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_JOBGRAPHS_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_JOBGRAPHS_PATH = "recovery.zookeeper.path.jobgraphs";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_CHECKPOINTS_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_CHECKPOINTS_PATH = "recovery.zookeeper.path.checkpoints";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH}. */
	@Deprecated
	public static final String ZOOKEEPER_CHECKPOINT_COUNTER_PATH = "recovery.zookeeper.path.checkpoint-counter";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_SESSION_TIMEOUT}. */
	@Deprecated
	public static final String ZOOKEEPER_SESSION_TIMEOUT = "recovery.zookeeper.client.session-timeout";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_CONNECTION_TIMEOUT}. */
	@Deprecated
	public static final String ZOOKEEPER_CONNECTION_TIMEOUT = "recovery.zookeeper.client.connection-timeout";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_RETRY_WAIT}. */
	@Deprecated
	public static final String ZOOKEEPER_RETRY_WAIT = "recovery.zookeeper.client.retry-wait";

	/** Deprecated in favour of {@link #HA_ZOOKEEPER_MAX_RETRY_ATTEMPTS}. */
	@Deprecated
	public static final String ZOOKEEPER_MAX_RETRY_ATTEMPTS = "recovery.zookeeper.client.max-retry-attempts";

	// ---------------------------- Metrics -----------------------------------

	/**
	 * The list of named reporters. Names are defined here and per-reporter configs
	 * are given with the reporter config prefix and the reporter name.
	 *
	 * Example:
	 * <pre>{@code
	 * metrics.reporters = foo, bar
	 *
	 * metrics.reporter.foo.class = org.apache.flink.metrics.reporter.JMXReporter
	 * metrics.reporter.foo.interval = 10
	 *
	 * metrics.reporter.bar.class = org.apache.flink.metrics.graphite.GraphiteReporter
	 * metrics.reporter.bar.port = 1337
	 * }</pre>
	 */
	public static final String METRICS_REPORTERS_LIST = "metrics.reporters";

	/**
	 * The prefix for per-reporter configs. Has to be combined with a reporter name and
	 * the configs mentioned below.
	 */
	public static final String METRICS_REPORTER_PREFIX = "metrics.reporter.";

	/** The class of the reporter to use. This is used as a suffix in an actual reporter config */
	public static final String METRICS_REPORTER_CLASS_SUFFIX = "class";
	
	/** The interval between reports. This is used as a suffix in an actual reporter config */
	public static final String METRICS_REPORTER_INTERVAL_SUFFIX = "interval";

	/**	The delimiter used to assemble the metric identifier. This is used as a suffix in an actual reporter config. */
	public static final String METRICS_REPORTER_SCOPE_DELIMITER = "scope.delimiter";

	/** The delimiter used to assemble the metric identifier. */
	public static final String METRICS_SCOPE_DELIMITER = "metrics.scope.delimiter";

	/** The scope format string that is applied to all metrics scoped to a JobManager. */
	public static final String METRICS_SCOPE_NAMING_JM = "metrics.scope.jm";

	/** The scope format string that is applied to all metrics scoped to a TaskManager. */
	public static final String METRICS_SCOPE_NAMING_TM = "metrics.scope.tm";

	/** The scope format string that is applied to all metrics scoped to a job on a JobManager. */
	public static final String METRICS_SCOPE_NAMING_JM_JOB = "metrics.scope.jm.job";

	/** The scope format string that is applied to all metrics scoped to a job on a TaskManager. */
	public static final String METRICS_SCOPE_NAMING_TM_JOB = "metrics.scope.tm.job";

	/** The scope format string that is applied to all metrics scoped to a task. */
	public static final String METRICS_SCOPE_NAMING_TASK = "metrics.scope.task";

	/** The scope format string that is applied to all metrics scoped to an operator. */
	public static final String METRICS_SCOPE_NAMING_OPERATOR = "metrics.scope.operator";

	/** The number of measured latencies to maintain at each operator */
	public static final String METRICS_LATENCY_HISTORY_SIZE = "metrics.latency.history-size";


	// ---------------------------- Checkpoints -------------------------------

	/** The default directory for savepoints. */
	@PublicEvolving
	public static final String SAVEPOINT_DIRECTORY_KEY = "state.savepoints.dir";

	/** The default directory used for persistent checkpoints. */
	@PublicEvolving
	public static final String CHECKPOINTS_DIRECTORY_KEY = "state.checkpoints.dir";

	/**
	 * This key was used in Flink versions <= 1.1.X with the savepoint backend
	 * configuration. We now always use the FileSystem for savepoints. For this,
	 * the only relevant config key is {@link #SAVEPOINT_DIRECTORY_KEY}.
	 */
	@Deprecated
	public static final String SAVEPOINT_FS_DIRECTORY_KEY = "savepoints.state.backend.fs.dir";

	// ------------------------------------------------------------------------
	//                            Default Values
	// ------------------------------------------------------------------------

	// ---------------------------- Parallelism -------------------------------
	
	/**
	 * The default parallelism for operations.
	 */
	public static final int DEFAULT_PARALLELISM = 1;
	
	/**
	 * The default number of execution retries.
	 */
	public static final int DEFAULT_EXECUTION_RETRIES = 0;

	// ------------------------------ Runtime ---------------------------------

	/**
	 * The default library cache manager cleanup interval in seconds
	 */
	public static final long DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL = 3600;
	
	/**
	 * The default network port to connect to for communication with the job manager.
	 */
	public static final int DEFAULT_JOB_MANAGER_IPC_PORT = 6123;

	/**
	 * The default network port of the resource manager.
	 */
	public static final int DEFAULT_RESOURCE_MANAGER_IPC_PORT = 0;

	/**
	 * The default value to override ssl support for blob service transport
	 */
	public static final boolean DEFAULT_BLOB_SERVICE_SSL_ENABLED = true;

	/**
	 * Default number of retries for failed BLOB fetches.
	 */
	public static final int DEFAULT_BLOB_FETCH_RETRIES = 5;

	/**
	 * Default number of concurrent BLOB fetch operations.
	 */
	public static final int DEFAULT_BLOB_FETCH_CONCURRENT = 50;

	/**
	 * Default BLOB fetch connection backlog.
	 */
	public static final int DEFAULT_BLOB_FETCH_BACKLOG = 1000;

	/**
	 * Default BLOB server port. 0 means ephemeral port.
	 */
	public static final String DEFAULT_BLOB_SERVER_PORT = "0";

	/**
	 * The default network port the task manager expects incoming IPC connections. The {@code 0} means that
	 * the TaskManager searches for a free port.
	 */
	public static final int DEFAULT_TASK_MANAGER_IPC_PORT = 0;

	/**
	 * The default network port the task manager expects to receive transfer envelopes on. The {@code 0} means that
	 * the TaskManager searches for a free port.
	 */
	public static final int DEFAULT_TASK_MANAGER_DATA_PORT = 0;

	/**
	 * The default value to override ssl support for task manager's data transport
	 */
	public static final boolean DEFAULT_TASK_MANAGER_DATA_SSL_ENABLED = true;

	/**
	 * The default directory for temporary files of the task manager.
	 */
	public static final String DEFAULT_TASK_MANAGER_TMP_PATH = System.getProperty("java.io.tmpdir");
	
	/**
	 * The default fraction of the free memory allocated by the task manager's memory manager.
	 */
	public static final float DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION = 0.7f;
	
	/**
	 * Default number of buffers used in the network stack.
	 */
	public static final int DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS = 2048;

	/**
	 * Default size of memory segments in the network stack and the memory manager.
	 */
	public static final int DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE = 32768;

	/**
	 * The implementation to use for spillable/spilled intermediate results, which have both
	 * synchronous and asynchronous implementations: "sync" or "async".
	 */
	public static final String DEFAULT_TASK_MANAGER_NETWORK_DEFAULT_IO_MODE = "sync";

	/**
	 * Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.
	 */
	public static final boolean DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD = false;

	/**
	 * The interval (in ms) for the log thread to log the current memory usage.
	 */
	public static final long DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS = 5000L;

	/**
	 * The default task manager's maximum registration duration
	 */
	public static final String DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION = "Inf";

	/**
	 * The default task manager's initial registration pause.
	 */
	public static final String DEFAULT_TASK_MANAGER_INITIAL_REGISTRATION_PAUSE = "500 ms";

	/**
	 * The default task manager's maximum registration pause.
	 */
	public static final String DEFAULT_TASK_MANAGER_MAX_REGISTRATION_PAUSE = "30 s";

	/**
	 * The default task manager's refused registration pause.
	 */
	public static final String DEFAULT_TASK_MANAGER_REFUSED_REGISTRATION_PAUSE = "10 s";

	/**
	 * The default setting for TaskManager memory eager allocation of managed memory
	 */
	public static final boolean DEFAULT_TASK_MANAGER_MEMORY_PRE_ALLOCATE = false;

	/**
	 * Deprecated. Please use {@link TaskManagerOptions#TASK_CANCELLATION_INTERVAL}.
	 */
	@Deprecated
	public static final long DEFAULT_TASK_CANCELLATION_INTERVAL_MILLIS = 30000;

	// ------------------------ Runtime Algorithms ------------------------
	
	/**
	 * Default setting for the switch for hash join bloom filters for spilled partitions.
	 */
	public static final boolean DEFAULT_RUNTIME_HASH_JOIN_BLOOM_FILTERS = false;
	
	/**
	 * The default value for the maximum spilling fan in/out.
	 */
	public static final int DEFAULT_SPILLING_MAX_FAN = 128;
	
	/**
	 * The default percentage of the sort memory to be full before data is spilled.
	 */
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
	 */
	public static final int DEFAULT_YARN_HEAP_CUTOFF = 600;

	/**
	 * Relative amount of memory to subtract from Java process memory to get the TaskManager
	 * heap size
	 */
	public static final float DEFAULT_YARN_HEAP_CUTOFF_RATIO = 0.25f;

	/**
	 * Default port for the application master is 0, which means
	 * the operating system assigns an ephemeral port
	 */
	public static final String DEFAULT_YARN_JOB_MANAGER_PORT = "0";

	// ------ Mesos-Specific Configuration ------
	// For more configuration entries please see {@code MesosTaskManagerParameters}.

	/** The default failover timeout provided to Mesos (10 mins) */
	public static final int DEFAULT_MESOS_FAILOVER_TIMEOUT_SECS = 10 * 60;

	/**
	 * The default network port to listen on for the Mesos artifact server.
	 */
	public static final int DEFAULT_MESOS_ARTIFACT_SERVER_PORT = 0;

	/**
	 * The default Mesos framework name for the ResourceManager to use.
	 */
	public static final String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_NAME = "Flink";

	public static final String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_ROLE = "*";

	public static final String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_USER = "";

	/** Default value to override SSL support for the Artifact Server */
	public static final boolean DEFAULT_MESOS_ARTIFACT_SERVER_SSL_ENABLED = true;

	// ------------------------ File System Behavior ------------------------

	/**
	 * The default filesystem to be used, if no other scheme is specified in the
	 * user-provided URI (= local filesystem)
	 * */
	public static final String DEFAULT_FILESYSTEM_SCHEME = "file:///";
	
	/**
	 * The default behavior with respect to overwriting existing files (= not overwrite)
	 */
	public static final boolean DEFAULT_FILESYSTEM_OVERWRITE = false;

	/**
	 * The default behavior for output directory creating (create only directory when parallelism &gt; 1).
	 */
	public static final boolean DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY = false;
	
	
	// ---------------------------- Compiler -------------------------------

	/**
	 * The default maximum number of line samples taken by the delimited input format.
	 */
	public static final int DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES = 10;
	
	/**
	 * The default minimum number of line samples taken by the delimited input format.
	 */
	public static final int DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES = 2;
	
	/**
	 * The default maximum sample length before sampling is aborted (2 MiBytes).
	 */
	public static final int DEFAULT_DELIMITED_FORMAT_MAX_SAMPLE_LEN = 2 * 1024 * 1024;
	
	
	// ------------------------- JobManager Web Frontend ----------------------

	/** The config key for the address of the JobManager web frontend. */
	public static final ConfigOption<String> DEFAULT_JOB_MANAGER_WEB_FRONTEND_ADDRESS =
		key("jobmanager.web.address")
			.noDefaultValue();

	/** The config key for the port of the JobManager web frontend.
	 * Setting this value to {@code -1} disables the web frontend. */
	public static final int DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT = 8081;

	/** Default value to override SSL support for the JobManager web UI */
	public static final boolean DEFAULT_JOB_MANAGER_WEB_SSL_ENABLED = true;

	/** The default number of archived jobs for the jobmanager */
	public static final int DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT = 5;

	/** By default, submitting jobs from the web-frontend is allowed. */
	public static final boolean DEFAULT_JOB_MANAGER_WEB_SUBMIT_ENABLED = true;

	/** Default flag to disable checkpoint stats. */
	public static final boolean DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_DISABLE = false;

	/** Default number of checkpoints to remember for recent history. */
	public static final int DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE = 10;

	/** Time after which cached stats are cleaned up. */
	public static final int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_CLEAN_UP_INTERVAL = 10 * 60 * 1000;

	/** Time after which available stats are deprecated and need to be refreshed (by resampling). */
	public static final int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_REFRESH_INTERVAL = 60 * 1000;

	/** Number of samples to take to determine back pressure. */
	public static final int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_NUM_SAMPLES = 100;

	/** Delay between samples to determine back pressure. */
	public static final int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_DELAY = 50;

	// ------------------------------ Akka Values ------------------------------

	public static String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL = "1000 s";

	public static String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE = "6000 s";

	public static double DEFAULT_AKKA_TRANSPORT_THRESHOLD = 300.0;

	public static double DEFAULT_AKKA_WATCH_THRESHOLD = 12;

	public static int DEFAULT_AKKA_DISPATCHER_THROUGHPUT = 15;

	public static boolean DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS = false;

	public static String DEFAULT_AKKA_FRAMESIZE = "10485760b";

	public static String DEFAULT_AKKA_ASK_TIMEOUT = "10 s";

	public static String DEFAULT_AKKA_LOOKUP_TIMEOUT = "10 s";

	public static String DEFAULT_AKKA_CLIENT_TIMEOUT = "60 s";

	public static boolean DEFAULT_AKKA_SSL_ENABLED = true;

	// ----------------------------- SSL Values --------------------------------

	public static boolean DEFAULT_SECURITY_SSL_ENABLED = false;

	public static String DEFAULT_SECURITY_SSL_PROTOCOL = "TLSv1.2";

	public static String DEFAULT_SECURITY_SSL_ALGORITHMS = "TLS_RSA_WITH_AES_128_CBC_SHA";

	public static boolean DEFAULT_SECURITY_SSL_VERIFY_HOSTNAME = true;

	// ----------------------------- Streaming Values --------------------------
	
	public static String DEFAULT_STATE_BACKEND = "jobmanager";

	// ----------------------------- LocalExecution ----------------------------

	/**
	 * Sets the number of local task managers
	 */
	public static final String LOCAL_NUMBER_TASK_MANAGER = "local.number-taskmanager";

	public static final int DEFAULT_LOCAL_NUMBER_TASK_MANAGER = 1;

	public static final String LOCAL_NUMBER_JOB_MANAGER = "local.number-jobmanager";

	public static final int DEFAULT_LOCAL_NUMBER_JOB_MANAGER = 1;

	public static final String LOCAL_NUMBER_RESOURCE_MANAGER = "local.number-resourcemanager";

	public static final int DEFAULT_LOCAL_NUMBER_RESOURCE_MANAGER = 1;

	public static final String LOCAL_START_WEBSERVER = "local.start-webserver";

	// --------------------------- High Availability ---------------------------------

	@PublicEvolving
	public static String DEFAULT_HA_MODE = "none";

	/** Deprecated in favour of {@link #DEFAULT_HA_MODE} */
	@Deprecated
	public static String DEFAULT_RECOVERY_MODE = "standalone";

	/**
	 * Default port used by the job manager if not in standalone recovery mode. If <code>0</code>
	 * the OS picks a random port port.
	 */
	@PublicEvolving
	public static final String DEFAULT_HA_JOB_MANAGER_PORT = "0";

	/** Deprecated in favour of {@link #DEFAULT_HA_JOB_MANAGER_PORT} */
	@Deprecated
	public static final String DEFAULT_RECOVERY_JOB_MANAGER_PORT = "0";

	// --------------------------- ZooKeeper ----------------------------------

	public static final String DEFAULT_ZOOKEEPER_DIR_KEY = "/flink";

	public static final String DEFAULT_ZOOKEEPER_NAMESPACE_KEY = "/default";

	public static final String DEFAULT_ZOOKEEPER_LATCH_PATH = "/leaderlatch";

	public static final String DEFAULT_ZOOKEEPER_LEADER_PATH = "/leader";

	public static final String DEFAULT_ZOOKEEPER_JOBGRAPHS_PATH = "/jobgraphs";

	public static final String DEFAULT_ZOOKEEPER_CHECKPOINTS_PATH = "/checkpoints";

	public static final String DEFAULT_ZOOKEEPER_CHECKPOINT_COUNTER_PATH = "/checkpoint-counter";

	public static final String DEFAULT_ZOOKEEPER_MESOS_WORKERS_PATH = "/mesos-workers";

	public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 60000;

	public static final int DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT = 15000;

	public static final int DEFAULT_ZOOKEEPER_RETRY_WAIT = 5000;

	public static final int DEFAULT_ZOOKEEPER_MAX_RETRY_ATTEMPTS = 3;

	// - Defaults for required ZooKeeper configuration keys -------------------

	/** ZooKeeper default client port. */
	public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;

	/** ZooKeeper default init limit. */
	public static final int DEFAULT_ZOOKEEPER_INIT_LIMIT = 10;

	/** ZooKeeper default sync limit. */
	public static final int DEFAULT_ZOOKEEPER_SYNC_LIMIT = 5;

	/** ZooKeeper default peer port. */
	public static final int DEFAULT_ZOOKEEPER_PEER_PORT = 2888;

	/** ZooKeeper default leader port. */
	public static final int DEFAULT_ZOOKEEPER_LEADER_PORT = 3888;

	/** Defaults for ZK client security **/
	public static final boolean DEFAULT_ZOOKEEPER_SASL_DISABLE = true;

	/** ACL options supported "creator" or "open" */
	public static final String DEFAULT_HA_ZOOKEEPER_CLIENT_ACL = "open";

	// ----------------------------- Metrics ----------------------------

	/** The default number of measured latencies to maintain at each operator */
	public static final int DEFAULT_METRICS_LATENCY_HISTORY_SIZE = 128;

	// ----------------------------- Environment Variables ----------------------------

	/** The environment variable name which contains the location of the configuration directory */
	public static final String ENV_FLINK_CONF_DIR = "FLINK_CONF_DIR";

	/** The environment variable name which contains the location of the lib folder */
	public static final String ENV_FLINK_LIB_DIR = "FLINK_LIB_DIR";

	/** The environment variable name which contains the location of the bin directory */
	public static final String ENV_FLINK_BIN_DIR = "FLINK_BIN_DIR";

	/** The environment variable name which contains the Flink installation root directory */
	public static final String ENV_FLINK_HOME_DIR = "FLINK_HOME";

	// -------------------------------- Security -------------------------------

	/**
	 * The config parameter defining security credentials required
	 * for securing Flink cluster.
	 */

	/** Keytab file key name to be used in flink configuration file */
	public static final String SECURITY_KEYTAB_KEY = "security.keytab";

	/** Kerberos security principal key name to be used in flink configuration file */
	public static final String SECURITY_PRINCIPAL_KEY = "security.principal";


	/**
	 * Not instantiable.
	 */
	private ConfigConstants() {}
}
