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

/**
 * This class contains all constants for the configuration. That includes the configuration keys and
 * the default values.
 */
public final class ConfigConstants {

	// ------------------------------------------------------------------------
	//                            Configuration Keys
	// ------------------------------------------------------------------------
	
	// ---------------------------- Parallelism -------------------------------

	/**
	 * The config parameter defining the default parallelism for jobs.
	 */
	public static final String DEFAULT_PARALLELISM_KEY = "parallelism.default";

	/**
	 * Config parameter for the number of re-tries for failed tasks. Setting this
	 * value to 0 effectively disables fault tolerance.
	 */
	public static final String DEFAULT_EXECUTION_RETRIES_KEY = "execution-retries.default";

	/**
	 * Config parameter for the delay between execution retries. The value must be specified in the
	 * notation "10 s" or "1 min" (style of Scala Finite Durations)
	 */
	public static final String DEFAULT_EXECUTION_RETRY_DELAY_KEY = "execution-retries.delay";
	
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
	 * The config parameter defining the directories for temporary files.
	 */
	public static final String TASK_MANAGER_TMP_DIR_KEY = "taskmanager.tmp.dirs";

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
	 *
	 */
	public static final String TASK_MANAGER_MAX_REGISTRATION_DURATION = "taskmanager.maxRegistrationDuration";

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

	// ------------------------ YARN Configuration ------------------------

	/**
	 * Percentage of heap space to remove from containers started by YARN.
	 */
	public static final String YARN_HEAP_CUTOFF_RATIO = "yarn.heap-cutoff-ratio";

	/**
	 * Minimum amount of memory to remove from the heap space as a safety margin.
	 */
	public static final String YARN_HEAP_CUTOFF_MIN = "yarn.heap-cutoff-min";

	/**
	 * Reallocate failed YARN containers.
	 */
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
	 */
	public static final String YARN_APPLICATION_MASTER_ENV_PREFIX = "yarn.application-master.env.";

	/**
	 * Similar to the {@see YARN_APPLICATION_MASTER_ENV_PREFIX}, this configuration prefix allows
	 * setting custom environment variables.
	 */
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
	 * The config parameter defining the number of archived jobs for the jobmanager
	 */
	public static final String JOB_MANAGER_WEB_ARCHIVE_COUNT = "jobmanager.web.history";

	/**
	 * The log file location (may be in /log for standalone but under log directory when using YARN)
	 */
	public static final String JOB_MANAGER_WEB_LOG_PATH_KEY = "jobmanager.web.log.path";


	// ------------------------------ Web Client ------------------------------

	/**
	 * The config parameter defining port for the pact web-frontend server.
	 */
	public static final String WEB_FRONTEND_PORT_KEY = "webclient.port";

	/**
	 * The config parameter defining the temporary data directory for the web client.
	 */
	public static final String WEB_TMP_DIR_KEY = "webclient.tempdir";

	/**
	 * The config parameter defining the directory that programs are uploaded to.
	 */
	public static final String WEB_JOB_UPLOAD_DIR_KEY = "webclient.uploaddir";

	/**
	 * The config parameter defining the directory that JSON plan dumps are written to.
	 */
	public static final String WEB_PLAN_DUMP_DIR_KEY = "webclient.plandump";

	/**
	 * The config parameter defining the port to the htaccess file protecting the web client.
	 */
	public static final String WEB_ACCESS_FILE_KEY = "webclient.access";
	

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
	 * Timeout for all blocking calls
	 */
	public static final String AKKA_ASK_TIMEOUT = "akka.ask.timeout";

	/**
	 * Timeout for all blocking calls that look up remote actors
	 */
	public static final String AKKA_LOOKUP_TIMEOUT = "akka.lookup.timeout";

	/**
	 * Exit JVM on fatal Akka errors
	 */
	public static final String AKKA_JVM_EXIT_ON_FATAL_ERROR = "akka.jvm-exit-on-fatal-error";
	
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

	// --------------------------- Recovery -----------------------------------

	/** Defines recovery mode used for the cluster execution ("standalone", "zookeeper") */
	public static final String RECOVERY_MODE = "recovery.mode";

	// --------------------------- ZooKeeper ----------------------------------

	/** ZooKeeper servers. */
	public static final String ZOOKEEPER_QUORUM_KEY = "recovery.zookeeper.quorum";

	/**
	 * File system state backend base path for recoverable state handles. Recovery state is written
	 * to this path and the file state handles are persisted for recovery.
	 */
	public static final String ZOOKEEPER_RECOVERY_PATH = "recovery.zookeeper.storageDir";

	/** ZooKeeper root path. */
	public static final String ZOOKEEPER_DIR_KEY = "recovery.zookeeper.path.root";

	public static final String ZOOKEEPER_LATCH_PATH = "recovery.zookeeper.path.latch";

	public static final String ZOOKEEPER_LEADER_PATH = "recovery.zookeeper.path.leader";

	/** ZooKeeper root path (ZNode) for job graphs. */
	public static final String ZOOKEEPER_JOBGRAPHS_PATH = "recovery.zookeeper.path.jobgraphs";

	/** ZooKeeper root path (ZNode) for completed checkpoints. */
	public static final String ZOOKEEPER_CHECKPOINTS_PATH = "recovery.zookeeper.path.checkpoints";

	/** ZooKeeper root path (ZNode) for checkpoint counters. */
	public static final String ZOOKEEPER_CHECKPOINT_COUNTER_PATH = "recovery.zookeeper.path.checkpoint-counter";

	public static final String ZOOKEEPER_SESSION_TIMEOUT = "recovery.zookeeper.client.session-timeout";

	public static final String ZOOKEEPER_CONNECTION_TIMEOUT = "recovery.zookeeper.client.connection-timeout";

	public static final String ZOOKEEPER_RETRY_WAIT = "recovery.zookeeper.client.retry-wait";

	public static final String ZOOKEEPER_MAX_RETRY_ATTEMPTS = "recovery.zookeeper.client.max-retry-attempts";

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
	 * The default setting for TaskManager memory eager allocation of managed memory
	 */
	public static final boolean DEFAULT_TASK_MANAGER_MEMORY_PRE_ALLOCATE = false;

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

	// ------------------------ YARN Configuration ------------------------

	/**
	 * Minimum amount of Heap memory to subtract from the requested TaskManager size.
	 * We came up with these values experimentally.
	 * Flink fails when the cutoff is set only to 500 mb.
	 */
	public static final int DEFAULT_YARN_MIN_HEAP_CUTOFF = 600;

	/**
	 * Relative amount of memory to subtract from the requested memory.
	 */
	public static final float DEFAULT_YARN_HEAP_CUTOFF_RATIO = 0.25f;

	/**
	 * Default port for the application master is 0, which means
	 * the operating system assigns an ephemeral port
	 */
	public static final String DEFAULT_YARN_APPLICATION_MASTER_PORT = "0";
	
	
	// ------------------------ File System Behavior ------------------------

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
	
	/**
	 * The config key for the port of the JobManager web frontend.
	 * Setting this value to {@code -1} disables the web frontend.
	 */
	public static final int DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT = 8081;
	
	/**
	 * The default number of archived jobs for the jobmanager
	 */
	public static final int DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT = 5;
	
	
	// ------------------------------ Web Client ------------------------------
	
	/**
	 * The default port to launch the web frontend server on.
	 */
	public static final int DEFAULT_WEBCLIENT_PORT = 8080;

	/**
	 * The default directory to store temporary objects (e.g. during file uploads).
	 */
	public static final String DEFAULT_WEB_TMP_DIR = 
			System.getProperty("java.io.tmpdir") == null ? "/tmp" : System.getProperty("java.io.tmpdir");

	/**
	 * The default directory for temporary plan dumps from the web frontend.
	 */
	public static final String DEFAULT_WEB_PLAN_DUMP_DIR = DEFAULT_WEB_TMP_DIR + "/webclient-plans/";

	/**
	 * The default directory to store uploaded jobs in.
	 */
	public static final String DEFAULT_WEB_JOB_STORAGE_DIR = DEFAULT_WEB_TMP_DIR + "/webclient-jobs/";
	

	/**
	 * The default path to the file containing the list of access privileged users and passwords.
	 */
	public static final String DEFAULT_WEB_ACCESS_FILE_PATH = null;

	// ------------------------------ Akka Values ------------------------------

	public static String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL = "1000 s";

	public static String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE = "6000 s";

	public static double DEFAULT_AKKA_TRANSPORT_THRESHOLD = 300.0;

	public static double DEFAULT_AKKA_WATCH_THRESHOLD = 12;

	public static int DEFAULT_AKKA_DISPATCHER_THROUGHPUT = 15;

	public static boolean DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS = false;

	public static String DEFAULT_AKKA_FRAMESIZE = "10485760b";

	public static String DEFAULT_AKKA_ASK_TIMEOUT = "100 s";

	public static String DEFAULT_AKKA_LOOKUP_TIMEOUT = "10 s";
	
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

	public static final String LOCAL_START_WEBSERVER = "local.start-webserver";

  	// --------------------------- Recovery ---------------------------------

	public static String DEFAULT_RECOVERY_MODE = "standalone";

	// --------------------------- ZooKeeper ----------------------------------

	public static final String DEFAULT_ZOOKEEPER_DIR_KEY = "/flink";

	public static final String DEFAULT_ZOOKEEPER_LATCH_PATH = "/leaderlatch";

	public static final String DEFAULT_ZOOKEEPER_LEADER_PATH = "/leader";

	public static final String DEFAULT_ZOOKEEPER_JOBGRAPHS_PATH = "/jobgraphs";

	public static final String DEFAULT_ZOOKEEPER_CHECKPOINTS_PATH = "/checkpoints";

	public static final String DEFAULT_ZOOKEEPER_CHECKPOINT_COUNTER_PATH = "/checkpoint-counter";

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

	/**
	 * Not instantiable.
	 */
	private ConfigConstants() {}
}
