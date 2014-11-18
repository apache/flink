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
	 * The config parameter defining the default degree of parallelism for jobs.
	 */
	public static final String DEFAULT_PARALLELIZATION_DEGREE_KEY = "parallelization.degree.default";
	
	/**
	 * Config parameter for the number of re-tries for failed tasks. Setting this
	 * value to 0 effectively disables fault tolerance.
	 */
	public static final String DEFAULT_EXECUTION_RETRIES_KEY = "execution-retries.default";
	
	// -------------------------------- Runtime -------------------------------

	/**
	 * The config parameter defining the storage directory to be used by the blob server.
	 */
	public static final String BLOB_STORAGE_DIRECTORY_KEY = "blob.storage.directory";

	/**
	 * The config parameter defining the cleanup interval of the library cache manager.
	 */
	public static final String LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL = "library-cache-manager" +
			".cleanup.interval";
	
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
	 * The config parameter defining the number of handler threads for the jobmanager RPC service.
	 */
	public static final String JOB_MANAGER_IPC_HANDLERS_KEY = "jobmanager.rpc.numhandler";

	/**
	 * The config parameter defining the number of seconds that a task manager heartbeat may be missing before it is
	 * marked as failed.
	 */
	public static final String JOB_MANAGER_DEAD_TASKMANAGER_TIMEOUT_KEY = "jobmanager.max-heartbeat-delay-before-failure.msecs";
	
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
	 * The key for the config parameter defining whether the memory manager allocates memory lazy.
	 */
	public static final String TASK_MANAGER_MEMORY_LAZY_ALLOCATION_KEY = "taskmanager.memory.lazyalloc";
	
	
	/**
	 * The config parameter defining the number of buffers used in the network stack. This defines the
	 * number of possible tasks and shuffles.
	 */
	public static final String TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY = "taskmanager.network.numberOfBuffers";

	/**
	 * The config parameter defining the size of the buffers used in the network stack.
	 */
	public static final String TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY = "taskmanager.network.bufferSizeInBytes";

	/**
	 * The config parameter defining the number of task slots of a task manager.
	 */
	public static final String TASK_MANAGER_NUM_TASK_SLOTS = "taskmanager.numberOfTaskSlots";

	/**
	 * The number of incoming network IO threads (e.g. incoming connection threads used in NettyConnectionManager
	 * for the ServerBootstrap.)
	 */
	public static final String TASK_MANAGER_NET_NUM_IN_THREADS_KEY = "taskmanager.net.numInThreads";

	/**
	 * The number of outgoing network IO threads (e.g. outgoing connection threads used in NettyConnectionManager for
	 * the Bootstrap.)
	 */
	public static final String TASK_MANAGER_NET_NUM_OUT_THREADS_KEY = "taskmanager.net.numOutThreads";

	/**
	 * The low water mark used in NettyConnectionManager for the Bootstrap.
	 */
	public static final String TASK_MANAGER_NET_NETTY_LOW_WATER_MARK = "taskmanager.net.nettyLowWaterMark";

	/**
	 * The high water mark used in NettyConnectionManager for the Bootstrap.
	 */
	public static final String TASK_MANAGER_NET_NETTY_HIGH_WATER_MARK = "taskmanager.net.nettyHighWaterMark";
	
	/**
	 * Parameter for the interval in which the TaskManager sends the periodic heart beat messages
	 * to the JobManager (in msecs).
	 */
	public static final String TASK_MANAGER_HEARTBEAT_INTERVAL_KEY = "taskmanager.heartbeat-interval";

	/**
	 * Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.
	 */
	public static final String TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD = "taskmanager.debug.memory.startLogThread";

	/**
	 * The interval (in ms) for the log thread to log the current memory usage.
	 */
	public static final String TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS = "taskmanager.debug.memory.logIntervalMs";

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
	 * The config parameter defining the timeout for filesystem stream opening.
	 * A value of 0 indicates infinite waiting.
	 */
	public static final String FS_STREAM_OPENING_TIMEOUT_KEY = "taskmanager.runtime.fs_timeout";
	
	/**
	 * The parameter defining the polling interval (in seconds) for the JobClient.
	 */
	public static final String JOBCLIENT_POLLING_INTERVAL_KEY = "jobclient.polling.interval";

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
	
	// ------------------------ File System Bahavior ------------------------

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
	 * The port for the pact web-frontend server.
	 */
	public static final String JOB_MANAGER_WEB_PORT_KEY = "jobmanager.web.port";

	/**
	 * The parameter defining the directory containing the web documents for the jobmanager web frontend.
	 */
	public static final String JOB_MANAGER_WEB_ROOT_PATH_KEY = "jobmanager.web.rootpath";

	/**
	 * The config parameter defining the path to the htaccess file protecting the web frontend.
	 */
	public static final String JOB_MANAGER_WEB_ACCESS_FILE_KEY = "jobmanager.web.access";
	
	/**
	 * The config parameter defining the number of archived jobs for the jobmanager
	 */
	public static final String JOB_MANAGER_WEB_ARCHIVE_COUNT = "jobmanager.web.history";
	
	public static final String JOB_MANAGER_WEB_LOG_PATH_KEY = "jobmanager.web.logpath";
	
	
	// ------------------------------ Web Client ------------------------------
	
	/**
	 * The config parameter defining port for the pact web-frontend server.
	 */
	public static final String WEB_FRONTEND_PORT_KEY = "webclient.port";

	/**
	 * The config parameter defining the directory containing the web documents.
	 */
	public static final String WEB_ROOT_PATH_KEY = "webclient.rootpath";

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
	
	// ----------------------------- YARN Client ----------------------------
	
	public static final String YARN_AM_PRC_PORT = "yarn.am.rpc.port";
	
	// ----------------------------- Miscellaneous ----------------------------
	
	/**
	 * The key to the Flink base directory path
	 */
	public static final String FLINK_BASE_DIR_PATH_KEY = "flink.base.dir.path";
	
	public static final String FLINK_JVM_OPTIONS = "env.java.opts";

	// ------------------------------------------------------------------------
	//                            Default Values
	// ------------------------------------------------------------------------

	// ---------------------------- Parallelism -------------------------------
	
	/**
	 * The default degree of parallelism for operations.
	 */
	public static final int DEFAULT_PARALLELIZATION_DEGREE = 1;
	
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
	 * The default number of handler threads for the jobmanager RPC service.
	 */
	public static final int DEFAULT_JOB_MANAGER_IPC_HANDLERS = 8;
	
	/**
	 * Default number of seconds after which a task manager is marked as failed.
	 */
	// 30 seconds (its enough to get to mars, should be enough to detect failure)
	public static final int DEFAULT_JOB_MANAGER_DEAD_TASKMANAGER_TIMEOUT = 30*1000;
	
	/**
	 * The default network port the task manager expects incoming IPC connections. The {@code -1} means that
	 * the TaskManager searches for a free port.
	 */
	public static final int DEFAULT_TASK_MANAGER_IPC_PORT = -1;

	/**
	 * The default network port the task manager expects to receive transfer envelopes on. The {@code -1} means that
	 * the TaskManager searches for a free port.
	 */
	public static final int DEFAULT_TASK_MANAGER_DATA_PORT = -1;

	/**
	 * The default directory for temporary files of the task manager.
	 */
	public static final String DEFAULT_TASK_MANAGER_TMP_PATH = System.getProperty("java.io.tmpdir");
	
	/**
	 * The default fraction of the free memory allocated by the task manager's memory manager.
	 */
	public static final float DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION = 0.7f;
	
	/**
	 * The default setting for the memory manager lazy allocation feature.
	 */
	public static final boolean DEFAULT_TASK_MANAGER_MEMORY_LAZY_ALLOCATION = false;

	/**
	 * Default number of buffers used in the network stack.
	 */
	public static final int DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS = 2048;

	/**
	 * Default size of network stack buffers.
	 */
	public static final int DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE = 32768;

	/**
	 * Default number of incoming network IO threads (e.g. number of incoming connection threads used in
	 * NettyConnectionManager for the ServerBootstrap). If set to -1, a reasonable default depending on the number of
	 * cores will be picked.
	 */
	public static final int DEFAULT_TASK_MANAGER_NET_NUM_IN_THREADS = -1;

	/**
	 * Default number of outgoing network IO threads (e.g. number of outgoing connection threads used in
	 * NettyConnectionManager for the Bootstrap). If set to -1, a reasonable default depending on the number of cores
	 * will be picked.
	 */
	public static final int DEFAULT_TASK_MANAGER_NET_NUM_OUT_THREADS = -1;

	/**
	 * Default low water mark used in NettyConnectionManager for the Bootstrap. If set to -1, NettyConnectionManager
	 * will use half of the network buffer size as the low water mark.
	 */
	public static final int DEFAULT_TASK_MANAGER_NET_NETTY_LOW_WATER_MARK = -1;

	/**
	 * Default high water mark used in NettyConnectionManager for the Bootstrap. If set to -1, NettyConnectionManager
	 * will use the network buffer size as the high water mark.
	 */
	public static final int DEFAULT_TASK_MANAGER_NET_NETTY_HIGH_WATER_MARK = -1;

	/**
	 * The default interval for TaskManager heart beats (5000 msecs).
	 */
	public static final int DEFAULT_TASK_MANAGER_HEARTBEAT_INTERVAL = 5000;

	/**
	 * Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.
	 */
	public static final boolean DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD = false;

	/**
	 * The interval (in ms) for the log thread to log the current memory usage.
	 */
	public static final int DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS = 5000;
	
	/**
	 * The default value for the JobClient's polling interval. 2 Seconds.
	 */
	public static final int DEFAULT_JOBCLIENT_POLLING_INTERVAL = 2;
	
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
	
	
	// ------------------------ File System Behavior ------------------------

	/**
	 * The default behavior with respect to overwriting existing files (= not overwrite)
	 */
	public static final boolean DEFAULT_FILESYSTEM_OVERWRITE = false;

	/**
	 * The default behavior for output directory creating (create only directory when parallelism > 1).
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
	 */
	public static final int DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT = 8081;

	/**
	 * The default directory name of the info server
	 */
	public static final String DEFAULT_JOB_MANAGER_WEB_PATH_NAME = "web-docs-infoserver";
	
	/**
	 * The default path of the directory for info server containing the web documents.
	 */
	public static final String DEFAULT_JOB_MANAGER_WEB_ROOT_PATH = "./resources/"+DEFAULT_JOB_MANAGER_WEB_PATH_NAME+"/";
	
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
	 * The default path of the directory containing the web documents.
	 */
	public static final String DEFAULT_WEB_ROOT_DIR = "./resources/web-docs/";

	/**
	 * The default directory to store temporary objects (e.g. during file uploads).
	 */
	public static final String DEFAULT_WEB_TMP_DIR = System.getProperty("java.io.tmpdir") == null ? "/tmp" : System
		.getProperty("java.io.tmpdir");

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
	
	// ----------------------------- YARN ----------------------------
	
	public static final int DEFAULT_YARN_AM_RPC_PORT = 10245;
	

	// ----------------------------- LocalExecution ----------------------------

	/**
	 * Sets the number of local task managers
	 */
	public static final String LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER = "localinstancemanager.numtaskmanager";
	
	
	// ----------------------------- Deprecated --------------------------------

	/**
	 * The default definition for an instance type, if no other configuration is provided.
	 */
	public static final String DEFAULT_INSTANCE_TYPE = "default,1,1,1,1,0"; // minimalistic instance type until "cloud" model is fully removed.

	/**
	 * The default index for the default instance type.
	 */
	public static final int DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX = 1;

	// ------------------------------------------------------------------------
	
	/**
	 * Not instantiable.
	 */
	private ConfigConstants() {}
}
