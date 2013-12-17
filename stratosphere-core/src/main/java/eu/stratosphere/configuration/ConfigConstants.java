/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.configuration;

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
	 * The config parameter defining the maximal intra-node parallelism for jobs.
	 */
	public static final String PARALLELIZATION_MAX_INTRA_NODE_DEGREE_KEY = "parallelization.intra-node.default";

	
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
	 * The config parameter defining the number of handler threads for the jobmanager RPC service.
	 */
	public static final String JOB_MANAGER_IPC_HANDLERS_KEY = "jobmanager.rpc.numhandler";

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
	 * The config parameter defining the number of buffers used in the network stack. This defines the
	 * number of possible tasks and shuffles.
	 */
	public static final String TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY = "taskmanager.network.numberOfBuffers";

	/**
	 * The config parameter defining the size of the buffers used in the network stack.
	 */
	public static final String TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY = "taskmanager.network.bufferSizeInBytes";
	
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
	 * The config parameter defining whether to use the special multicast logic
	 * for broadcasts. Use with caution! The multicast logic is experimental at this point.
	 */
	public static final String USE_MULTICAST_FOR_BROADCAST = "taskmanager.runtime.multicast-for-broadcast";
	
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
	
	// ----------------------------- Miscellaneous ----------------------------
	
	/**
	 * The key for Stratosphere's base directory path
	 */
	public static final String STRATOSPHERE_BASE_DIR_PATH_KEY = "stratosphere.base.dir.path";

	
	
	// ------------------------------------------------------------------------
	//                            Default Values
	// ------------------------------------------------------------------------

	// ---------------------------- Parallelism -------------------------------
	
	/**
	 * The default degree of parallelism for operations.
	 */
	public static final int DEFAULT_PARALLELIZATION_DEGREE = -1;

	/**
	 * The default intra-node parallelism.
	 */
	public static final int DEFAULT_MAX_INTRA_NODE_PARALLELIZATION_DEGREE = -1;
	
	// ------------------------------ Runtime ---------------------------------
	
	/**
	 * The default network port to connect to for communication with the job manager.
	 */
	public static final int DEFAULT_JOB_MANAGER_IPC_PORT = 6123;

	/**
	 * The default number of handler threads for the jobmanager RPC service.
	 */
	public static final int DEFAULT_JOB_MANAGER_IPC_HANDLERS = 8;
	
	/**
	 * The default network port the task manager expects incoming IPC connections.
	 */
	public static final int DEFAULT_TASK_MANAGER_IPC_PORT = 6122;

	/**
	 * The default network port the task manager expects to receive transfer envelopes on.
	 */
	public static final int DEFAULT_TASK_MANAGER_DATA_PORT = 6121;

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
	 * Default size of network stack buffers.
	 */
	public static final int DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE = 32768;
	
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
	
	/**
	 * The config parameter defining whether to use the special multicast logic
	 * for broadcasts.
	 */
	public static final boolean DEFAULT_USE_MULTICAST_FOR_BROADCAST = false;
	
	
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
	
	
	// ----------------------------- Deprecated --------------------------------

	/**
	 * The default definition for an instance type, if no other configuration is provided.
	 */
	public static final String DEFAULT_INSTANCE_TYPE = "default,2,1,1024,10,10";

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
