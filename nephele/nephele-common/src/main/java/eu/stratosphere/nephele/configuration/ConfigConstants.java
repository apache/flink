/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.configuration;

/**
 * This class contains all constants for the configuration. That includes the configuration keys and
 * the default values.
 */
public final class ConfigConstants {

	// ------------------------------------------------------------------------
	// Configuration Keys
	// ------------------------------------------------------------------------

	// -------------------------- Addresses and Ports -------------------------

	/**
	 * The key for the config parameter defining the network address to connect to
	 * for communication with the job manager.
	 */
	public static final String JOB_MANAGER_IPC_ADDRESS_KEY = "jobmanager.rpc.address";

	/**
	 * The key for the config parameter defining the network port to connect to
	 * for communication with the job manager.
	 */
	public static final String JOB_MANAGER_IPC_PORT_KEY = "jobmanager.rpc.port";

	/**
	 * The key for the parameter defining the task manager's IPC port from the configuration.
	 */
	public static final String TASK_MANAGER_IPC_PORT_KEY = "taskmanager.rpc.port";

	/**
	 * The key for the config parameter defining the task manager's data port from the configuration.
	 */
	public static final String TASK_MANAGER_DATA_PORT_KEY = "taskmanager.data.port";

	/**
	 * The key for the config parameter defining the directories for temporary files.
	 */
	public static final String TASK_MANAGER_TMP_DIR_KEY = "taskmanager.tmp.dirs";
	
	/**
	 * The key for the config parameter defining the default number of retries for failed tasks.
	 */
	public static final String JOB_EXECUTION_RETRIES_KEY = "job.execution.retries";

	/**
	 * The key for the config parameter defining the amount of memory available for the task manager's
	 * memory manager (in megabytes).
	 */
	public static final String MEMORY_MANAGER_AVAILABLE_MEMORY_SIZE_KEY = "taskmanager.memory.size";

	/**
	 * The key defining the amount polling interval (in seconds) for the JobClient.
	 */
	public static final String JOBCLIENT_POLLING_INTERVAL_KEY = "jobclient.polling.internval";
	
	/**
	 * The key for the config parameter defining flag to terminate a job at job-client shutdown.
	 */
	public static final String JOBCLIENT_SHUTDOWN_TERMINATEJOB_KEY = "jobclient.shutdown.terminatejob";

	// ------------------------------------------------------------------------
	// Default Values
	// ------------------------------------------------------------------------

	/**
	 * The default network port to connect to for communication with the job manager.
	 */
	public static final int DEFAULT_JOB_MANAGER_IPC_PORT = 6123;

	/**
	 * The default network port the task manager expects incoming IPC connections.
	 */
	public static final int DEFAULT_TASK_MANAGER_IPC_PORT = 6122;

	/**
	 * The default network port the task manager expects to receive transfer envelopes on.
	 */
	public static final int DEFAULT_TASK_MANAGER_DATA_PORT = 6121;

	/**
	 * The default amount of memory assigned to each task manager (in megabytes).
	 */
	public static final int DEFAULT_MEMORY_MANAGER_AVAILABLE_MEMORY = 512;
	
	/**
	 * The default number of retries for failed tasks.
	 */
	public static final int DEFAULT_JOB_EXECUTION_RETRIES = 0;

	/**
	 * The default minimal amount of memory that the memory manager does not occupy (in megabytes).
	 */
	public static final long DEFAULT_MEMORY_MANAGER_MIN_UNRESERVED_MEMORY = 256 * 1024 * 1024;

	/**
	 * The default directory for temporary files of the task manager.
	 */
	public static final String DEFAULT_TASK_MANAGER_TMP_PATH = System.getProperty("java.io.tmpdir");

	/**
	 * The default value for the JobClient's polling interval. 5 Seconds.
	 */
	public static final int DEFAULT_JOBCLIENT_POLLING_INTERVAL = 5;
	
	/**
	 * The default value for the flag to terminate a job on job-client shutdown.
	 */
	public static final boolean DEFAULT_JOBCLIENT_SHUTDOWN_TERMINATEJOB = true;

	// ----------------------------- Instances --------------------------------

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
	 * Private default constructor to prevent instantiation.
	 */
	private ConfigConstants() {
	}
}
