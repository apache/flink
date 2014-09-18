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

package org.apache.flink.runtime.minicluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class LocalFlinkMiniCluster extends FlinkMiniCluster {
	private static final Logger LOG = LoggerFactory.getLogger(LocalFlinkMiniCluster.class);

	private Configuration configuration;
	private final String configDir;

	public LocalFlinkMiniCluster(String configDir){
		this.configDir = configDir;
		this.configuration = null;
	}

	// ------------------------------------------------------------------------
	// Life cycle and Job Submission
	// ------------------------------------------------------------------------

	public JobClient getJobClient(JobGraph jobGraph) throws Exception {
		if(configuration == null){
			throw new RuntimeException("The cluster has not been started yet.");
		}

		Configuration jobConfiguration = jobGraph.getJobConfiguration();
		int jobManagerRPCPort = getJobManagerRPCPort();
		jobConfiguration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, HOSTNAME);
		jobConfiguration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRPCPort);
		return new JobClient(jobGraph, jobConfiguration, getClass().getClassLoader());
	}

	public int getJobManagerRPCPort() {
		return configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);
	}

	public Configuration getConfiguration(final Configuration userConfiguration) {
		if(configuration == null){
			String forkNumberString = System.getProperty("forkNumber");
			int forkNumber = -1;
			try {
				forkNumber = Integer.parseInt(forkNumberString);
			} catch (NumberFormatException e) {
				// running inside and IDE, so the forkNumber property is not properly set
				// just ignore
			}

			if(configDir != null){
				GlobalConfiguration.loadConfiguration(configDir);
				configuration = GlobalConfiguration.getConfiguration();
			}else{
				configuration = getDefaultConfig(userConfiguration);
			}

			configuration.addAll(userConfiguration);

			if(forkNumber != -1) {

				int jobManagerRPC = 1024 + forkNumber * 300;
				int taskManagerRPC = 1024 + forkNumber * 300 + 100;
				int taskManagerDATA = 1024 + forkNumber * 300 + 200;

				configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerRPC);
				configuration.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, taskManagerRPC);
				configuration.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, taskManagerDATA);
			}

			initializeIOFormatClasses();
		}

		return configuration;
	}

	@Override
	public ActorRef startJobManager(final ActorSystem system, final Configuration configuration) {
		Configuration config = configuration.clone();

		return JobManager.startActorWithConfiguration(config, system);
	}

	@Override
	public ActorRef startTaskManager(final ActorSystem system, final Configuration configuration, final int index) {
		Configuration config = configuration.clone();

		int rpcPort = config.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT);
		int dataPort = config.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT);

		if(rpcPort > 0){
			config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, rpcPort + index);
		}

		if(dataPort > 0){
			config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, dataPort + index);
		}

		return TaskManager.startActorWithConfiguration(HOSTNAME, config, system);
	}

	private static void initializeIOFormatClasses() {
		try {
			Method im = FileInputFormat.class.getDeclaredMethod("initDefaultsFromConfiguration");
			im.setAccessible(true);
			im.invoke(null);

			Method om = FileOutputFormat.class.getDeclaredMethod("initDefaultsFromConfiguration");
			om.setAccessible(true);
			om.invoke(null);
		}
		catch (Exception e) {
			LOG.error("Cannot (re) initialize the globally loaded defaults. Some classes might mot follow the specified default behavior.");
		}
	}

	public static Configuration getDefaultConfig(final Configuration userConfiguration)
	{
		final Configuration config = new Configuration();

		// addresses and ports
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, HOSTNAME);
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
		config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT);
		config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT);

		config.setBoolean(ConfigConstants.TASK_MANAGER_MEMORY_LAZY_ALLOCATION_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_LAZY_ALLOCATION);

		// polling interval
		config.setInteger(ConfigConstants.JOBCLIENT_POLLING_INTERVAL_KEY, ConfigConstants.DEFAULT_JOBCLIENT_POLLING_INTERVAL);

		// file system behavior
		config.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, ConfigConstants.DEFAULT_FILESYSTEM_OVERWRITE);
		config.setBoolean(ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY,
				ConfigConstants.DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY);

		long memorySize = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag();

		// at this time, we need to scale down the memory, because we cannot dedicate all free memory to the
		// memory manager. we have to account for the buffer pools as well, and the job manager#s data structures
		long bufferMem = ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS * ConfigConstants
				.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE;

		int numTaskManager = userConfiguration.getInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER,
				1);
		int taskManagerNumSlots = userConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS,
				ConfigConstants.DEFAULT_TASK_MANAGER_NUM_TASK_SLOTS);

		memorySize = memorySize - (bufferMem * numTaskManager);

		// apply the fraction that makes sure memory is left to the heap for other data structures and UDFs.
		memorySize = (long) (memorySize * ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION);

		//convert from bytes to megabytes
		memorySize >>>= 20;


		memorySize /= numTaskManager;

		config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize);

		config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTaskManager);

		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, taskManagerNumSlots);

		return config;
	}
}
