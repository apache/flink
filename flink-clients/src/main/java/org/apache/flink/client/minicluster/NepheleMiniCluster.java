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

package org.apache.flink.client.minicluster;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.instance.InstanceManager;
import org.apache.flink.runtime.instance.LocalInstanceManager;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.messages.JobmanagerMessages;
import org.apache.flink.runtime.taskmanager.TaskManager;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.Future;


public class NepheleMiniCluster {
	
	private static final Logger LOG = LoggerFactory.getLogger(NepheleMiniCluster.class);
	
	private static final int DEFAULT_JM_RPC_PORT = 6498;
	
	private static final int DEFAULT_TM_RPC_PORT = 6501;
	
	private static final int DEFAULT_TM_DATA_PORT = 7501;
	
	private static final long DEFAULT_MEMORY_SIZE = -1;

	private static final int DEFAULT_NUM_TASK_MANAGER = 1;

	private static final boolean DEFAULT_LAZY_MEMORY_ALLOCATION = true;

	private static final int DEFAULT_TASK_MANAGER_NUM_SLOTS = 1;

	private static final String HOSTNAME = "localhost";

	// --------------------------------------------------------------------------------------------
	
	private final Object startStopLock = new Object();
	
	private int jobManagerRpcPort = DEFAULT_JM_RPC_PORT;
	
	private int taskManagerRpcPort = DEFAULT_TM_RPC_PORT;
	
	private int taskManagerDataPort = DEFAULT_TM_DATA_PORT;

	private int numTaskManager = DEFAULT_NUM_TASK_MANAGER;

	private int taskManagerNumSlots = DEFAULT_TASK_MANAGER_NUM_SLOTS;
	
	private long memorySize = DEFAULT_MEMORY_SIZE;
	
	private String configDir;

	private String hdfsConfigFile;
	
	private boolean lazyMemoryAllocation = DEFAULT_LAZY_MEMORY_ALLOCATION;
	
	private boolean defaultOverwriteFiles = false;
	
	private boolean defaultAlwaysCreateDirectory = false;

	
	private JobManager jobManager;

	// ------------------------------------------------------------------------
	//  Constructor and feature / properties setup
	// ------------------------------------------------------------------------

	public int getJobManagerRpcPort() {
		return jobManagerRpcPort;
	}
	
	public void setJobManagerRpcPort(int jobManagerRpcPort) {
		this.jobManagerRpcPort = jobManagerRpcPort;
	}

	public int getTaskManagerRpcPort() {
		return taskManagerRpcPort;
	}

	public void setTaskManagerRpcPort(int taskManagerRpcPort) {
		this.taskManagerRpcPort = taskManagerRpcPort;
	}

	public int getTaskManagerDataPort() {
		return taskManagerDataPort;
	}

	public void setTaskManagerDataPort(int taskManagerDataPort) {
		this.taskManagerDataPort = taskManagerDataPort;
	}
	
	public long getMemorySize() {
		return memorySize;
	}
	
	public void setMemorySize(long memorySize) {
		this.memorySize = memorySize;
	}
	
	public String getConfigDir() {
		return configDir;
	}

	public void setConfigDir(String configDir) {
		this.configDir = configDir;
	}

	public String getHdfsConfigFile() {
		return hdfsConfigFile;
	}
	
	public void setHdfsConfigFile(String hdfsConfigFile) {
		this.hdfsConfigFile = hdfsConfigFile;
	}
	
	public boolean isLazyMemoryAllocation() {
		return lazyMemoryAllocation;
	}
	
	public void setLazyMemoryAllocation(boolean lazyMemoryAllocation) {
		this.lazyMemoryAllocation = lazyMemoryAllocation;
	}
	
	public boolean isDefaultOverwriteFiles() {
		return defaultOverwriteFiles;
	}
	
	public void setDefaultOverwriteFiles(boolean defaultOverwriteFiles) {
		this.defaultOverwriteFiles = defaultOverwriteFiles;
	}
	
	public boolean isDefaultAlwaysCreateDirectory() {
		return defaultAlwaysCreateDirectory;
	}
	
	public void setDefaultAlwaysCreateDirectory(boolean defaultAlwaysCreateDirectory) {
		this.defaultAlwaysCreateDirectory = defaultAlwaysCreateDirectory;
	}

	public void setNumTaskManager(int numTaskManager) { this.numTaskManager = numTaskManager; }

	public int getNumTaskManager() { return numTaskManager; }

	public void setTaskManagerNumSlots(int taskManagerNumSlots) { this.taskManagerNumSlots = taskManagerNumSlots; }

	public int getTaskManagerNumSlots() { return taskManagerNumSlots; }

	// ------------------------------------------------------------------------
	// Life cycle and Job Submission
	// ------------------------------------------------------------------------
	
	public JobClient getJobClient(JobGraph jobGraph) throws Exception {
		Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, HOSTNAME);
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRpcPort);
		return new JobClient(jobGraph, configuration, getClass().getClassLoader());
	}

	public void start() throws Exception {

		String forkNumberString = System.getProperty("forkNumber");
		int forkNumber = -1;
		try {
			forkNumber = Integer.parseInt(forkNumberString);
		} catch (NumberFormatException e) {
			// running inside and IDE, so the forkNumber property is not properly set
			// just ignore
		}
		if (forkNumber != -1) {
			// we are running inside a surefire/failsafe test, determine forkNumber and set
			// ports accordingly so that we can have multiple parallel instances

			jobManagerRpcPort = 1024 + forkNumber * 300;
			taskManagerRpcPort = 1024 + forkNumber * 300 + 100;
			taskManagerDataPort = 1024 + forkNumber * 300 + 200;
		}

		synchronized (startStopLock) {
			// set up the global configuration
			if (this.configDir != null) {
				GlobalConfiguration.loadConfiguration(configDir);
			} else {
				Configuration conf = getMiniclusterDefaultConfig(jobManagerRpcPort, taskManagerRpcPort,
					taskManagerDataPort, memorySize, hdfsConfigFile, lazyMemoryAllocation, defaultOverwriteFiles,
						defaultAlwaysCreateDirectory, taskManagerNumSlots, numTaskManager);
				GlobalConfiguration.includeConfiguration(conf);
			}

			// force the input/output format classes to load the default values from the configuration.
			// we need to do this here, because the format classes may have been initialized before the mini cluster was started
			initializeIOFormatClasses();

			Configuration configuration = GlobalConfiguration.getConfiguration();
			
			// start the job manager
			jobManager = JobManager.startActorSystemAndActor("flink", HOSTNAME, jobManagerRpcPort, "jobmanager",
					configuration);

			int tmRPCPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT);
			int tmDataPort = GlobalConfiguration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT);

			for(int i = 0; i < numTaskTracker; i++){
				Configuration tmConfiguration = GlobalConfiguration.getConfiguration();
				tmConfiguration.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, tmRPCPort + i);
				tmConfiguration.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, tmDataPort + i);
				ActorSystem taskManager = TaskManager.startActorSystemAndActor("flink", HOSTNAME, tmRPCPort+i,
						"taskmanager" + (i+1), configuration);
				taskManagers.add(taskManager);
			}

			// start the job manager
			jobManager = new JobManager(ExecutionMode.LOCAL);
	
			waitForJobManagerToBecomeReady(taskManagerNumSlots * numTaskManager);
		}
	}

	public void stop() throws Exception {
		synchronized (this.startStopLock) {
			if (jobManager != null) {
				jobManager.shutdown();
				jobManager = null;
			}
		}
	}
	
	public TaskManager[] getTaskManagers() {
		JobManager jm = this.jobManager;
		if (jm != null) {
			InstanceManager im = jm.getInstanceManager();
			if (im instanceof LocalInstanceManager) {
				return ((LocalInstanceManager) im).getTaskManagers();
			}
		}
		
		return null;
	}

	// ------------------------------------------------------------------------
	// Network utility methods
	// ------------------------------------------------------------------------
	
	private void waitForJobManagerToBecomeReady(int numTaskManagers) throws Exception {
		boolean notReady = true;

		ActorSelection jobmanagerSelection = jobManager.actorSelection("jobmanager");
		Timeout timeout = new Timeout(1L, TimeUnit.MINUTES);

		while(notReady){
			Future<Object> futureNumTaskManagers = Patterns.ask(jobmanagerSelection,
					JobmanagerMessages.RequestNumberRegisteredTaskManager$.MODULE$, timeout);

			int numRegisteredTaskManagers = (Integer)Await.result(futureNumTaskManagers, timeout.duration());

			if(numRegisteredTaskManagers < numTaskManagers){
				Thread.sleep(50);
		}
		
		// make sure that not just the jobmanager has the slots, but also the taskmanager
		// has figured out its registration. under rare races, calls can be scheduled before that otherwise
		TaskManager[] tms = getTaskManagers();
		for (TaskManager tm : tms) {
			while (tm.getRegisteredId() == null) {
				Thread.sleep(10);
			}
		}
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
	
	public static Configuration getMiniclusterDefaultConfig(int jobManagerRpcPort, int taskManagerRpcPort,
			int taskManagerDataPort, long memorySize, String hdfsConfigFile, boolean lazyMemory,
			boolean defaultOverwriteFiles, boolean defaultAlwaysCreateDirectory,
			int taskManagerNumSlots, int numTaskManager)
	{
		final Configuration config = new Configuration();
		
		// addresses and ports
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRpcPort);
		config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, taskManagerRpcPort);
		config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, taskManagerDataPort);
		
		// with the low dop, we can use few RPC handlers
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_HANDLERS_KEY, 2);
		
		config.setBoolean(ConfigConstants.TASK_MANAGER_MEMORY_LAZY_ALLOCATION_KEY, lazyMemory);
		
		// polling interval
		config.setInteger(ConfigConstants.JOBCLIENT_POLLING_INTERVAL_KEY, 2);
		
		// hdfs
		if (hdfsConfigFile != null) {
			config.setString(ConfigConstants.HDFS_DEFAULT_CONFIG, hdfsConfigFile);
		}
		
		// file system behavior
		config.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, defaultOverwriteFiles);
		config.setBoolean(ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY, defaultAlwaysCreateDirectory);

		if (memorySize < 0){
			memorySize = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag();

			// at this time, we need to scale down the memory, because we cannot dedicate all free memory to the
			// memory manager. we have to account for the buffer pools as well, and the job manager#s data structures
			long bufferMem = GlobalConfiguration.getLong(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS) *
					GlobalConfiguration.getLong(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
							ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE);

			memorySize = memorySize - (bufferMem * numTaskManager);
			
			// apply the fraction that makes sure memory is left to the heap for other data structures and UDFs.
			memorySize = (long) (memorySize * ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION);

			//convert from bytes to megabytes
			memorySize >>>= 20;
		}

		memorySize /= numTaskManager;

		config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize);

		config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTaskManager);

		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, taskManagerNumSlots);
		
		return config;
	}
}
