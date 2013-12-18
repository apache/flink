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

package eu.stratosphere.client.minicluster;

import java.util.Map;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.JobManager.ExecutionMode;


public class NepheleMiniCluster {
	
	private static final int DEFAULT_JM_RPC_PORT = 6498;
	
	private static final int DEFAULT_TM_RPC_PORT = 6501;
	
	private static final int DEFAULT_TM_DATA_PORT = 7501;
	
	private static final boolean DEFAULT_VISUALIZER_ENABLED = true;

	// --------------------------------------------------------------------------------------------
	
	private final Object startStopLock = new Object();
	
	private int jobManagerRpcPort = DEFAULT_JM_RPC_PORT;
	
	private int taskManagerRpcPort = DEFAULT_TM_RPC_PORT;
	
	private int taskManagerDataPort = DEFAULT_TM_DATA_PORT;
	
	private String configDir;

	private String hdfsConfigFile;

	private boolean visualizerEnabled = DEFAULT_VISUALIZER_ENABLED;

	
	private Thread runner;

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
	
	public boolean isVisualizerEnabled() {
		return visualizerEnabled;
	}
	
	public void setVisualizerEnabled(boolean visualizerEnabled) {
		this.visualizerEnabled = visualizerEnabled;
	}
	
	
	// ------------------------------------------------------------------------
	// Life cycle and Job Submission
	// ------------------------------------------------------------------------
	
	public JobClient getJobClient(JobGraph jobGraph) throws Exception {
		Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRpcPort);
		return new JobClient(jobGraph, configuration);
	}
	
	public void start() throws Exception {
		synchronized (startStopLock) {
			// set up the global configuration
			if (this.configDir != null) {
				GlobalConfiguration.loadConfiguration(configDir);
			} else {
				Configuration conf = getMiniclusterDefaultConfig(jobManagerRpcPort, taskManagerRpcPort,
					taskManagerDataPort, hdfsConfigFile, visualizerEnabled);
				GlobalConfiguration.includeConfiguration(conf);
			}
			
			// before we start the jobmanager, we need to make sure that there are no lingering IPC threads from before
			// check that all threads are done before we return
			Thread[] allThreads = new Thread[Thread.activeCount()];
			int numThreads = Thread.enumerate(allThreads);
			
			for (int i = 0; i < numThreads; i++) {
				Thread t = allThreads[i];
				String name = t.getName();
				if (name.equals("Local Taskmanager IO Loop") || name.startsWith("IPC")) {
					t.join();
				}
			}
			
			// start the job manager
			jobManager = new JobManager(ExecutionMode.LOCAL);
			runner = new Thread("JobManager Task Loop") {
				@Override
				public void run() {
					// run the main task loop
					jobManager.runTaskLoop();
				}
			};
			runner.setDaemon(true);
			runner.start();
	
			waitForJobManagerToBecomeReady();
		}
	}

	public void stop() throws Exception {
		synchronized (this.startStopLock) {
			if (jobManager != null) {
				jobManager.shutdown();
				jobManager = null;
			}
	
			if (runner != null) {
				runner.interrupt();
				runner.join();
				runner = null;
			}
		}
	}

	// ------------------------------------------------------------------------
	// Network utility methods
	// ------------------------------------------------------------------------
	
	private void waitForJobManagerToBecomeReady() throws InterruptedException {
		Map<InstanceType, InstanceTypeDescription> instanceMap;
		while ((instanceMap = jobManager.getMapOfAvailableInstanceTypes()) == null || instanceMap.isEmpty()) {
			Thread.sleep(100);
		}
	}
	
	public static Configuration getMiniclusterDefaultConfig(int jobManagerRpcPort, int taskManagerRpcPort,
			int taskManagerDataPort, String hdfsConfigFile, boolean visualization)
	{
		final Configuration config = new Configuration();
		
		// addresses and ports
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRpcPort);
		config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, taskManagerRpcPort);
		config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, taskManagerDataPort);
		
		// polling interval
		config.setInteger(ConfigConstants.JOBCLIENT_POLLING_INTERVAL_KEY, 2);
		
		// enable / disable features
		config.setInteger(ConfigConstants.JOB_EXECUTION_RETRIES_KEY, 0);
		config.setBoolean("jobmanager.visualization.enable", visualization);
		
		// hdfs
		if (hdfsConfigFile != null) {
			config.setString("fs.hdfs.hdfsdefault", hdfsConfigFile);
		}
		return config;
	}
}