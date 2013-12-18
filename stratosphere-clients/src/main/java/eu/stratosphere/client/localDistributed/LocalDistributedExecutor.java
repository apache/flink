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

package eu.stratosphere.client.localDistributed;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.Job;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.instance.local.LocalTaskManagerThread;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.JobManager.ExecutionMode;

/**
 * This executor allows to execute a Job on one machine (locally) using multiple
 * TaskManagers that communicate via TCP/IP, not memory.
 */
public class LocalDistributedExecutor  {
	
	private static int JOBMANAGER_RPC_PORT = 6498;
	private boolean running = false;
	
	
	public static class JobManagerThread extends Thread {
		JobManager jm;
		
		public JobManagerThread(JobManager jm) {
			this.jm = jm;
		}
		@Override
		public void run() {
			jm.runTaskLoop();
		}
	}
	
	public void startNephele(final int numTaskMgr) throws InterruptedException {
		if(running) {
			return;
		}
		
		Configuration conf = NepheleMiniCluster.getMiniclusterDefaultConfig(JOBMANAGER_RPC_PORT, 6500,
				7501, null, true);
		GlobalConfiguration.includeConfiguration(conf);
			
		// start job manager
		JobManager jobManager;
		try {
			jobManager = new JobManager(ExecutionMode.CLUSTER);
		}
		catch (Exception e) {
			e.printStackTrace();
			return;
		}
	
		
		JobManagerThread jobManagerThread = new JobManagerThread(jobManager);
		jobManagerThread.setDaemon(true);
		jobManagerThread.start();
		
		// start the taskmanagers
		List<LocalTaskManagerThread> tms = new ArrayList<LocalTaskManagerThread>();
		for(int tm = 0; tm < numTaskMgr; tm++) {
			// The whole thing can only work if we assign different ports to each TaskManager
			Configuration tmConf = new Configuration();
			tmConf.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
						ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT + tm + numTaskMgr);
			tmConf.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT+tm); // taskmanager.data.port
			GlobalConfiguration.includeConfiguration(tmConf);
			LocalTaskManagerThread t = new LocalTaskManagerThread("LocalDistributedExecutor: LocalTaskManagerThread-#"+tm,numTaskMgr);
			t.start();
			tms.add(t);
		}
		
		final int sleepTime = 100;
		final int maxSleep = 1000 * 2 * numTaskMgr; // we wait 2 seconds PER TaskManager.
		int slept = 0;
		// wait for all taskmanagers to register to the JM
		while(jobManager.getNumberOfTaskTrackers() < numTaskMgr) {
			Thread.sleep(sleepTime);
			if(slept >= maxSleep) {
				throw new RuntimeException("Waited for more than 2 seconds per TaskManager to register at "
						+ "the JobManager.");
			}
		}
		this.running = true;
	}
	
	public void run(final JobGraph jobGraph) throws Exception {
		if(!running) {
			throw new IllegalStateException("Nephele has not been started");
		}
		runNepheleJobGraph(jobGraph);
	}
	
	public void run(final Job plan) throws Exception {
		if(!running) {
			throw new IllegalStateException("Nephele has not been started");
		}
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);
		
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);
		runNepheleJobGraph(jobGraph);
	}
	
	private void runNepheleJobGraph(JobGraph jobGraph) throws Exception {
		try {
			JobClient jobClient = getJobClient(jobGraph);
			jobClient.submitJobAndWait();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private JobClient getJobClient(JobGraph jobGraph) throws Exception {
		Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, JOBMANAGER_RPC_PORT);
		return new JobClient(jobGraph, configuration);
	}
}
