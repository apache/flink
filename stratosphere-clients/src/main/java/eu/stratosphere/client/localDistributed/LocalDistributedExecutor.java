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

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.PlanExecutor;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.JobManager.ExecutionMode;
import eu.stratosphere.nephele.taskmanager.TaskManager;

/**
 * This executor allows to execute jobs locally on a single machine using multiple task managers.
 * <p>
 * The task managers are started in separate threads and communicate via network channels (TCP/IP) when the tasks are
 * executed. This is the main difference to {@link eu.stratosphere.client.LocalExecutor}, where tasks communicate via
 * memory channels and don't go through the network stack.
 */
public class LocalDistributedExecutor extends PlanExecutor {
	
	private static final int JOB_MANAGER_RPC_PORT = 6498;

	private static final int SLEEP_TIME = 50;

	private static final int START_STOP_TIMEOUT = 2000;

	//------------------------------------------------------------------------------------------------------------------

	private boolean running = false;

	private JobManager jobManager;

	private List<TaskManager> taskManagers = new ArrayList<TaskManager>();


	public synchronized void start(int numTaskMgr) throws Exception {
		if (this.running) {
			return;
		}
		

		// we need to down size the memory. determine the memory and divide it by the number of task managers
		long javaMem = HardwareDescriptionFactory.extractFromSystem().getSizeOfFreeMemory();
		javaMem /= numTaskMgr;
		
		// convert memory from bytes to megabytes
		javaMem >>>= 20;
		
		Configuration conf = NepheleMiniCluster.getMiniclusterDefaultConfig(
				JOB_MANAGER_RPC_PORT, 6500, 7501, javaMem, null, false, true, false);
		GlobalConfiguration.includeConfiguration(conf);
			
		// start job manager
		this.jobManager = new JobManager(ExecutionMode.CLUSTER);
		
		// start the task managers
		for (int tm = 0; tm < numTaskMgr; tm++) {
			// The whole thing can only work if we assign different ports to each TaskManager
			Configuration tmConf = new Configuration();

			tmConf.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT + tm + numTaskMgr);

			tmConf.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
					ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT + tm); // taskmanager.data.port

			GlobalConfiguration.includeConfiguration(tmConf);

			TaskManager t = new TaskManager();
			taskManagers.add(t);
		}

		int timeout = START_STOP_TIMEOUT * this.taskManagers.size();

		// wait for all task managers to register at the JM
		for (int sleep = 0; sleep < timeout; sleep += SLEEP_TIME) {
			if (jobManager.getNumberOfTaskTrackers() >= numTaskMgr) {
				break;
			}

			Thread.sleep(SLEEP_TIME);
		}

		if (jobManager.getNumberOfTaskTrackers() < numTaskMgr) {
			throw new RuntimeException(String.format("Task manager start up timed out (%d ms).", timeout));
		}

		this.running = true;
	}

	public synchronized void stop() throws InterruptedException, IOException {
		if (!this.running) {
			return;
		}

		// 1. shut down task managers
		for (TaskManager t : this.taskManagers) {
			t.shutdown();
		}

		boolean isTaskManagersShutDown = false;

		// wait for task managers to shut down
		int timeout = START_STOP_TIMEOUT * this.taskManagers.size();

		for (int sleep = 0; sleep < timeout; sleep += SLEEP_TIME) {
			isTaskManagersShutDown = true;

			for (TaskManager t : this.taskManagers) {
				if (!t.isShutDown()) {
					isTaskManagersShutDown = false;
				}
			}

			if (isTaskManagersShutDown) {
				break;
			}

			Thread.sleep(SLEEP_TIME);
		}

		if (!isTaskManagersShutDown) {
			throw new RuntimeException(String.format("TaskManager shut down timed out (%d ms).", timeout));
		}

		// 2. shut down job manager
		this.jobManager.shutdown();

		for (int sleep = 0; sleep < START_STOP_TIMEOUT; sleep += SLEEP_TIME) {
			if (this.jobManager.isShutDown()) {
				break;
			}

			Thread.sleep(SLEEP_TIME);
		}

		try {
			if (!this.jobManager.isShutDown()) {
				throw new RuntimeException(String.format("Job manager shut down timed out (%d ms).", START_STOP_TIMEOUT));
			}
		} finally {
			this.taskManagers.clear();
			this.jobManager = null;
			this.running = false;
		}
	}

	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		synchronized (this) {
			return run(plan);
		}
	}
	
	@Override
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		if (!this.running) {
			throw new IllegalStateException("LocalDistributedExecutor has not been started.");
		}
		
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);
		PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
		return dumper.getOptimizerPlanAsJSON(op);
	}
	
	public JobExecutionResult run(JobGraph jobGraph) throws Exception {
		if (!this.running) {
			throw new IllegalStateException("LocalDistributedExecutor has not been started.");
		}

		return runNepheleJobGraph(jobGraph);
	}
	
	public JobExecutionResult run(Plan plan) throws Exception {
		if (!this.running) {
			throw new IllegalStateException("LocalDistributedExecutor has not been started.");
		}

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);
		
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);

		return runNepheleJobGraph(jobGraph);
	}

	private JobExecutionResult runNepheleJobGraph(JobGraph jobGraph) throws Exception {
		try {
			JobClient jobClient = getJobClient(jobGraph);

			return jobClient.submitJobAndWait();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	private JobClient getJobClient(JobGraph jobGraph) throws Exception {
		Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, JOB_MANAGER_RPC_PORT);
		return new JobClient(jobGraph, configuration);
	}
}
