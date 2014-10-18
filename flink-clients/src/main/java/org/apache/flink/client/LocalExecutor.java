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


package org.apache.flink.client;

import java.util.List;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.common.Program;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.minicluster.NepheleMiniCluster;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.contextcheck.ContextChecker;
import org.apache.flink.compiler.dag.DataSinkNode;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plandump.PlanJSONDumpGenerator;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;

/**
 * A class for executing a {@link Plan} on a local embedded Flink runtime instance.
 */
public class LocalExecutor extends PlanExecutor {
	
	private static boolean DEFAULT_OVERWRITE = false;

	private static final int DEFAULT_TASK_MANAGER_NUM_SLOTS = -1;

	private final Object lock = new Object();	// we lock to ensure singleton execution
	
	private NepheleMiniCluster nephele;

	// ---------------------------------- config options ------------------------------------------
	
	private int jobManagerRpcPort = -1;
	
	private int taskManagerRpcPort = -1;
	
	private int taskManagerDataPort = -1;

	private int taskManagerNumSlots = DEFAULT_TASK_MANAGER_NUM_SLOTS;

	private String configDir;

	private String hdfsConfigFile;
	
	private boolean defaultOverwriteFiles = DEFAULT_OVERWRITE;
	
	private boolean defaultAlwaysCreateDirectory = false;

	// --------------------------------------------------------------------------------------------
	
	public LocalExecutor() {
		if (!ExecutionEnvironment.localExecutionIsAllowed()) {
			throw new InvalidProgramException("The LocalEnvironment cannot be used when submitting a program through a client.");
		}
	}

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

	public String getHdfsConfig() {
		return hdfsConfigFile;
	}
	
	public void setHdfsConfig(String hdfsConfig) {
		this.hdfsConfigFile = hdfsConfig;
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

	public void setTaskManagerNumSlots(int taskManagerNumSlots) { this.taskManagerNumSlots = taskManagerNumSlots; }

	public int getTaskManagerNumSlots() { return this.taskManagerNumSlots; }
	
	// --------------------------------------------------------------------------------------------
	
	public void start() throws Exception {
		synchronized (this.lock) {
			if (this.nephele == null) {
				
				// create the embedded runtime
				this.nephele = new NepheleMiniCluster();
				
				// configure it, if values were changed. otherwise the embedded runtime uses the internal defaults
				if (jobManagerRpcPort > 0) {
					nephele.setJobManagerRpcPort(jobManagerRpcPort);
				}
				if (taskManagerRpcPort > 0) {
					nephele.setTaskManagerRpcPort(jobManagerRpcPort);
				}
				if (taskManagerDataPort > 0) {
					nephele.setTaskManagerDataPort(taskManagerDataPort);
				}
				if (configDir != null) {
					nephele.setConfigDir(configDir);
				}
				if (hdfsConfigFile != null) {
					nephele.setHdfsConfigFile(hdfsConfigFile);
				}
				nephele.setDefaultOverwriteFiles(defaultOverwriteFiles);
				nephele.setDefaultAlwaysCreateDirectory(defaultAlwaysCreateDirectory);
				nephele.setTaskManagerNumSlots(taskManagerNumSlots);
				
				// start it up
				this.nephele.start();
			} else {
				throw new IllegalStateException("The local executor was already started.");
			}
		}
	}

	/**
	 * Stop the local executor instance. You should not call executePlan after this.
	 */
	public void stop() throws Exception {
		synchronized (this.lock) {
			if (this.nephele != null) {
				this.nephele.stop();
				this.nephele = null;
			} else {
				throw new IllegalStateException("The local executor was not started.");
			}
		}
	}

	/**
	 * Execute the given plan on the local Nephele instance, wait for the job to
	 * finish and return the runtime in milliseconds.
	 * 
	 * @param plan The plan of the program to execute.
	 * @return The net runtime of the program, in milliseconds.
	 * 
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		if (plan == null) {
			throw new IllegalArgumentException("The plan may not be null.");
		}
		
		ContextChecker checker = new ContextChecker();
		checker.check(plan);
		
		synchronized (this.lock) {
			
			// check if we start a session dedicated for this execution
			final boolean shutDownAtEnd;
			if (this.nephele == null) {
				// we start a session just for us now
				shutDownAtEnd = true;
				
				// configure the number of local slots equal to the parallelism of the local plan
				if (this.taskManagerNumSlots == DEFAULT_TASK_MANAGER_NUM_SLOTS) {
					int maxParallelism = plan.getMaximumParallelism();
					if (maxParallelism > 0) {
						this.taskManagerNumSlots = maxParallelism;
					}
				}
				
				start();
			} else {
				// we use the existing session
				shutDownAtEnd = false;
			}

			try {
				PactCompiler pc = new PactCompiler(new DataStatistics());
				OptimizedPlan op = pc.compile(plan);
				
				NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
				JobGraph jobGraph = jgg.compileJobGraph(op);
				
				JobClient jobClient = this.nephele.getJobClient(jobGraph);
				JobExecutionResult result = jobClient.submitJobAndWait();
				return result;
			}
			finally {
				if (shutDownAtEnd) {
					stop();
				}
			}
		}
	}

	/**
	 * Returns a JSON dump of the optimized plan.
	 * 
	 * @param plan
	 *            The program's plan.
	 * @return JSON dump of the optimized plan.
	 * @throws Exception
	 */
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();
	
		return gen.getOptimizerPlanAsJSON(op);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Static variants that internally bring up an instance and shut it down after the execution
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Executes the program described by the given plan assembler.
	 * 
	 * @param pa The program's plan assembler. 
	 * @param args The parameters.
	 * @return The net runtime of the program, in milliseconds.
	 * 
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	public static JobExecutionResult execute(Program pa, String... args) throws Exception {
		return execute(pa.getPlan(args));
	}
	
	/**
	 * Executes the program represented by the given Pact plan.
	 * 
	 * @param plan The program's plan. 
	 * @return The net runtime of the program, in milliseconds.
	 * 
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	public static JobExecutionResult execute(Plan plan) throws Exception {
		LocalExecutor exec = new LocalExecutor();
		try {
			exec.start();
			return exec.executePlan(plan);
		} finally {
			exec.stop();
		}
	}

	/**
	 * Returns a JSON dump of the optimized plan.
	 * 
	 * @param plan
	 *            The program's plan.
	 * @return JSON dump of the optimized plan.
	 * @throws Exception
	 */
	public static String optimizerPlanAsJSON(Plan plan) throws Exception {
		LocalExecutor exec = new LocalExecutor();
		try {
			exec.start();
			PactCompiler pc = new PactCompiler(new DataStatistics());
			OptimizedPlan op = pc.compile(plan);
			PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();

			return gen.getOptimizerPlanAsJSON(op);
		} finally {
			exec.stop();
		}
	}

	/**
	 * Return unoptimized plan as JSON.
	 * 
	 * @param plan The program plan.
	 * @return The plan as a JSON object.
	 */
	public static String getPlanAsJSON(Plan plan) {
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();
		List<DataSinkNode> sinks = PactCompiler.createPreOptimizedPlan(plan);
		return gen.getPactPlanAsJSON(sinks);
	}

	/**
	 * By default, local environments do not overwrite existing files.
	 * 
	 * NOTE: This method must be called prior to initializing the LocalExecutor or a 
	 * {@link org.apache.flink.api.java.LocalEnvironment}.
	 * 
	 * @param overwriteByDefault True to overwrite by default, false to not overwrite by default.
	 */
	public static void setOverwriteFilesByDefault(boolean overwriteByDefault) {
		DEFAULT_OVERWRITE = overwriteByDefault;
	}
}
