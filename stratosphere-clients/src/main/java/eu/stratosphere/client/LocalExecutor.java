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

package eu.stratosphere.client;

import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.dag.DataSinkNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;

/**
 * A class for executing a {@link Plan} on a local embedded Stratosphere instance.
 */
public class LocalExecutor implements PlanExecutor {

	private final Object lock = new Object();	// we lock to ensure singleton execution
	
	private NepheleMiniCluster nephele;

	// ---------------------------------- config options ------------------------------------------
	
	private int jobManagerRpcPort = -1;
	
	private int taskManagerRpcPort = -1;
	
	private int taskManagerDataPort = -1;
	
	private String configDir;

	private String hdfsConfigFile;
	
	private boolean defaultOverwriteFiles = false;
	
	private boolean defaultAlwaysCreateDirectory = false;
	
	// --------------------------------------------------------------------------------------------
	
	public LocalExecutor() {
		if (System.getProperty("log4j.configuration") == null) {
			setLoggingLevel(Level.INFO);
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
		synchronized (this.lock) {
			if (this.nephele == null) {
				throw new IllegalStateException("The local executor has not been started.");
			}

			PactCompiler pc = new PactCompiler(new DataStatistics());
			OptimizedPlan op = pc.compile(plan);
			
			NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
			JobGraph jobGraph = jgg.compileJobGraph(op);
			
			JobClient jobClient = this.nephele.getJobClient(jobGraph);
			JobExecutionResult result = jobClient.submitJobAndWait();
			return result;
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
		if (this.nephele == null) {
			throw new Exception("The local executor has not been started.");
		}

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
	 * @param pa The program's plan. 
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
	 * @return
	 */
	public static String getPlanAsJSON(Plan plan) {
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();
		List<DataSinkNode> sinks = PactCompiler.createPreOptimizedPlan(plan);
		return gen.getPactPlanAsJSON(sinks);
	}
	
	/**
	 * Utility method for logging
	 */
	public static void setLoggingLevel(Level lvl) {
		Logger root = Logger.getRootLogger();
		root.removeAllAppenders();
		PatternLayout layout = new PatternLayout("%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n");
		ConsoleAppender appender = new ConsoleAppender(layout, "System.err");
		root.addAppender(appender);
		root.setLevel(lvl);
	}
}