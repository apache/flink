/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.client;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;

/**
 * A class for executing a {@link Plan} on a local Nephele instance. Note that
 * no HDFS instance or anything of that nature is provided. You must therefore
 * only use data sources and sinks with paths beginning with "file://" in your
 * plan.
 * 
 * When the class is instantiated a local nephele instance is started, this can
 * be stopped by calling stopNephele.
 */
public class LocalExecutor {

	private final Object lock = new Object();	// we lock to ensure singleton execution
	
	private NepheleMiniCluster nephele;

	
	
	public void start() throws Exception {
		synchronized (this.lock) {
			this.nephele = new NepheleMiniCluster();
			this.nephele.start();
		}
	}
	
	/**
	 * Stop the local executor instance. You should not call executePlan after this.
	 */
	public void stop() throws Exception {
		synchronized (this.lock) {
			this.nephele.stop();
			this.nephele = null;
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
	public long executePlan(Plan plan) throws Exception {
		synchronized (this.lock) {
			if (this.nephele == null) {
				throw new Exception("The local executor has not been started.");
			}

			PactCompiler pc = new PactCompiler();
			OptimizedPlan op = pc.compile(plan);
			
			NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
			JobGraph jobGraph = jgg.compileJobGraph(op);
			
			JobClient jobClient = this.nephele.getJobClient(jobGraph);
			return jobClient.submitJobAndWait();
		}
	}
	
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
	public static long execute(PlanAssembler pa, String... args) throws Exception {
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
	public static long execute(Plan plan) throws Exception {
		LocalExecutor exec = new LocalExecutor();
		try {
			exec.start();
			return exec.executePlan(plan);
		} finally {
			if (exec != null) {
				exec.stop();
			}
		}
	}
}