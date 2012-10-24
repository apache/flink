/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.File;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;

/**
 * A class for executing a {@link Plan} on a local Nephele instance. Note that
 * no HDFS instance or anything of that nature is provided. You must therefore
 * only use data sources and sinks with paths beginning with "file://" in your
 * plan.
 * 
 * When the class is instantiated a local nephele instance is started, this can
 * be stopped by calling stopNephele.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class LocalPlanExecutor {

	private NepheleMiniCluster nephele;

	/**
	 * Starts the local Nephele instance.
	 */
	public LocalPlanExecutor() throws Exception {
		String nepheleConfigDir = System.getProperty("java.io.tmpdir") + "/minicluster/nephele/config";
		nephele = new NepheleMiniCluster(nepheleConfigDir, "");
	}

	/**
	 * Stop the local Nephele instance. You should not call executePlan after
	 * this.
	 */
	public void stopNephele() throws Exception {
		nephele.stop();
		nephele = null;

		File f = new File(System.getProperty("java.io.tmpdir") + "/minicluster");
		f.delete();
	}

	/**
	 * Execute the given plan on the local Nephele instance, wait for the job to
	 * finish and return the runtime in milliseconds.
	 */
	public long executePlan(Plan plan) throws Exception {
		if (nephele == null) {
			throw new Exception("We have no running Nephele instance.");
		}

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);
		JobGraphGenerator jgg = new JobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);
		JobClient jobClient = nephele.getJobClient(jobGraph);
		return jobClient.submitJobAndWait();
	}
}
