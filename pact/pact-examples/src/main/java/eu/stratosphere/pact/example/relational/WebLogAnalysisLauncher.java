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

package eu.stratosphere.pact.example.relational;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;

public class WebLogAnalysisLauncher {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length != 7) {
			System.out
				.println("RelationalOLAPLauncher <jarFile> <docsPath> <ranksPath> <visitsPath> <resultPath> <DegreeOfParallelism> <hostName>");
			System.exit(1);
		}

		String jarFilePath = args[0];
		String docsPath = args[1];
		String ranksPath = args[2];
		String visitsPath = args[3];
		String resultPath = args[4];
		String dop = args[5];
		String hostName = args[6];

		WebLogAnalysis wla = new WebLogAnalysis();
		Plan plan = wla.getPlan(dop, docsPath, ranksPath, visitsPath, resultPath);

		PactCompiler pc = new PactCompiler();
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);

		Path testJarFile = new Path(jarFilePath);

		jobGraph.addJar(testJarFile);
		// set up job configuration
		Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, hostName);
		configuration.setBoolean(ConfigConstants.JOBCLIENT_SHUTDOWN_TERMINATEJOB_KEY, false);

		// submit job to Nephele
		JobClient jobClient;
		try {
			jobClient = new JobClient(jobGraph, configuration);
			jobClient.submitJobAndWait();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
