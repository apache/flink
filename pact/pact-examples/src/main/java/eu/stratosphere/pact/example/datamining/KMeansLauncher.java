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

package eu.stratosphere.pact.example.datamining;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;

public class KMeansLauncher {

	/**
	 * Runs PACT implementation of a K-Means clustering algorithm on a Nephele cluster.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		// check
		if (args.length != 7) {
			System.out
				.println("KMeansLauncher <jarFile> <dataPath> <clusterPath> <resultPath> <numIterations> <DegreeOfParallelism> <hostName>");
			System.exit(1);
		}

		// parse cluster job parameters
		String jarFilePath = args[0];
		String dataPath = args[1];
		String initClusterPath = args[2];
		String resultPath = args[3];
		int numIterations = Integer.parseInt(args[4]);
		String dop = args[5];
		String hostName = args[6];

		// get path to jar file with kMeans classes
		Path kMeansJarFile = new Path(jarFilePath);

		// set cluster center input
		String clusterPath = initClusterPath;

		// for each iteration
		for (int i = 0; i < numIterations; i++) {

			// TODO: replace with PACT compiler source code interface

			// obtain the PACT plan
			KMeansIteration kmi = new KMeansIteration();
			Plan plan = kmi.getPlan(dop, dataPath, clusterPath, resultPath);

			// compile the PACT plan
			PactCompiler pc = new PactCompiler();
			OptimizedPlan op = pc.compile(plan);

			// generate Nephele JobGraph
			JobGraphGenerator jgg = new JobGraphGenerator();
			JobGraph jobGraph = jgg.compileJobGraph(op);

			// set up job configuration
			jobGraph.addJar(kMeansJarFile);
			Configuration configuration = jobGraph.getJobConfiguration();
			configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, hostName);
			configuration.setBoolean(ConfigConstants.JOBCLIENT_SHUTDOWN_TERMINATEJOB_KEY, false);

			// submit job to Nephele and wait until its done
			JobClient jobClient;
			try {
				jobClient = new JobClient(jobGraph, configuration);
				jobClient.submitJobAndWait();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JobExecutionException e) {
				e.printStackTrace();
			}

			clusterPath = resultPath;

		}

	}

}
