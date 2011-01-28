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

package eu.stratosphere.pact.client.nephele.api;

import java.io.IOException;
import java.net.InetAddress;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.client.AbstractJobResult.ReturnCode;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;

/**
 * Encapsulates the functionality necessary to submit a pact program to a nephele cluster.
 * 
 * @author Moritz Kaufmann
 */
public class Client {
	private final Configuration nepheleConfig; // the configuration describing the job manager address

	/**
	 * Creates a new instance of the class that submits the jobs to a nephele jobmanager
	 * at the given address using the default port.
	 * 
	 * @param jobManagerAddress
	 *        adress of the jobmanager
	 */
	public Client(InetAddress jobManagerAddress) {
		this.nepheleConfig = new Configuration();
		nepheleConfig.setString("jobmanager.rpc.address", jobManagerAddress.getHostAddress());
	}

	/**
	 * Creates a instance that submits the pact programs to the jobmanager defined in the
	 * configuration.
	 * 
	 * @param nepheleConfig
	 * 			create a new client based on this configuration
	 */
	public Client(Configuration nepheleConfig) {
		this.nepheleConfig = nepheleConfig;
	}

	/**
	 * Submits the pact program to the cluster for execution.
	 * 
	 * @param prog
	 *        The program to be submitted
	 * @throws ErrorInPlanAssemblerException
	 * 			This error is thrown if there is a error during plan generation which may indicate
	 * 			missing arguments for the plan or an error in the plan generation itself
	 * @throws ProgramInvocationException
	 * 			Indicates a error in the program configuration where the plan assembler can not be 
	 * 			instantiated.
	 */
	public void run(PactProgram prog) throws ProgramInvocationException, ErrorInPlanAssemblerException {
		assert nepheleConfig != null;

		JobGraph jobGraph = prog.getCompiledPlan();
		jobGraph.addJar(new Path(prog.getJarFile().getAbsolutePath()));

		// submit job to nephele
		nepheleConfig.setBoolean("jobclient.shutdown.terminatejob", false); // TODO: terminate job logic is broken

		JobSubmissionResult result = null;

		JobClient client;
		try {
			client = new JobClient(jobGraph, nepheleConfig);
		} catch (IOException e) {
			throw new ProgramInvocationException("Could not open job manager: " + e.getMessage(), e);
		}

		try {
			result = client.submitJob();
		} catch (IOException e) {
			throw new ProgramInvocationException("Could not submit job to job manager: " + e.getMessage(), e);
		}

		if (result.getReturnCode() != ReturnCode.SUCCESS) {
			throw new ProgramInvocationException("The job was not successfully submitted to the nephele job manager"
				+ (result.getDescription() == null ? "." : ": " + result.getDescription()));
			// (result.getDescription() == null ? "." : ": " + result.getDescription().split("\n")[0]));
		}
	}
}
