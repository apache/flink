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
import java.net.InetSocketAddress;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.client.AbstractJobResult.ReturnCode;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;

/**
 * Encapsulates the functionality necessary to compile and submit a pact program to a nephele cluster.
 * 
 * @author Moritz Kaufmann
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class Client {
	
	private final Configuration nepheleConfig;	// the configuration describing the job manager address
	
	private final PactCompiler compiler;		// the compiler to compile the jobs

	// ------------------------------------------------------------------------
	//                            Construction
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new instance of the class that submits the jobs to a nephele job-manager.
	 * at the given address using the default port.
	 * 
	 * @param jobManagerAddress Address and port of the job-manager.
	 */
	public Client(InetSocketAddress jobManagerAddress) {
		this.nepheleConfig = new Configuration();
		nepheleConfig.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerAddress.getAddress().getHostAddress());
		nepheleConfig.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerAddress.getPort());
		
		this.compiler = new PactCompiler(new DataStatistics(), new FixedSizeClusterCostEstimator(), jobManagerAddress);
	}

	/**
	 * Creates a instance that submits the pact programs to the job-manager defined in the
	 * configuration.
	 * 
	 * @param nepheleConfig
	 * 			create a new client based on this configuration
	 */
	public Client(Configuration nepheleConfig) {
		this.nepheleConfig = nepheleConfig;
		
		// instantiate the address to the job manager
		final String address = nepheleConfig.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		if (address == null) {
			throw new CompilerException("Cannot find address to job manager's RPC service in the global configuration.");
		}
		
		final int port = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
		if (port < 0) {
			throw new CompilerException("Cannot find port to job manager's RPC service in the global configuration.");
		}

		final InetSocketAddress jobManagerAddress = new InetSocketAddress(address, port);
		this.compiler = new PactCompiler(new DataStatistics(), new FixedSizeClusterCostEstimator(), jobManagerAddress);
	}

	
	// ------------------------------------------------------------------------
	//                      Compilation and Submission
	// ------------------------------------------------------------------------
	
	/**
	 * Creates the optimized plan for a given pact program, using this client's compiler.
	 *  
	 * @param prog The program to be compiled.
	 * @return The compiled and optimized plan, as returned by the compiler.
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the pact program could not be instantiated from its jar file.
	 * @throws ErrorInPlanAssemblerException Thrown, if the plan assembler function causes an exception.
	 */
	public OptimizedPlan getOptimizedPlan(PactProgram prog) throws CompilerException, ProgramInvocationException, ErrorInPlanAssemblerException {
		return compiler.compile(prog.getPlan());
	}
	
	/**
	 * Creates the job-graph, which is ready for submission, from a compiled and optimized pact program.
	 * The original pact-program is required to access the original jar file.
	 * 
	 * @param prog The original pact program.
	 * @param optPlan The optimized plan.
	 * @return The nephele job graph, generated from the optimized plan.
	 */
	public JobGraph getJobGraph(PactProgram prog, OptimizedPlan optPlan) {
		JobGraphGenerator gen = new JobGraphGenerator();
		JobGraph job = gen.compileJobGraph(optPlan);
		job.addJar(new Path(prog.getJarFile().getAbsolutePath()));
		return job;
	}
	
	
	/**
	 * Runs a pact program on the nephele system whose job-manager is configured in this client's configuration.
	 * This method involves all steps, from compiling, job-graph generation to submission.
	 * 
	 * @param prog The program to be executed.
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the pact program could not be instantiated from its jar file,
	 *                                    or if the submission failed. That might be either due to an I/O problem,
	 *                                    i.e. the job-manager is unreachable, or due to the fact that the execution
	 *                                    on the nephele system failed.
	 * @throws ErrorInPlanAssemblerException Thrown, if the plan assembler function causes an exception.
	 */
	public void run(PactProgram prog) throws CompilerException, ProgramInvocationException, ErrorInPlanAssemblerException {
		run(prog, getOptimizedPlan(prog));
	}
	
	/**
	 * Submits the given program to the nephele job-manager for execution. The first step of teh compilation process is skipped and
	 * the given compiled plan is taken.
	 * 
	 * @param prog The original pact program.
	 * @param compiledPlan The optimized plan.
	 * @throws ProgramInvocationException Thrown, if the pact program could not be instantiated from its jar file,
	 *                                    or if the submission failed. That might be either due to an I/O problem,
	 *                                    i.e. the job-manager is unreachable, or due to the fact that the execution
	 *                                    on the nephele system failed.
	 */
	public void run(PactProgram prog, OptimizedPlan compiledPlan) throws ProgramInvocationException {
		JobGraph job = getJobGraph(prog, compiledPlan);
		run(job);
	}

	/**
	 * Submits the job-graph to the nephele job-manager for execution.
	 * 
	 * @param prog The program to be submitted.
	 * @throws ProgramInvocationException Thrown, if the submission failed. That might be either due to an I/O problem,
	 *                                    i.e. the job-manager is unreachable, or due to the fact that the execution
	 *                                    on the nephele system failed.
	 */
	public void run(JobGraph jobGraph) throws ProgramInvocationException {
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
