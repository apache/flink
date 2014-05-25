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

package eu.stratosphere.client.program;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.contextcheck.ContextChecker;
import eu.stratosphere.compiler.costs.DefaultCostEstimator;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.client.AbstractJobResult.ReturnCode;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.jobgraph.JobGraph;

/**
 * Encapsulates the functionality necessary to submit a program to a remote cluster.
 */
public class Client {
	
	private static final Log LOG = LogFactory.getLog(Client.class);
	
	
	private final Configuration configuration;	// the configuration describing the job manager address
	
	private final PactCompiler compiler;		// the compiler to compile the jobs

	private boolean printStatusDuringExecution;
	
	// ------------------------------------------------------------------------
	//                            Construction
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new instance of the class that submits the jobs to a job-manager.
	 * at the given address using the default port.
	 * 
	 * @param jobManagerAddress Address and port of the job-manager.
	 */
	public Client(InetSocketAddress jobManagerAddress, Configuration config) {
		this.configuration = config;
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerAddress.getAddress().getHostAddress());
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerAddress.getPort());
		
		this.compiler = new PactCompiler(new DataStatistics(), new DefaultCostEstimator(), jobManagerAddress);
		
		//  Disable Local Execution when using a Client
		ContextEnvironment.disableLocalExecution();
	}

	/**
	 * Creates a instance that submits the programs to the job-manager defined in the
	 * configuration.
	 * 
	 * @param config The config used to obtain the job-manager's address.
	 */
	public Client(Configuration config) {
		this.configuration = config;
		
		// instantiate the address to the job manager
		final String address = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		if (address == null) {
			throw new CompilerException("Cannot find address to job manager's RPC service in the global configuration.");
		}
		
		final int port = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
		if (port < 0) {
			throw new CompilerException("Cannot find port to job manager's RPC service in the global configuration.");
		}

		final InetSocketAddress jobManagerAddress = new InetSocketAddress(address, port);
		this.compiler = new PactCompiler(new DataStatistics(), new DefaultCostEstimator(), jobManagerAddress);
		
		//  Disable Local Execution when using a Client
		ContextEnvironment.disableLocalExecution();
	}
	
	public void setPrintStatusDuringExecution(boolean print) {
		this.printStatusDuringExecution = print;
	}

	public String getJobManagerAddress() {
		return this.configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
	}
	
	public int getJobManagerPort() {
		return this.configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);
	}
	
	// ------------------------------------------------------------------------
	//                      Compilation and Submission
	// ------------------------------------------------------------------------
	
	public String getOptimizedPlanAsJson(PackagedProgram prog, int parallelism) throws CompilerException, ProgramInvocationException {
		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON(getOptimizedPlan(prog, parallelism));
	}
	
	public OptimizedPlan getOptimizedPlan(PackagedProgram prog, int parallelism) throws CompilerException, ProgramInvocationException {
		if (prog.isUsingProgramEntryPoint()) {
			return getOptimizedPlan(prog.getPlanWithJars(), parallelism);
		}
		else if (prog.isUsingInteractiveMode()) {
			// temporary hack to support the optimizer plan preview
			OptimizerPlanEnvironment env = new OptimizerPlanEnvironment(this.compiler);
			if (parallelism > 0) {
				env.setDegreeOfParallelism(parallelism);
			}
			env.setAsContext();
			try {
				prog.invokeInteractiveModeForExecution();
			} catch (Throwable t) {
				// the invocation gets aborted with the preview plan
			}
			return env.optimizerPlan;
		}
		else {
			throw new RuntimeException();
		}
	}
	
	public OptimizedPlan getOptimizedPlan(Plan p, int parallelism) throws CompilerException {
		if (parallelism > 0 && p.getDefaultParallelism() <= 0) {
			p.setDefaultParallelism(parallelism);
		}
		
		ContextChecker checker = new ContextChecker();
		checker.check(p);
		return this.compiler.compile(p);
	}
	
	
	/**
	 * Creates the optimized plan for a given program, using this client's compiler.
	 *  
	 * @param prog The program to be compiled.
	 * @return The compiled and optimized plan, as returned by the compiler.
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the program could not be instantiated from its jar file.
	 */
	public OptimizedPlan getOptimizedPlan(JobWithJars prog, int parallelism) throws CompilerException, ProgramInvocationException {
		return getOptimizedPlan(prog.getPlan(), parallelism);
	}
	
	public JobGraph getJobGraph(PackagedProgram prog, OptimizedPlan optPlan) throws ProgramInvocationException {
		return getJobGraph(optPlan, prog.getAllLibraries());
	}
	
	private JobGraph getJobGraph(OptimizedPlan optPlan, List<File> jarFiles) {
		NepheleJobGraphGenerator gen = new NepheleJobGraphGenerator();
		JobGraph job = gen.compileJobGraph(optPlan);
		
		for (File jar : jarFiles) {
			job.addJar(new Path(jar.getAbsolutePath()));
		}
		
		return job;
	}
	
	public JobExecutionResult run(final PackagedProgram prog, int parallelism, boolean wait) throws ProgramInvocationException {
		Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());
		if (prog.isUsingProgramEntryPoint()) {
			return run(prog.getPlanWithJars(), parallelism, wait);
		}
		else if (prog.isUsingInteractiveMode()) {
			ContextEnvironment env = new ContextEnvironment(this, prog.getAllLibraries(), prog.getUserCodeClassLoader());
			
			if (parallelism > 0) {
				env.setDegreeOfParallelism(parallelism);
			}
			env.setAsContext();
			
			if (wait) {
				// invoke here
				prog.invokeInteractiveModeForExecution();
			}
			else {
				// invoke in the background
				Thread backGroundRunner = new Thread("Program Runner") {
					public void run() {
						try {
							prog.invokeInteractiveModeForExecution();
						}
						catch (Throwable t) {
							LOG.error("The program execution failed.", t);
						}
					};
				};
				backGroundRunner.start();
			}
			return null;
		}
		else {
			throw new RuntimeException();
		}
	}
	
	public JobExecutionResult run(PackagedProgram prog, OptimizedPlan optimizedPlan, boolean wait) throws ProgramInvocationException {
		return run(optimizedPlan, prog.getAllLibraries(), wait);

	}
	
	/**
	 * Runs a program on the nephele system whose job-manager is configured in this client's configuration.
	 * This method involves all steps, from compiling, job-graph generation to submission.
	 * 
	 * @param prog The program to be executed.
	 * @param wait A flag that indicates whether this function call should block until the program execution is done.
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the program could not be instantiated from its jar file,
	 *                                    or if the submission failed. That might be either due to an I/O problem,
	 *                                    i.e. the job-manager is unreachable, or due to the fact that the execution
	 *                                    on the nephele system failed.
	 * @throws JobInstantiationException Thrown, if the plan assembler function causes an exception.
	 */
	public JobExecutionResult run(JobWithJars prog, int parallelism, boolean wait) throws CompilerException, ProgramInvocationException {
		return run(getOptimizedPlan(prog, parallelism), prog.getJarFiles(), wait);
	}
	

	public JobExecutionResult run(OptimizedPlan compiledPlan, List<File> libraries, boolean wait) throws ProgramInvocationException {
		JobGraph job = getJobGraph(compiledPlan, libraries);
		return run(job, wait);
	}

	public JobExecutionResult run(JobGraph jobGraph, boolean wait) throws ProgramInvocationException {
		JobClient client;
		try {
			client = new JobClient(jobGraph, configuration);
		}
		catch (IOException e) {
			throw new ProgramInvocationException("Could not open job manager: " + e.getMessage());
		}
		
		client.setConsoleStreamForReporting(this.printStatusDuringExecution ? System.out : null);

		try {
			if (wait) {
				return client.submitJobAndWait();
			}
			else {
				JobSubmissionResult result = client.submitJob();
				
				if (result.getReturnCode() != ReturnCode.SUCCESS) {
					throw new ProgramInvocationException("The job was not successfully submitted to the nephele job manager"
						+ (result.getDescription() == null ? "." : ": " + result.getDescription()));
				}
			}
		}
		catch (IOException e) {
			throw new ProgramInvocationException("Could not submit job to job manager: " + e.getMessage());
		}
		catch (JobExecutionException jex) {
			if(jex.isJobCanceledByUser()) {
				throw new ProgramInvocationException("The program has been canceled");
			} else {
				throw new ProgramInvocationException("The program execution failed: " + jex.getMessage());
			}
		}
		return new JobExecutionResult(-1, null);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class OptimizerPlanEnvironment extends ExecutionEnvironment {
		
		private final PactCompiler compiler;
		
		private OptimizedPlan optimizerPlan;
		
		
		private OptimizerPlanEnvironment(PactCompiler compiler) {
			this.compiler = compiler;
		}
		
		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			Plan plan = createProgramPlan(jobName);
			if (getDegreeOfParallelism() > 0) {
				plan.setDefaultParallelism(getDegreeOfParallelism());
			}
			this.optimizerPlan = compiler.compile(plan);
			
			// do not go on with anything now!
			throw new ProgramAbortException();
		}

		@Override
		public String getExecutionPlan() throws Exception {
			Plan plan = createProgramPlan("unused");
			if (getDegreeOfParallelism() > 0) {
				plan.setDefaultParallelism(getDegreeOfParallelism());
			}
			
			this.optimizerPlan = compiler.compile(plan);
			
			// do not go on with anything now!
			throw new ProgramAbortException();
		}
		
		private void setAsContext() {
			initializeContextEnvironment(this);
		}
	}
	
	static final class ProgramAbortException extends Error {
		private static final long serialVersionUID = 1L;
	}
}
