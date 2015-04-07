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

package org.apache.flink.client.program;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import com.google.common.base.Preconditions;

/**
 * Encapsulates the functionality necessary to submit a program to a remote cluster.
 */
public class Client {
	
	private static final Logger LOG = LoggerFactory.getLogger(Client.class);
	
	
	private final Configuration configuration;	// the configuration describing the job manager address
	
	private final Optimizer compiler;		// the compiler to compile the jobs
	
	private boolean printStatusDuringExecution = true;

	/**
	 * If != -1, this field specifies the total number of available slots on the cluster
	 * connected to the client.
	 */
	private int maxSlots = -1;

	/**
	 * ID of the last job submitted with this client.
	 */
	private JobID lastJobId = null;

	private ClassLoader userCodeClassLoader;
	
	// ------------------------------------------------------------------------
	//                            Construction
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new instance of the class that submits the jobs to a job-manager.
	 * at the given address using the default port.
	 * 
	 * @param jobManagerAddress Address and port of the job-manager.
	 */
	public Client(InetSocketAddress jobManagerAddress, Configuration config, ClassLoader userCodeClassLoader, int maxSlots) {
		Preconditions.checkNotNull(config, "Configuration is null");
		this.configuration = config;
		
		// using the host string instead of the host name saves a reverse name lookup
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerAddress.getAddress().getHostAddress());
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerAddress.getPort());
		
		this.compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), configuration);
		this.userCodeClassLoader = userCodeClassLoader;
		this.maxSlots = maxSlots;
	}

	/**
	 * Creates a instance that submits the programs to the job-manager defined in the
	 * configuration.
	 * 
	 * @param config The config used to obtain the job-manager's address.
	 */
	public Client(Configuration config, ClassLoader userCodeClassLoader) {
		Preconditions.checkNotNull(config, "Configuration is null");
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

		this.compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), configuration);
		this.userCodeClassLoader = userCodeClassLoader;
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

	/**
	 * @return -1 if unknown. The maximum number of available processing slots at the Flink cluster
	 * connected to this client.
	 */
	public int getMaxSlots() {
		return this.maxSlots;
	}
	
	// ------------------------------------------------------------------------
	//                      Compilation and Submission
	// ------------------------------------------------------------------------
	
	public String getOptimizedPlanAsJson(PackagedProgram prog, int parallelism) throws CompilerException, ProgramInvocationException {
		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON((OptimizedPlan) getOptimizedPlan(prog, parallelism));
	}
	
	public FlinkPlan getOptimizedPlan(PackagedProgram prog, int parallelism) throws CompilerException, ProgramInvocationException {
		Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());
		if (prog.isUsingProgramEntryPoint()) {
			return getOptimizedPlan(prog.getPlanWithJars(), parallelism);
		}
		else if (prog.isUsingInteractiveMode()) {
			// temporary hack to support the optimizer plan preview
			OptimizerPlanEnvironment env = new OptimizerPlanEnvironment(this.compiler);
			if (parallelism > 0) {
				env.setParallelism(parallelism);
			}
			env.setAsContext();
			
			// temporarily write syserr and sysout to a byte array.
			PrintStream originalOut = System.out;
			PrintStream originalErr = System.err;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			System.setOut(new PrintStream(baos));
			ByteArrayOutputStream baes = new ByteArrayOutputStream();
			System.setErr(new PrintStream(baes));
			try {
				ContextEnvironment.enableLocalExecution(false);
				prog.invokeInteractiveModeForExecution();
			}
			catch (ProgramInvocationException e) {
				throw e;
			}
			catch (Throwable t) {
				// the invocation gets aborted with the preview plan
				if (env.optimizerPlan != null) {
					return env.optimizerPlan;
				} else {
					throw new ProgramInvocationException("The program caused an error: ", t);
				}
			}
			finally {
				ContextEnvironment.enableLocalExecution(true);
				System.setOut(originalOut);
				System.setErr(originalErr);
				System.err.println(baes);
				System.out.println(baos);
			}
			
			throw new ProgramInvocationException(
					"The program plan could not be fetched - the program aborted pre-maturely.\n"
					+ "System.err: " + baes.toString() + '\n'
					+ "System.out: " + baos.toString() + '\n');
		}
		else {
			throw new RuntimeException();
		}
	}
	
	public FlinkPlan getOptimizedPlan(Plan p, int parallelism) throws CompilerException {
		if (parallelism > 0 && p.getDefaultParallelism() <= 0) {
			LOG.debug("Changing plan default parallelism from {} to {}",p.getDefaultParallelism(), parallelism);
			p.setDefaultParallelism(parallelism);
		}
		LOG.debug("Set parallelism {}, plan default parallelism {}", parallelism, p.getDefaultParallelism());

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
	public FlinkPlan getOptimizedPlan(JobWithJars prog, int parallelism) throws CompilerException, ProgramInvocationException {
		return getOptimizedPlan(prog.getPlan(), parallelism);
	}
	
	public JobGraph getJobGraph(PackagedProgram prog, FlinkPlan optPlan) throws ProgramInvocationException {
		return getJobGraph(optPlan, prog.getAllLibraries());
	}
	
	private JobGraph getJobGraph(FlinkPlan optPlan, List<File> jarFiles) {
		JobGraph job;
		if (optPlan instanceof StreamingPlan) {
			job = ((StreamingPlan) optPlan).getJobGraph();
		} else {
			JobGraphGenerator gen = new JobGraphGenerator();
			job = gen.compileJobGraph((OptimizedPlan) optPlan);
		}

		for (File jar : jarFiles) {
			job.addJar(new Path(jar.getAbsolutePath()));
		}

		return job;
	}

	public JobSubmissionResult run(final PackagedProgram prog, int parallelism, boolean wait) throws ProgramInvocationException {
		Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());
		if (prog.isUsingProgramEntryPoint()) {
			return run(prog.getPlanWithJars(), parallelism, wait);
		}
		else if (prog.isUsingInteractiveMode()) {
			LOG.info("Starting program in interactive mode");
			ContextEnvironment.setAsContext(this, prog.getAllLibraries(), prog.getUserCodeClassLoader(), parallelism, wait);
			ContextEnvironment.enableLocalExecution(false);

			// invoke here
			try {
				prog.invokeInteractiveModeForExecution();
			}
			finally {
				ContextEnvironment.enableLocalExecution(true);
			}

			return new JobSubmissionResult(lastJobId);
		}
		else {
			throw new RuntimeException();
		}
	}
	
	public JobSubmissionResult run(PackagedProgram prog, OptimizedPlan optimizedPlan, boolean wait) throws ProgramInvocationException {
		return run(optimizedPlan, prog.getAllLibraries(), wait);

	}
	
	/**
	 * Runs a program on Flink cluster whose job-manager is configured in this client's configuration.
	 * This method involves all steps, from compiling, job-graph generation to submission.
	 * 
	 * @param prog The program to be executed.
	 * @param parallelism The default parallelism to use when running the program. The default parallelism is used
	 *                    when the program does not set a parallelism by itself.
	 * @param wait A flag that indicates whether this function call should block until the program execution is done.
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the program could not be instantiated from its jar file,
	 *                                    or if the submission failed. That might be either due to an I/O problem,
	 *                                    i.e. the job-manager is unreachable, or due to the fact that the
	 *                                    parallel execution failed.
	 */
	public JobSubmissionResult run(JobWithJars prog, int parallelism, boolean wait) throws CompilerException, ProgramInvocationException {
		return run((OptimizedPlan) getOptimizedPlan(prog, parallelism), prog.getJarFiles(), wait);
	}
	

	public JobSubmissionResult run(OptimizedPlan compiledPlan, List<File> libraries, boolean wait) throws ProgramInvocationException {
		JobGraph job = getJobGraph(compiledPlan, libraries);
		this.lastJobId = job.getJobID();
		return run(job, wait);
	}

	public JobSubmissionResult run(JobGraph jobGraph, boolean wait) throws ProgramInvocationException {
		this.lastJobId = jobGraph.getJobID();
		final String hostname = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		if (hostname == null) {
			throw new ProgramInvocationException("Could not find hostname of job manager.");
		}

		FiniteDuration timeout = AkkaUtils.getTimeout(configuration);

		final ActorSystem actorSystem;
		final ActorRef client;

		try {
			Tuple2<ActorSystem, ActorRef> pair = JobClient.startActorSystemAndActor(configuration, false);
			actorSystem = pair._1();
			client = pair._2();
		}
		catch (Exception e) {
			throw new ProgramInvocationException("Could not build up connection to JobManager.", e);
		}

		try {
			JobClient.uploadJarFiles(jobGraph, hostname, client, timeout);
		}
		catch (IOException e) {
			throw new ProgramInvocationException("Could not upload the program's JAR files to the JobManager.", e);
		}

		try{
			if (wait) {
				SerializedJobExecutionResult result =
						JobClient.submitJobAndWait(jobGraph, printStatusDuringExecution, client, timeout);
				try {
					return result.toJobExecutionResult(this.userCodeClassLoader);
				}
				catch (Exception e) {
					throw new ProgramInvocationException(
							"Failed to deserialize the accumulator result after the job execution", e);
				}
			}
			else {
				JobClient.submitJobDetached(jobGraph, client, timeout);
				// return a "Fake" execution result with the JobId
				return new JobSubmissionResult(jobGraph.getJobID());
			}
		}
		catch (JobExecutionException e) {
			throw new ProgramInvocationException("The program execution failed: " + e.getMessage(), e);
		}
		catch (Exception e) {
			throw new ProgramInvocationException("Exception during program execution.", e);
		}
		finally {
			actorSystem.shutdown();
			actorSystem.awaitTermination();
		}
	}

	// --------------------------------------------------------------------------------------------
	
	public static final class OptimizerPlanEnvironment extends ExecutionEnvironment {
		
		private final Optimizer compiler;
		
		private FlinkPlan optimizerPlan;
		
		
		private OptimizerPlanEnvironment(Optimizer compiler) {
			this.compiler = compiler;
		}
		
		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			Plan plan = createProgramPlan(jobName);
			this.optimizerPlan = compiler.compile(plan);
			
			// do not go on with anything now!
			throw new ProgramAbortException();
		}

		@Override
		public String getExecutionPlan() throws Exception {
			Plan plan = createProgramPlan(null, false);
			this.optimizerPlan = compiler.compile(plan);
			
			// do not go on with anything now!
			throw new ProgramAbortException();
		}
		
		private void setAsContext() {
			ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
				
				@Override
				public ExecutionEnvironment createExecutionEnvironment() {
					return OptimizerPlanEnvironment.this;
				}
			};
			initializeContextEnvironment(factory);
		}
		
		public void setPlan(FlinkPlan plan){
			this.optimizerPlan = plan;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * A special exception used to abort programs when the caller is only interested in the
	 * program plan, rather than in the full execution.
	 */
	public static final class ProgramAbortException extends Error {
		private static final long serialVersionUID = 1L;
	}
}
