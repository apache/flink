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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsErroneous;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsFound;
import org.apache.flink.runtime.messages.accumulators.RequestAccumulatorResults;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorSystem;

/**
 * Encapsulates the functionality necessary to submit a program to a remote cluster.
 */
public class Client {

	private static final Logger LOG = LoggerFactory.getLogger(Client.class);

	/** The optimizer used in the optimization of batch programs */
	final Optimizer compiler;

	/** The actor system used to communicate with the JobManager */
	private final ActorSystem actorSystem;

	/** Configuration of the client */
	private final Configuration config;

	/** Timeout for futures */
	private final FiniteDuration timeout;

	/** Lookup timeout for the job manager retrieval service */
	private final FiniteDuration lookupTimeout;

	/**
	 * If != -1, this field specifies the total number of available slots on the cluster
	 * connected to the client.
	 */
	private final int maxSlots;

	/** Flag indicating whether to sysout print execution updates */
	private boolean printStatusDuringExecution = true;

	/**
	 * For interactive invocations, the Job ID is only available after the ContextEnvironment has
	 * been run inside the user JAR. We pass the Client to every instance of the ContextEnvironment
	 * which lets us access the last JobID here.
	 */
	private JobID lastJobID;

	// ------------------------------------------------------------------------
	//                            Construction
	// ------------------------------------------------------------------------

	/**
	 * Creates a instance that submits the programs to the JobManager defined in the
	 * configuration. This method will try to resolve the JobManager hostname and throw an exception
	 * if that is not possible.
	 *
	 * @param config The config used to obtain the job-manager's address, and used to configure the optimizer.
	 *
	 * @throws java.io.IOException Thrown, if the client's actor system could not be started.
	 * @throws java.net.UnknownHostException Thrown, if the JobManager's hostname could not be resolved.
	 */
	public Client(Configuration config) throws IOException {
		this(config, -1);
	}

	/**
	 * Creates a new instance of the class that submits the jobs to a job-manager.
	 * at the given address using the default port.
	 *
	 * @param config The configuration for the client-side processes, like the optimizer.
	 * @param maxSlots maxSlots The number of maxSlots on the cluster if != -1.
	 *
	 * @throws java.io.IOException Thrown, if the client's actor system could not be started.
	 * @throws java.net.UnknownHostException Thrown, if the JobManager's hostname could not be resolved.
	 */
	public Client(Configuration config, int maxSlots) throws IOException {
		this.config = Preconditions.checkNotNull(config);
		this.compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), config);
		this.maxSlots = maxSlots;

		LOG.info("Starting client actor system");

		try {
			this.actorSystem = JobClient.startJobClientActorSystem(config);
		} catch (Exception e) {
			throw new IOException("Could start client actor system.", e);
		}

		timeout = AkkaUtils.getClientTimeout(config);
		lookupTimeout = AkkaUtils.getLookupTimeout(config);
	}

	// ------------------------------------------------------------------------
	//  Startup & Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Shuts down the client. This stops the internal actor system and actors.
	 */
	public void shutdown() {
		if (!this.actorSystem.isTerminated()) {
			this.actorSystem.shutdown();
			this.actorSystem.awaitTermination();
		}
	}

	// ------------------------------------------------------------------------
	//  Configuration
	// ------------------------------------------------------------------------

	/**
	 * Configures whether the client should print progress updates during the execution to {@code System.out}.
	 * All updates are logged via the SLF4J loggers regardless of this setting.
	 *
	 * @param print True to print updates to standard out during execution, false to not print them.
	 */
	public void setPrintStatusDuringExecution(boolean print) {
		this.printStatusDuringExecution = print;
	}

	/**
	 * @return whether the client will print progress updates during the execution to {@code System.out}
	 */
	public boolean getPrintStatusDuringExecution() {
		return this.printStatusDuringExecution;
	}

	/**
	 * @return -1 if unknown. The maximum number of available processing slots at the Flink cluster
	 * connected to this client.
	 */
	public int getMaxSlots() {
		return this.maxSlots;
	}

	// ------------------------------------------------------------------------
	//  Access to the Program's Plan
	// ------------------------------------------------------------------------

	public static String getOptimizedPlanAsJson(Optimizer compiler, PackagedProgram prog, int parallelism)
			throws CompilerException, ProgramInvocationException
	{
		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON((OptimizedPlan) getOptimizedPlan(compiler, prog, parallelism));
	}

	public static FlinkPlan getOptimizedPlan(Optimizer compiler, PackagedProgram prog, int parallelism)
			throws CompilerException, ProgramInvocationException
	{
		Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());
		if (prog.isUsingProgramEntryPoint()) {
			return getOptimizedPlan(compiler, prog.getPlanWithJars(), parallelism);
		} else if (prog.isUsingInteractiveMode()) {
			// temporary hack to support the optimizer plan preview
			OptimizerPlanEnvironment env = new OptimizerPlanEnvironment(compiler);
			if (parallelism > 0) {
				env.setParallelism(parallelism);
			}

			return env.getOptimizedPlan(prog);
		} else {
			throw new RuntimeException("Couldn't determine program mode.");
		}
	}

	public static OptimizedPlan getOptimizedPlan(Optimizer compiler, Plan p, int parallelism) throws CompilerException {
		if (parallelism > 0 && p.getDefaultParallelism() <= 0) {
			LOG.debug("Changing plan default parallelism from {} to {}", p.getDefaultParallelism(), parallelism);
			p.setDefaultParallelism(parallelism);
		}
		LOG.debug("Set parallelism {}, plan default parallelism {}", parallelism, p.getDefaultParallelism());

		return compiler.compile(p);
	}

	// ------------------------------------------------------------------------
	//  Program submission / execution
	// ------------------------------------------------------------------------

	public JobSubmissionResult runBlocking(PackagedProgram prog, int parallelism) throws ProgramInvocationException {
		Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());
		if (prog.isUsingProgramEntryPoint()) {
			return runBlocking(prog.getPlanWithJars(), parallelism, prog.getSavepointPath());
		}
		else if (prog.isUsingInteractiveMode()) {
			LOG.info("Starting program in interactive mode");
			ContextEnvironment.setAsContext(new ContextEnvironmentFactory(this, prog.getAllLibraries(),
					prog.getClasspaths(), prog.getUserCodeClassLoader(), parallelism, true,
					prog.getSavepointPath()));

			// invoke here
			try {
				prog.invokeInteractiveModeForExecution();
			}
			finally {
				ContextEnvironment.unsetContext();
			}

			return new JobSubmissionResult(lastJobID);
		}
		else {
			throw new RuntimeException();
		}
	}

	public JobSubmissionResult runDetached(PackagedProgram prog, int parallelism)
			throws ProgramInvocationException
	{
		Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());
		if (prog.isUsingProgramEntryPoint()) {
			return runDetached(prog.getPlanWithJars(), parallelism, prog.getSavepointPath());
		}
		else if (prog.isUsingInteractiveMode()) {
			LOG.info("Starting program in interactive mode");
			ContextEnvironmentFactory factory = new ContextEnvironmentFactory(this, prog.getAllLibraries(),
					prog.getClasspaths(), prog.getUserCodeClassLoader(), parallelism, false,
					prog.getSavepointPath());
			ContextEnvironment.setAsContext(factory);

			// invoke here
			try {
				prog.invokeInteractiveModeForExecution();
				return ((DetachedEnvironment) factory.getLastEnvCreated()).finalizeExecute();
			}
			finally {
				ContextEnvironment.unsetContext();
			}
		}
		else {
			throw new RuntimeException("PackagedProgram does not have a valid invocation mode.");
		}
	}

	public JobExecutionResult runBlocking(JobWithJars program, int parallelism) throws ProgramInvocationException {
		return runBlocking(program, parallelism, null);
	}

	/**
	 * Runs a program on the Flink cluster to which this client is connected. The call blocks until the
	 * execution is complete, and returns afterwards.
	 *
	 * @param program The program to be executed.
	 * @param parallelism The default parallelism to use when running the program. The default parallelism is used
	 *                    when the program does not set a parallelism by itself.
	 *
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the program could not be instantiated from its jar file,
	 *                                    or if the submission failed. That might be either due to an I/O problem,
	 *                                    i.e. the job-manager is unreachable, or due to the fact that the
	 *                                    parallel execution failed.
	 */
	public JobExecutionResult runBlocking(JobWithJars program, int parallelism, String savepointPath)
			throws CompilerException, ProgramInvocationException {
		ClassLoader classLoader = program.getUserCodeClassLoader();
		if (classLoader == null) {
			throw new IllegalArgumentException("The given JobWithJars does not provide a usercode class loader.");
		}

		OptimizedPlan optPlan = getOptimizedPlan(compiler, program, parallelism);
		return runBlocking(optPlan, program.getJarFiles(), program.getClasspaths(), classLoader, savepointPath);
	}

	public JobSubmissionResult runDetached(JobWithJars program, int parallelism) throws ProgramInvocationException {
		return runDetached(program, parallelism, null);
	}

	/**
	 * Submits a program to the Flink cluster to which this client is connected. The call returns after the
	 * program was submitted and does not wait for the program to complete.
	 *
	 * @param program The program to be executed.
	 * @param parallelism The default parallelism to use when running the program. The default parallelism is used
	 *                    when the program does not set a parallelism by itself.
	 *
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the program could not be instantiated from its jar file,
	 *                                    or if the submission failed. That might be either due to an I/O problem,
	 *                                    i.e. the job-manager is unreachable.
	 */
	public JobSubmissionResult runDetached(JobWithJars program, int parallelism, String savepointPath)
			throws CompilerException, ProgramInvocationException {
		ClassLoader classLoader = program.getUserCodeClassLoader();
		if (classLoader == null) {
			throw new IllegalArgumentException("The given JobWithJars does not provide a usercode class loader.");
		}

		OptimizedPlan optimizedPlan = getOptimizedPlan(compiler, program, parallelism);
		return runDetached(optimizedPlan, program.getJarFiles(), program.getClasspaths(), classLoader, savepointPath);
	}

	public JobExecutionResult runBlocking(
			FlinkPlan compiledPlan, List<URL> libraries, List<URL> classpaths, ClassLoader classLoader) throws ProgramInvocationException {
		return runBlocking(compiledPlan, libraries, classpaths, classLoader, null);
	}

	public JobExecutionResult runBlocking(FlinkPlan compiledPlan, List<URL> libraries, List<URL> classpaths,
			ClassLoader classLoader, String savepointPath) throws ProgramInvocationException
	{
		JobGraph job = getJobGraph(compiledPlan, libraries, classpaths, savepointPath);
		return runBlocking(job, classLoader);
	}

	public JobSubmissionResult runDetached(FlinkPlan compiledPlan, List<URL> libraries, List<URL> classpaths, ClassLoader classLoader) throws ProgramInvocationException {
		return runDetached(compiledPlan, libraries, classpaths, classLoader, null);
	}

	public JobSubmissionResult runDetached(FlinkPlan compiledPlan, List<URL> libraries, List<URL> classpaths,
			ClassLoader classLoader, String savepointPath) throws ProgramInvocationException
	{
		JobGraph job = getJobGraph(compiledPlan, libraries, classpaths, savepointPath);
		return runDetached(job, classLoader);
	}

	public JobExecutionResult runBlocking(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		LeaderRetrievalService leaderRetrievalService;
		try {
			leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(config);
		} catch (Exception e) {
			throw new ProgramInvocationException("Could not create the leader retrieval service.", e);
		}

		try {
			this.lastJobID = jobGraph.getJobID();
			return JobClient.submitJobAndWait(actorSystem, leaderRetrievalService, jobGraph, timeout, printStatusDuringExecution, classLoader);
		} catch (JobExecutionException e) {
			throw new ProgramInvocationException("The program execution failed: " + e.getMessage(), e);
		}
	}

	public JobSubmissionResult runDetached(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		ActorGateway jobManagerGateway;

		try {
			jobManagerGateway = getJobManagerGateway();
		} catch (Exception e) {
			throw new ProgramInvocationException("Failed to retrieve the JobManager gateway.", e);
		}

		LOG.info("Checking and uploading JAR files");
		try {
			JobClient.uploadJarFiles(jobGraph, jobManagerGateway, timeout);
		}
		catch (IOException e) {
			throw new ProgramInvocationException("Could not upload the program's JAR files to the JobManager.", e);
		}
		try {
			this.lastJobID = jobGraph.getJobID();
			JobClient.submitJobDetached(jobManagerGateway, jobGraph, timeout, classLoader);
			return new JobSubmissionResult(jobGraph.getJobID());
		} catch (JobExecutionException e) {
				throw new ProgramInvocationException("The program execution failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Cancels a job identified by the job id.
	 * @param jobId the job id
	 * @throws Exception In case an error occurred.
	 */
	public void cancel(JobID jobId) throws Exception {
		final ActorGateway jobManagerGateway = getJobManagerGateway();

		final Future<Object> response;
		try {
			response = jobManagerGateway.ask(new JobManagerMessages.CancelJob(jobId), timeout);
		} catch (final Exception e) {
			throw new ProgramInvocationException("Failed to query the job manager gateway.", e);
		}

		final Object result = Await.result(response, timeout);

		if (result instanceof JobManagerMessages.CancellationSuccess) {
			LOG.info("Job cancellation with ID " + jobId + " succeeded.");
		} else if (result instanceof JobManagerMessages.CancellationFailure) {
			final Throwable t = ((JobManagerMessages.CancellationFailure) result).cause();
			LOG.info("Job cancellation with ID " + jobId + " failed.", t);
			throw new Exception("Failed to cancel the job because of \n" + t.getMessage());
		} else {
			throw new Exception("Unknown message received while cancelling: " + result.getClass().getName());
		}
	}

	/**
	 * Stops a program on Flink cluster whose job-manager is configured in this client's configuration.
	 * Stopping works only for streaming programs. Be aware, that the program might continue to run for
	 * a while after sending the stop command, because after sources stopped to emit data all operators
	 * need to finish processing.
	 * 
	 * @param jobId
	 *            the job ID of the streaming program to stop
	 * @throws Exception
	 *             If the job ID is invalid (ie, is unknown or refers to a batch job) or if sending the stop signal
	 *             failed. That might be due to an I/O problem, ie, the job-manager is unreachable.
	 */
	public void stop(final JobID jobId) throws Exception {
		final ActorGateway jobManagerGateway = getJobManagerGateway();

		final Future<Object> response;
		try {
			response = jobManagerGateway.ask(new JobManagerMessages.StopJob(jobId), timeout);
		} catch (final Exception e) {
			throw new ProgramInvocationException("Failed to query the job manager gateway.", e);
		}

		final Object result = Await.result(response, timeout);

		if (result instanceof JobManagerMessages.StoppingSuccess) {
			LOG.info("Job stopping with ID " + jobId + " succeeded.");
		} else if (result instanceof JobManagerMessages.StoppingFailure) {
			final Throwable t = ((JobManagerMessages.StoppingFailure) result).cause();
			LOG.info("Job stopping with ID " + jobId + " failed.", t);
			throw new Exception("Failed to stop the job because of \n" + t.getMessage());
		} else {
			throw new Exception("Unknown message received while stopping: " + result.getClass().getName());
		}
	}

	/**
	 * Requests and returns the accumulators for the given job identifier. Accumulators can be
	 * requested while a is running or after it has finished. The default class loader is used
	 * to deserialize the incoming accumulator results.
	 * @param jobID The job identifier of a job.
	 * @return A Map containing the accumulator's name and its value.
	 */
	public Map<String, Object> getAccumulators(JobID jobID) throws Exception {
		return getAccumulators(jobID, ClassLoader.getSystemClassLoader());
	}

	/**
	 * Requests and returns the accumulators for the given job identifier. Accumulators can be
	 * requested while a is running or after it has finished.
	 * @param jobID The job identifier of a job.
	 * @param loader The class loader for deserializing the accumulator results.
	 * @return A Map containing the accumulator's name and its value.
	 */
	public Map<String, Object> getAccumulators(JobID jobID, ClassLoader loader) throws Exception {
		ActorGateway jobManagerGateway = getJobManagerGateway();

		Future<Object> response;
		try {
			response = jobManagerGateway.ask(new RequestAccumulatorResults(jobID), timeout);
		} catch (Exception e) {
			throw new Exception("Failed to query the job manager gateway for accumulators.", e);
		}

		Object result = Await.result(response, timeout);

		if (result instanceof AccumulatorResultsFound) {
			Map<String, SerializedValue<Object>> serializedAccumulators =
					((AccumulatorResultsFound) result).result();

			return AccumulatorHelper.deserializeAccumulators(serializedAccumulators, loader);

		} else if (result instanceof AccumulatorResultsErroneous) {
			throw ((AccumulatorResultsErroneous) result).cause();
		} else {
			throw new Exception("Failed to fetch accumulators for the job " + jobID + ".");
		}
	}


	// ------------------------------------------------------------------------
	//  Sessions
	// ------------------------------------------------------------------------

	/**
	 * Tells the JobManager to finish the session (job) defined by the given ID.
	 * 
	 * @param jobId The ID that identifies the session.
	 */
	public void endSession(JobID jobId) throws Exception {
		if (jobId == null) {
			throw new IllegalArgumentException("The JobID must not be null.");
		}
		endSessions(Collections.singletonList(jobId));
	}

	/**
	 * Tells the JobManager to finish the sessions (jobs) defined by the given IDs.
	 *
	 * @param jobIds The IDs that identify the sessions.
	 */
	public void endSessions(List<JobID> jobIds) throws Exception {
		if (jobIds == null) {
			throw new IllegalArgumentException("The JobIDs must not be null");
		}

		ActorGateway jobManagerGateway = getJobManagerGateway();
		
		for (JobID jid : jobIds) {
			if (jid != null) {
				LOG.info("Telling job manager to end the session {}.", jid);
				jobManagerGateway.tell(new JobManagerMessages.RemoveCachedJob(jid));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Internal translation methods
	// ------------------------------------------------------------------------

	/**
	 * Creates the optimized plan for a given program, using this client's compiler.
	 *
	 * @param prog The program to be compiled.
	 * @return The compiled and optimized plan, as returned by the compiler.
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the program could not be instantiated from its jar file.
	 */
	private static OptimizedPlan getOptimizedPlan(Optimizer compiler, JobWithJars prog, int parallelism)
			throws CompilerException, ProgramInvocationException {
		return getOptimizedPlan(compiler, prog.getPlan(), parallelism);
	}

	public JobGraph getJobGraph(PackagedProgram prog, FlinkPlan optPlan) throws ProgramInvocationException {
		return getJobGraph(optPlan, prog.getAllLibraries(), prog.getClasspaths(), null);
	}

	public JobGraph getJobGraph(PackagedProgram prog, FlinkPlan optPlan, String savepointPath) throws ProgramInvocationException {
		return getJobGraph(optPlan, prog.getAllLibraries(), prog.getClasspaths(), savepointPath);
	}

	private JobGraph getJobGraph(FlinkPlan optPlan, List<URL> jarFiles, List<URL> classpaths, String savepointPath) {
		JobGraph job;
		if (optPlan instanceof StreamingPlan) {
			job = ((StreamingPlan) optPlan).getJobGraph();
			job.setSavepointPath(savepointPath);
		} else {
			JobGraphGenerator gen = new JobGraphGenerator(this.config);
			job = gen.compileJobGraph((OptimizedPlan) optPlan);
		}

		for (URL jar : jarFiles) {
			try {
				job.addJar(new Path(jar.toURI()));
			} catch (URISyntaxException e) {
				throw new RuntimeException("URL is invalid. This should not happen.", e);
			}
		}
 
		job.setClasspaths(classpaths);

		return job;
	}

	// ------------------------------------------------------------------------
	//  Helper methods
	// ------------------------------------------------------------------------

	/**
	 * Returns the {@link ActorGateway} of the current job manager leader using
	 * the {@link LeaderRetrievalService}.
	 *
	 * @return ActorGateway of the current job manager leader
	 * @throws Exception
	 */
	private ActorGateway getJobManagerGateway() throws Exception {
		LOG.info("Looking up JobManager");
		LeaderRetrievalService leaderRetrievalService;

		leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(config);

		return LeaderRetrievalUtils.retrieveLeaderGateway(
			leaderRetrievalService,
			actorSystem,
			lookupTimeout);
	}

}
