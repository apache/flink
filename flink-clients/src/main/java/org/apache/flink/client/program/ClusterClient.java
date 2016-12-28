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

import akka.actor.ActorSystem;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobListeningContext;
import org.apache.flink.runtime.client.JobRetrievalException;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsErroneous;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsFound;
import org.apache.flink.runtime.messages.accumulators.RequestAccumulatorResults;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Encapsulates the functionality necessary to submit a program to a remote cluster.
 */
public abstract class ClusterClient {

	private final Logger LOG = LoggerFactory.getLogger(getClass());

	/** The optimizer used in the optimization of batch programs */
	final Optimizer compiler;

	/** The actor system used to communicate with the JobManager. Lazily initialized upon first use */
	protected final LazyActorSystemLoader actorSystemLoader;

	/** Configuration of the client */
	protected final Configuration flinkConfig;

	/** Timeout for futures */
	protected final FiniteDuration timeout;

	/** Lookup timeout for the job manager retrieval service */
	private final FiniteDuration lookupTimeout;

	/** Flag indicating whether to sysout print execution updates */
	private boolean printStatusDuringExecution = true;

	/**
	 * For interactive invocations, the job results are only available after the ContextEnvironment has
	 * been run inside the user JAR. We pass the Client to every instance of the ContextEnvironment
	 * which lets us access the execution result here.
	 */
	private JobExecutionResult lastJobExecutionResult;

	/** Switch for blocking/detached job submission of the client */
	private boolean detachedJobSubmission = false;

	// ------------------------------------------------------------------------
	//                            Construction
	// ------------------------------------------------------------------------

	/**
	 * Creates a instance that submits the programs to the JobManager defined in the
	 * configuration. This method will try to resolve the JobManager hostname and throw an exception
	 * if that is not possible.
	 *
	 * @param flinkConfig The config used to obtain the job-manager's address, and used to configure the optimizer.
	 *
	 * @throws java.io.IOException Thrown, if the client's actor system could not be started.
	 */
	public ClusterClient(Configuration flinkConfig) throws IOException {

		this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
		this.compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), flinkConfig);

		this.timeout = AkkaUtils.getClientTimeout(flinkConfig);
		this.lookupTimeout = AkkaUtils.getLookupTimeout(flinkConfig);

		this.actorSystemLoader = new LazyActorSystemLoader(flinkConfig, LOG);
	}

	// ------------------------------------------------------------------------
	//  Startup & Shutdown
	// ------------------------------------------------------------------------

	protected static class LazyActorSystemLoader {

		private final Logger LOG;

		private final Configuration flinkConfig;

		private ActorSystem actorSystem;

		private LazyActorSystemLoader(Configuration flinkConfig, Logger LOG) {
			this.flinkConfig = flinkConfig;
			this.LOG = LOG;
		}

		/**
		 * Indicates whether the ActorSystem has already been instantiated.
		 * @return boolean True if it exists, False otherwise
		 */
		public boolean isLoaded() {
			return actorSystem != null;
		}

		public void shutdown() {
			if (isLoaded()) {
				actorSystem.shutdown();
				actorSystem.awaitTermination();
				actorSystem = null;
			}
		}

		/**
		 * Creates a new ActorSystem or returns an existing one.
		 * @return ActorSystem
		 */
		public ActorSystem get() {

			if (!isLoaded()) {
				// start actor system
				LOG.info("Starting client actor system.");

				String hostName = flinkConfig.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
				int port = flinkConfig.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);
				if (hostName == null || port == -1) {
					throw new RuntimeException("The initial JobManager address has not been set correctly.");
				}
				InetSocketAddress initialJobManagerAddress = new InetSocketAddress(hostName, port);

				// find name of own public interface, able to connect to the JM
				// try to find address for 2 seconds. log after 400 ms.
				InetAddress ownHostname;
				try {
					ownHostname = ConnectionUtils.findConnectingAddress(initialJobManagerAddress, 2000, 400);
				} catch (IOException e) {
					throw new RuntimeException("Failed to resolve JobManager address at " + initialJobManagerAddress, e);
				}
				actorSystem = AkkaUtils.createActorSystem(flinkConfig,
					new Some<>(new Tuple2<String, Object>(ownHostname.getCanonicalHostName(), 0)));
			}

			return actorSystem;
		}

	}

	/**
	 * Shuts down the client. This stops the internal actor system and actors.
	 */
	public void shutdown() {
		synchronized (this) {
			try {
				finalizeCluster();
			} finally {
				this.actorSystemLoader.shutdown();
			}
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
	 * Gets the current JobManager address (may change in case of a HA setup).
	 * @return The address (host and port) of the leading JobManager
	 */
	public InetSocketAddress getJobManagerAddress() {
		try {
			LeaderConnectionInfo leaderConnectionInfo =
				LeaderRetrievalUtils.retrieveLeaderConnectionInfo(
					LeaderRetrievalUtils.createLeaderRetrievalService(flinkConfig), timeout);

			return AkkaUtils.getInetSockeAddressFromAkkaURL(leaderConnectionInfo.getAddress());
		} catch (Exception e) {
			throw new RuntimeException("Failed to retrieve JobManager address", e);
		}
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
		Logger log = LoggerFactory.getLogger(ClusterClient.class);

		if (parallelism > 0 && p.getDefaultParallelism() <= 0) {
			log.debug("Changing plan default parallelism from {} to {}", p.getDefaultParallelism(), parallelism);
			p.setDefaultParallelism(parallelism);
		}
		log.debug("Set parallelism {}, plan default parallelism {}", parallelism, p.getDefaultParallelism());

		return compiler.compile(p);
	}

	// ------------------------------------------------------------------------
	//  Program submission / execution
	// ------------------------------------------------------------------------

	/**
	 * General purpose method to run a user jar from the CliFrontend in either blocking or detached mode, depending
	 * on whether {@code setDetached(true)} or {@code setDetached(false)}.
	 * @param prog the packaged program
	 * @param parallelism the parallelism to execute the contained Flink job
	 * @return The result of the execution
	 * @throws ProgramMissingJobException
	 * @throws ProgramInvocationException
	 */
	public JobSubmissionResult run(PackagedProgram prog, int parallelism)
			throws ProgramInvocationException, ProgramMissingJobException
	{
		Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());
		if (prog.isUsingProgramEntryPoint()) {

			final JobWithJars jobWithJars;
			if (hasUserJarsInClassPath(prog.getAllLibraries())) {
				jobWithJars = prog.getPlanWithoutJars();
			} else {
				jobWithJars = prog.getPlanWithJars();
			}

			return run(jobWithJars, parallelism, prog.getSavepointSettings());
		}
		else if (prog.isUsingInteractiveMode()) {
			LOG.info("Starting program in interactive mode");

			final List<URL> libraries;
			if (hasUserJarsInClassPath(prog.getAllLibraries())) {
				libraries = Collections.emptyList();
			} else {
				libraries = prog.getAllLibraries();
			}

			ContextEnvironmentFactory factory = new ContextEnvironmentFactory(this, libraries,
					prog.getClasspaths(), prog.getUserCodeClassLoader(), parallelism, isDetached(),
					prog.getSavepointSettings());
			ContextEnvironment.setAsContext(factory);

			try {
				// invoke main method
				prog.invokeInteractiveModeForExecution();
				if (lastJobExecutionResult == null && factory.getLastEnvCreated() == null) {
					throw new ProgramMissingJobException();
				}
				if (isDetached()) {
					// in detached mode, we execute the whole user code to extract the Flink job, afterwards we run it here
					return ((DetachedEnvironment) factory.getLastEnvCreated()).finalizeExecute();
				}
				else {
					// in blocking mode, we execute all Flink jobs contained in the user code and then return here
					return this.lastJobExecutionResult;
				}
			}
			finally {
				ContextEnvironment.unsetContext();
			}
		}
		else {
			throw new ProgramInvocationException("PackagedProgram does not have a valid invocation mode.");
		}
	}

	public JobSubmissionResult run(JobWithJars program, int parallelism) throws ProgramInvocationException {
		return run(program, parallelism, SavepointRestoreSettings.none());
	}

	/**
	 * Runs a program on the Flink cluster to which this client is connected. The call blocks until the
	 * execution is complete, and returns afterwards.
	 *
	 * @param jobWithJars The program to be executed.
	 * @param parallelism The default parallelism to use when running the program. The default parallelism is used
	 *                    when the program does not set a parallelism by itself.
	 *
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
	 * @throws ProgramInvocationException Thrown, if the program could not be instantiated from its jar file,
	 *                                    or if the submission failed. That might be either due to an I/O problem,
	 *                                    i.e. the job-manager is unreachable, or due to the fact that the
	 *                                    parallel execution failed.
	 */
	public JobSubmissionResult run(JobWithJars jobWithJars, int parallelism, SavepointRestoreSettings savepointSettings)
			throws CompilerException, ProgramInvocationException {
		ClassLoader classLoader = jobWithJars.getUserCodeClassLoader();
		if (classLoader == null) {
			throw new IllegalArgumentException("The given JobWithJars does not provide a usercode class loader.");
		}

		OptimizedPlan optPlan = getOptimizedPlan(compiler, jobWithJars, parallelism);
		return run(optPlan, jobWithJars.getJarFiles(), jobWithJars.getClasspaths(), classLoader, savepointSettings);
	}

	public JobSubmissionResult run(
			FlinkPlan compiledPlan, List<URL> libraries, List<URL> classpaths, ClassLoader classLoader) throws ProgramInvocationException {
		return run(compiledPlan, libraries, classpaths, classLoader, SavepointRestoreSettings.none());
	}

	public JobSubmissionResult run(FlinkPlan compiledPlan,
			List<URL> libraries, List<URL> classpaths, ClassLoader classLoader, SavepointRestoreSettings savepointSettings)
		throws ProgramInvocationException
	{
		JobGraph job = getJobGraph(compiledPlan, libraries, classpaths, savepointSettings);
		return submitJob(job, classLoader);
	}

	/**
	 * Submits a JobGraph blocking.
	 * @param jobGraph The JobGraph
	 * @param classLoader User code class loader to deserialize the results and errors (may contain custom classes).
	 * @return JobExecutionResult
	 * @throws ProgramInvocationException
	 */
	public JobExecutionResult run(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {

		waitForClusterToBeReady();

		final LeaderRetrievalService leaderRetrievalService;
		try {
			leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(flinkConfig, true);
		} catch (Exception e) {
			throw new ProgramInvocationException("Could not create the leader retrieval service", e);
		}

		try {
			logAndSysout("Submitting job with JobID: " + jobGraph.getJobID() + ". Waiting for job completion.");
			this.lastJobExecutionResult = JobClient.submitJobAndWait(actorSystemLoader.get(), flinkConfig,
				leaderRetrievalService, jobGraph, timeout, printStatusDuringExecution, classLoader);
			return this.lastJobExecutionResult;
		} catch (JobExecutionException e) {
			throw new ProgramInvocationException("The program execution failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Submits a JobGraph detached.
	 * @param jobGraph The JobGraph
	 * @param classLoader User code class loader to deserialize the results and errors (may contain custom classes).
	 * @return JobSubmissionResult
	 * @throws ProgramInvocationException
	 */
	public JobSubmissionResult runDetached(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {

		waitForClusterToBeReady();

		final ActorGateway jobManagerGateway;
		try {
			jobManagerGateway = getJobManagerGateway();
		} catch (Exception e) {
			throw new ProgramInvocationException("Failed to retrieve the JobManager gateway.", e);
		}

		try {
			logAndSysout("Submitting Job with JobID: " + jobGraph.getJobID() + ". Returning after job submission.");
			JobClient.submitJobDetached(jobManagerGateway, flinkConfig, jobGraph, timeout, classLoader);
			return new JobSubmissionResult(jobGraph.getJobID());
		} catch (JobExecutionException e) {
			throw new ProgramInvocationException("The program execution failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Reattaches to a running from from the supplied job id
	 * @param jobID The job id of the job to attach to
	 * @return The JobExecutionResult for the jobID
	 * @throws JobExecutionException if an error occurs during monitoring the job execution
	 */
	public JobExecutionResult retrieveJob(JobID jobID) throws JobExecutionException {
		final LeaderRetrievalService leaderRetrievalService;
		try {
			leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(flinkConfig);
		} catch (Exception e) {
			throw new JobRetrievalException(jobID, "Could not create the leader retrieval service", e);
		}

		ActorGateway jobManagerGateway;
		try {
			jobManagerGateway = getJobManagerGateway();
		} catch (Exception e) {
			throw new JobRetrievalException(jobID, "Could not retrieve the JobManager Gateway");
		}

		final JobListeningContext listeningContext = JobClient.attachToRunningJob(
				jobID,
				jobManagerGateway,
				flinkConfig,
				actorSystemLoader.get(),
				leaderRetrievalService,
				timeout,
				printStatusDuringExecution);

		return JobClient.awaitJobResult(listeningContext);
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
			logAndSysout("Job cancellation with ID " + jobId + " succeeded.");
		} else if (result instanceof JobManagerMessages.CancellationFailure) {
			final Throwable t = ((JobManagerMessages.CancellationFailure) result).cause();
			logAndSysout("Job cancellation with ID " + jobId + " failed because of " + t.getMessage());
			throw new Exception("Failed to cancel the job with id " + jobId, t);
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

	public JobGraph getJobGraph(PackagedProgram prog, FlinkPlan optPlan, SavepointRestoreSettings savepointSettings) throws ProgramInvocationException {
		return getJobGraph(optPlan, prog.getAllLibraries(), prog.getClasspaths(), savepointSettings);
	}

	private JobGraph getJobGraph(FlinkPlan optPlan, List<URL> jarFiles, List<URL> classpaths, SavepointRestoreSettings savepointSettings) {
		JobGraph job;
		if (optPlan instanceof StreamingPlan) {
			job = ((StreamingPlan) optPlan).getJobGraph();
			job.setSavepointRestoreSettings(savepointSettings);
		} else {
			JobGraphGenerator gen = new JobGraphGenerator(this.flinkConfig);
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
	public ActorGateway getJobManagerGateway() throws Exception {
		LOG.debug("Looking up JobManager");
		return LeaderRetrievalUtils.retrieveLeaderGateway(
			LeaderRetrievalUtils.createLeaderRetrievalService(flinkConfig),
			actorSystemLoader.get(),
			lookupTimeout);
	}

	/**
	 * Logs and prints to sysout if printing to stdout is enabled.
	 * @param message The message to log/print
	 */
	protected void logAndSysout(String message) {
		LOG.info(message);
		if (printStatusDuringExecution) {
			System.out.println(message);
		}
	}

	// ------------------------------------------------------------------------
	//  Abstract methods to be implemented by the cluster specific Client
	// ------------------------------------------------------------------------

	/**
	 * Blocks until the client has determined that the cluster is ready for Job submission.
	 *
	 * This is delayed until right before job submission to report any other errors first
	 * (e.g. invalid job definitions/errors in the user jar)
	 */
	public abstract void waitForClusterToBeReady();

	/**
	 * Returns an URL (as a string) to the JobManager web interface
	 */
	public abstract String getWebInterfaceURL();

	/**
	 * Returns the latest cluster status, with number of Taskmanagers and slots
	 */
	public abstract GetClusterStatusResponse getClusterStatus();

	/**
	 * May return new messages from the cluster.
	 * Messages can be for example about failed containers or container launch requests.
	 */
	protected abstract List<String> getNewMessages();

	/**
	 * Returns a string representation of the cluster.
	 */
	public abstract String getClusterIdentifier();

	/**
	 * Request the cluster to shut down or disconnect.
	 */
	protected abstract void finalizeCluster();

	/**
	 * Set the mode of this client (detached or blocking job execution).
	 * @param isDetached If true, the client will submit programs detached via the {@code run} method
	 */
	public void setDetached(boolean isDetached) {
		this.detachedJobSubmission = isDetached;
	}

	/**
	 * A flag to indicate whether this clients submits jobs detached.
	 * @return True if the Client submits detached, false otherwise
	 */
	public boolean isDetached() {
		return detachedJobSubmission;
	}

	/**
	 * Return the Flink configuration object
	 * @return The Flink configuration object
	 */
	public Configuration getFlinkConfiguration() {
		return flinkConfig.clone();
	}

	/**
	 * The client may define an upper limit on the number of slots to use
	 * @return -1 if unknown
	 */
	public abstract int getMaxSlots();

	/**
	 * Returns true if the client already has the user jar and providing it again would
	 * result in duplicate uploading of the jar.
	 */
	public abstract boolean hasUserJarsInClassPath(List<URL> userJarFiles);

	/**
	 * Calls the subclasses' submitJob method. It may decide to simply call one of the run methods or it may perform
	 * some custom job submission logic.
	 * @param jobGraph The JobGraph to be submitted
	 * @return JobSubmissionResult
	 */
	protected abstract JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader)
		throws ProgramInvocationException;

}
