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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
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
import org.apache.flink.runtime.akka.AkkaJobManagerGateway;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobListeningContext;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsErroneous;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsFound;
import org.apache.flink.runtime.messages.accumulators.RequestAccumulatorResults;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobDetails;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Encapsulates the functionality necessary to submit a program to a remote cluster.
 *
 * @param <T> type of the cluster id
 */
public abstract class ClusterClient<T> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	/** The optimizer used in the optimization of batch programs. */
	final Optimizer compiler;

	/** The actor system used to communicate with the JobManager. Lazily initialized upon first use */
	protected final LazyActorSystemLoader actorSystemLoader;

	/** Configuration of the client. */
	protected final Configuration flinkConfig;

	/** Timeout for futures. */
	protected final FiniteDuration timeout;

	/** Lookup timeout for the job manager retrieval service. */
	private final FiniteDuration lookupTimeout;

	/** Service factory for high available. */
	protected final HighAvailabilityServices highAvailabilityServices;

	private final boolean sharedHaServices;

	/** Flag indicating whether to sysout print execution updates. */
	private boolean printStatusDuringExecution = true;

	/**
	 * For interactive invocations, the job results are only available after the ContextEnvironment has
	 * been run inside the user JAR. We pass the Client to every instance of the ContextEnvironment
	 * which lets us access the execution result here.
	 */
	protected JobExecutionResult lastJobExecutionResult;

	/** Switch for blocking/detached job submission of the client. */
	private boolean detachedJobSubmission = false;

	/**
	 * Value returned by {@link #getMaxSlots()} if the number of maximum slots is unknown.
	 */
	public static final int MAX_SLOTS_UNKNOWN = -1;

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
	 * @throws Exception we cannot create the high availability services
	 */
	public ClusterClient(Configuration flinkConfig) throws Exception {
		this(
			flinkConfig,
			HighAvailabilityServicesUtils.createHighAvailabilityServices(
				flinkConfig,
				Executors.directExecutor(),
				HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION),
			false);
	}

	/**
	 * Creates a instance that submits the programs to the JobManager defined in the
	 * configuration. This method will try to resolve the JobManager hostname and throw an exception
	 * if that is not possible.
	 *
	 * @param flinkConfig The config used to obtain the job-manager's address, and used to configure the optimizer.
	 * @param highAvailabilityServices HighAvailabilityServices to use for leader retrieval
	 * @param sharedHaServices true if the HighAvailabilityServices are shared and must not be shut down
	 */
	public ClusterClient(Configuration flinkConfig, HighAvailabilityServices highAvailabilityServices, boolean sharedHaServices) {
		this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
		this.compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), flinkConfig);

		this.timeout = AkkaUtils.getClientTimeout(flinkConfig);
		this.lookupTimeout = AkkaUtils.getLookupTimeout(flinkConfig);

		this.actorSystemLoader = new LazyActorSystemLoader(
			highAvailabilityServices,
			Time.milliseconds(lookupTimeout.toMillis()),
			flinkConfig,
			log);

		this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
		this.sharedHaServices = sharedHaServices;
	}

	// ------------------------------------------------------------------------
	//  Startup & Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Utility class to lazily instantiate an {@link ActorSystem}.
	 */
	protected static class LazyActorSystemLoader {

		private final Logger log;

		private final HighAvailabilityServices highAvailabilityServices;

		private final Time timeout;

		private final Configuration configuration;

		private ActorSystem actorSystem;

		private LazyActorSystemLoader(
				HighAvailabilityServices highAvailabilityServices,
				Time timeout,
				Configuration configuration,
				Logger log) {
			this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
			this.timeout = Preconditions.checkNotNull(timeout);
			this.configuration = Preconditions.checkNotNull(configuration);
			this.log = Preconditions.checkNotNull(log);
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
		 * @throws Exception if the ActorSystem could not be created
		 */
		public ActorSystem get() throws FlinkException {

			if (!isLoaded()) {
				// start actor system
				log.info("Starting client actor system.");

				final InetAddress ownHostname;
				try {
					ownHostname = LeaderRetrievalUtils.findConnectingAddress(
						highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
						timeout);
				} catch (LeaderRetrievalException lre) {
					throw new FlinkException("Could not find out our own hostname by connecting to the " +
						"leading JobManager. Please make sure that the Flink cluster has been started.", lre);
				}

				try {
					actorSystem = BootstrapTools.startActorSystem(
						configuration,
						ownHostname.getCanonicalHostName(),
						0,
						log);
				} catch (Exception e) {
					throw new FlinkException("Could not start the ActorSystem lazily.", e);
				}
			}

			return actorSystem;
		}

	}

	/**
	 * Shuts down the client. This stops the internal actor system and actors.
	 */
	public void shutdown() throws Exception {
		synchronized (this) {
			actorSystemLoader.shutdown();

			if (!sharedHaServices && highAvailabilityServices != null) {
				highAvailabilityServices.close();
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
	 * Gets the current cluster connection info (may change in case of a HA setup).
	 *
	 * @return The the connection info to the leader component of the cluster
	 * @throws LeaderRetrievalException if the leader could not be retrieved
	 */
	public LeaderConnectionInfo getClusterConnectionInfo() throws LeaderRetrievalException {
		return LeaderRetrievalUtils.retrieveLeaderConnectionInfo(
			highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
			timeout);
	}

	// ------------------------------------------------------------------------
	//  Access to the Program's Plan
	// ------------------------------------------------------------------------

	public static String getOptimizedPlanAsJson(Optimizer compiler, PackagedProgram prog, int parallelism)
			throws CompilerException, ProgramInvocationException {
		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON((OptimizedPlan) getOptimizedPlan(compiler, prog, parallelism));
	}

	public static FlinkPlan getOptimizedPlan(Optimizer compiler, PackagedProgram prog, int parallelism)
			throws CompilerException, ProgramInvocationException {
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
			throws ProgramInvocationException, ProgramMissingJobException {
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
			log.info("Starting program in interactive mode (detached: {})", isDetached());

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
					throw new ProgramMissingJobException("The program didn't contain a Flink job.");
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
			throws ProgramInvocationException {
		JobGraph job = getJobGraph(flinkConfig, compiledPlan, libraries, classpaths, savepointSettings);
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

		final ActorSystem actorSystem;

		try {
			actorSystem = actorSystemLoader.get();
		} catch (FlinkException fe) {
			throw new ProgramInvocationException("Could not start the ActorSystem needed to talk to the " +
				"JobManager.", fe);
		}

		try {
			logAndSysout("Submitting job with JobID: " + jobGraph.getJobID() + ". Waiting for job completion.");
			this.lastJobExecutionResult = JobClient.submitJobAndWait(
				actorSystem,
				flinkConfig,
				highAvailabilityServices,
				jobGraph,
				timeout,
				printStatusDuringExecution,
				classLoader);

			return lastJobExecutionResult;
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
			JobClient.submitJobDetached(
				new AkkaJobManagerGateway(jobManagerGateway),
				flinkConfig,
				jobGraph,
				Time.milliseconds(timeout.toMillis()),
				classLoader);
			return new JobSubmissionResult(jobGraph.getJobID());
		} catch (JobExecutionException e) {
			throw new ProgramInvocationException("The program execution failed: " + e.getMessage(), e);
		}
	}

	/**
	 * Reattaches to a running from the supplied job id.
	 * @param jobID The job id of the job to attach to
	 * @return The JobExecutionResult for the jobID
	 * @throws JobExecutionException if an error occurs during monitoring the job execution
	 */
	public JobExecutionResult retrieveJob(JobID jobID) throws JobExecutionException {
		final ActorSystem actorSystem;

		try {
			actorSystem = actorSystemLoader.get();
		} catch (FlinkException fe) {
			throw new JobExecutionException(
				jobID,
				"Could not start the ActorSystem needed to talk to the JobManager.",
				fe);
		}

		final JobListeningContext listeningContext = JobClient.attachToRunningJob(
			jobID,
			flinkConfig,
			actorSystem,
			highAvailabilityServices,
			timeout,
			printStatusDuringExecution);

		return JobClient.awaitJobResult(listeningContext);
	}

	/**
	 * Reattaches to a running job with the given job id.
	 *
	 * @param jobID The job id of the job to attach to
	 * @return The JobExecutionResult for the jobID
	 * @throws JobExecutionException if an error occurs during monitoring the job execution
	 */
	public JobListeningContext connectToJob(JobID jobID) throws JobExecutionException {
		final ActorSystem actorSystem;

		try {
			actorSystem = actorSystemLoader.get();
		} catch (FlinkException fe) {
			throw new JobExecutionException(
				jobID,
				"Could not start the ActorSystem needed to talk to the JobManager.",
				fe);
		}

		return JobClient.attachToRunningJob(
			jobID,
			flinkConfig,
			actorSystem,
			highAvailabilityServices,
			timeout,
			printStatusDuringExecution);
	}

	/**
	 * Requests the {@link JobStatus} of the job with the given {@link JobID}.
	 */
	public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
		final ActorGateway jobManager;
		try {
			jobManager = getJobManagerGateway();
		} catch (FlinkException e) {
			throw new RuntimeException("Could not retrieve JobManage gateway.", e);
		}

		Future<Object> response = jobManager.ask(JobManagerMessages.getRequestJobStatus(jobId), timeout);

		CompletableFuture<Object> javaFuture = FutureUtils.<Object>toJava(response);

		return javaFuture.thenApply((responseMessage) -> {
			if (responseMessage instanceof JobManagerMessages.CurrentJobStatus) {
				return ((JobManagerMessages.CurrentJobStatus) responseMessage).status();
			} else if (responseMessage instanceof JobManagerMessages.JobNotFound) {
				throw new CompletionException(
					new IllegalStateException("Could not find job with JobId " + jobId));
			} else {
				throw new CompletionException(
					new IllegalStateException("Unknown JobManager response of type " + responseMessage.getClass()));
			}
		});
	}

	/**
	 * Cancels a job identified by the job id.
	 * @param jobId the job id
	 * @throws Exception In case an error occurred.
	 */
	public void cancel(JobID jobId) throws Exception {
		final ActorGateway jobManager = getJobManagerGateway();

		Object cancelMsg = new JobManagerMessages.CancelJob(jobId);

		Future<Object> response = jobManager.ask(cancelMsg, timeout);
		final Object rc = Await.result(response, timeout);

		if (rc instanceof JobManagerMessages.CancellationSuccess) {
			// no further action required
		} else if (rc instanceof JobManagerMessages.CancellationFailure) {
			throw new Exception("Canceling the job with ID " + jobId + " failed.",
				((JobManagerMessages.CancellationFailure) rc).cause());
		} else {
			throw new IllegalStateException("Unexpected response: " + rc);
		}
	}

	/**
	 * Cancels a job identified by the job id and triggers a savepoint.
	 * @param jobId the job id
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return path where the savepoint is located
	 * @throws Exception In case an error cocurred.
	 */
	public String cancelWithSavepoint(JobID jobId, @Nullable String savepointDirectory) throws Exception {
		final ActorGateway jobManager = getJobManagerGateway();

		Object cancelMsg = new JobManagerMessages.CancelJobWithSavepoint(jobId, savepointDirectory);

		Future<Object> response = jobManager.ask(cancelMsg, timeout);
		final Object rc = Await.result(response, timeout);

		if (rc instanceof JobManagerMessages.CancellationSuccess) {
			JobManagerMessages.CancellationSuccess success = (JobManagerMessages.CancellationSuccess) rc;
			return success.savepointPath();
		} else if (rc instanceof JobManagerMessages.CancellationFailure) {
			throw new Exception("Cancel & savepoint for the job with ID " + jobId + " failed.",
				((JobManagerMessages.CancellationFailure) rc).cause());
		} else {
			throw new IllegalStateException("Unexpected response: " + rc);
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
		final ActorGateway jobManager = getJobManagerGateway();

		Future<Object> response = jobManager.ask(new JobManagerMessages.StopJob(jobId), timeout);

		final Object rc = Await.result(response, timeout);

		if (rc instanceof JobManagerMessages.StoppingSuccess) {
			// no further action required
		} else if (rc instanceof JobManagerMessages.StoppingFailure) {
			throw new Exception("Stopping the job with ID " + jobId + " failed.",
				((JobManagerMessages.StoppingFailure) rc).cause());
		} else {
			throw new IllegalStateException("Unexpected response: " + rc);
		}
	}

	/**
	 * Triggers a savepoint for the job identified by the job id. The savepoint will be written to the given savepoint
	 * directory, or {@link org.apache.flink.configuration.CheckpointingOptions#SAVEPOINT_DIRECTORY} if it is null.
	 *
	 * @param jobId job id
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return path future where the savepoint is located
	 * @throws FlinkException if no connection to the cluster could be established
	 */
	public CompletableFuture<String> triggerSavepoint(JobID jobId, @Nullable String savepointDirectory) throws FlinkException {
		final ActorGateway jobManager = getJobManagerGateway();

		Future<Object> response = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobId, Option.<String>apply(savepointDirectory)),
			new FiniteDuration(1, TimeUnit.HOURS));
		CompletableFuture<Object> responseFuture = FutureUtils.<Object>toJava(response);

		return responseFuture.thenApply((responseMessage) -> {
			if (responseMessage instanceof JobManagerMessages.TriggerSavepointSuccess) {
				JobManagerMessages.TriggerSavepointSuccess success = (JobManagerMessages.TriggerSavepointSuccess) responseMessage;
				return success.savepointPath();
			} else if (responseMessage instanceof JobManagerMessages.TriggerSavepointFailure) {
				JobManagerMessages.TriggerSavepointFailure failure = (JobManagerMessages.TriggerSavepointFailure) responseMessage;
				throw new CompletionException(failure.cause());
			} else {
				throw new CompletionException(
					new IllegalStateException("Unknown JobManager response of type " + responseMessage.getClass()));
			}
		});
	}

	public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) throws FlinkException {
		final ActorGateway jobManager = getJobManagerGateway();

		Object msg = new JobManagerMessages.DisposeSavepoint(savepointPath);
		CompletableFuture<Object> responseFuture = FutureUtils.<Object>toJava(
			jobManager.ask(
				msg,
				timeout));

		return responseFuture.thenApply(
			(Object response) -> {
				if (response instanceof JobManagerMessages.DisposeSavepointSuccess$) {
					return Acknowledge.get();
				} else if (response instanceof JobManagerMessages.DisposeSavepointFailure) {
					JobManagerMessages.DisposeSavepointFailure failureResponse = (JobManagerMessages.DisposeSavepointFailure) response;

					if (failureResponse.cause() instanceof ClassNotFoundException) {
						throw new CompletionException(
							new ClassNotFoundException("Savepoint disposal failed, because of a " +
								"missing class. This is most likely caused by a custom state " +
								"instance, which cannot be disposed without the user code class " +
								"loader. Please provide the program jar with which you have created " +
								"the savepoint via -j <JAR> for disposal.",
								failureResponse.cause().getCause()));
					} else {
						throw new CompletionException(failureResponse.cause());
					}
				} else {
					throw new CompletionException(new FlinkRuntimeException("Unknown response type " + response.getClass().getSimpleName() + '.'));
				}
			});
	}

	/**
	 * Lists the currently running and finished jobs on the cluster.
	 *
	 * @return future collection of running and finished jobs
	 * @throws Exception if no connection to the cluster could be established
	 */
	public CompletableFuture<Collection<JobStatusMessage>> listJobs() throws Exception {
		final ActorGateway jobManager = getJobManagerGateway();

		Future<Object> response = jobManager.ask(new RequestJobDetails(true, false), timeout);
		CompletableFuture<Object> responseFuture = FutureUtils.<Object>toJava(response);

		return responseFuture.thenApply((responseMessage) -> {
			if (responseMessage instanceof MultipleJobsDetails) {
				MultipleJobsDetails details = (MultipleJobsDetails) responseMessage;

				final Collection<JobDetails> jobDetails = details.getJobs();
				Collection<JobStatusMessage> flattenedDetails = new ArrayList<>(jobDetails.size());
				jobDetails.forEach(detail -> flattenedDetails.add(new JobStatusMessage(detail.getJobId(), detail.getJobName(), detail.getStatus(), detail.getStartTime())));
				return flattenedDetails;
			} else {
				throw new CompletionException(
					new IllegalStateException("Unknown JobManager response of type " + responseMessage.getClass()));
			}
		});
	}

	/**
	 * Requests and returns the accumulators for the given job identifier. Accumulators can be
	 * requested while a is running or after it has finished. The default class loader is used
	 * to deserialize the incoming accumulator results.
	 * @param jobID The job identifier of a job.
	 * @return A Map containing the accumulator's name and its value.
	 */
	public Map<String, OptionalFailure<Object>> getAccumulators(JobID jobID) throws Exception {
		return getAccumulators(jobID, ClassLoader.getSystemClassLoader());
	}

	/**
	 * Requests and returns the accumulators for the given job identifier. Accumulators can be
	 * requested while a is running or after it has finished.
	 * @param jobID The job identifier of a job.
	 * @param loader The class loader for deserializing the accumulator results.
	 * @return A Map containing the accumulator's name and its value.
	 */
	public Map<String, OptionalFailure<Object>> getAccumulators(JobID jobID, ClassLoader loader) throws Exception {
		ActorGateway jobManagerGateway = getJobManagerGateway();

		Future<Object> response;
		try {
			response = jobManagerGateway.ask(new RequestAccumulatorResults(jobID), timeout);
		} catch (Exception e) {
			throw new Exception("Failed to query the job manager gateway for accumulators.", e);
		}

		Object result = Await.result(response, timeout);

		if (result instanceof AccumulatorResultsFound) {
			Map<String, SerializedValue<OptionalFailure<Object>>> serializedAccumulators =
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
				log.info("Telling job manager to end the session {}.", jid);
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

	public static JobGraph getJobGraph(Configuration flinkConfig, PackagedProgram prog, FlinkPlan optPlan, SavepointRestoreSettings savepointSettings) throws ProgramInvocationException {
		return getJobGraph(flinkConfig, optPlan, prog.getAllLibraries(), prog.getClasspaths(), savepointSettings);
	}

	public static JobGraph getJobGraph(Configuration flinkConfig, FlinkPlan optPlan, List<URL> jarFiles, List<URL> classpaths, SavepointRestoreSettings savepointSettings) {
		JobGraph job;
		if (optPlan instanceof StreamingPlan) {
			job = ((StreamingPlan) optPlan).getJobGraph();
			job.setSavepointRestoreSettings(savepointSettings);
		} else {
			JobGraphGenerator gen = new JobGraphGenerator(flinkConfig);
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
	public ActorGateway getJobManagerGateway() throws FlinkException {
		log.debug("Looking up JobManager");

		try {
			return LeaderRetrievalUtils.retrieveLeaderGateway(
				highAvailabilityServices.getJobManagerLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID),
				actorSystemLoader.get(),
				lookupTimeout);
		} catch (LeaderRetrievalException lre) {
			throw new FlinkException("Could not connect to the leading JobManager. Please check that the " +
				"JobManager is running.", lre);
		}
	}

	/**
	 * Logs and prints to sysout if printing to stdout is enabled.
	 * @param message The message to log/print
	 */
	protected void logAndSysout(String message) {
		log.info(message);
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
	 * <p>This is delayed until right before job submission to report any other errors first
	 * (e.g. invalid job definitions/errors in the user jar)
	 */
	public abstract void waitForClusterToBeReady();

	/**
	 * Returns an URL (as a string) to the JobManager web interface.
	 */
	public abstract String getWebInterfaceURL();

	/**
	 * Returns the latest cluster status, with number of Taskmanagers and slots.
	 */
	public abstract GetClusterStatusResponse getClusterStatus();

	/**
	 * May return new messages from the cluster.
	 * Messages can be for example about failed containers or container launch requests.
	 */
	public abstract List<String> getNewMessages();

	/**
	 * Returns the cluster id identifying the cluster to which the client is connected.
	 *
	 * @return cluster id of the connected cluster
	 */
	public abstract T getClusterId();

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
	 * Return the Flink configuration object.
	 * @return The Flink configuration object
	 */
	public Configuration getFlinkConfiguration() {
		return flinkConfig.clone();
	}

	/**
	 * The client may define an upper limit on the number of slots to use.
	 * @return <tt>-1</tt> ({@link #MAX_SLOTS_UNKNOWN}) if unknown
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
	public abstract JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader)
		throws ProgramInvocationException;

	/**
	 * Rescales the specified job such that it will have the new parallelism.
	 *
	 * @param jobId specifying the job to modify
	 * @param newParallelism specifying the new parallelism of the rescaled job
	 * @return Future which is completed once the rescaling has been completed
	 */
	public CompletableFuture<Acknowledge> rescaleJob(JobID jobId, int newParallelism) {
		throw new UnsupportedOperationException("The " + getClass().getSimpleName() + " does not support rescaling.");
	}

	public void shutDownCluster() {
		throw new UnsupportedOperationException("The " + getClass().getSimpleName() + " does not support shutDownCluster.");
	}
}
