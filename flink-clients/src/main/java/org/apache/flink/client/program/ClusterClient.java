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
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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

	/** Configuration of the client. */
	protected final Configuration flinkConfig;

	/** Timeout for futures. */
	protected final FiniteDuration timeout;

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
	public ClusterClient(
			Configuration flinkConfig,
			HighAvailabilityServices highAvailabilityServices,
			boolean sharedHaServices) {
		this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
		this.compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), flinkConfig);

		this.timeout = AkkaUtils.getClientTimeout(flinkConfig);

		this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
		this.sharedHaServices = sharedHaServices;
	}

	// ------------------------------------------------------------------------
	//  Startup & Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Shuts down the client. This stops the internal actor system and actors.
	 */
	public void shutdown() throws Exception {
		synchronized (this) {
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
			highAvailabilityServices.getDispatcherLeaderRetriever(),
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
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
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
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
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
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(prog.getUserCodeClassLoader());
			if (prog.isUsingProgramEntryPoint()) {
				final JobWithJars jobWithJars = prog.getPlanWithJars();
				return run(jobWithJars, parallelism, prog.getSavepointSettings());
			}
			else if (prog.isUsingInteractiveMode()) {
				log.info("Starting program in interactive mode (detached: {})", isDetached());

				final List<URL> libraries = prog.getAllLibraries();

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
		finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
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
	 * Requests the {@link JobStatus} of the job with the given {@link JobID}.
	 */
	public abstract CompletableFuture<JobStatus> getJobStatus(JobID jobId);

	/**
	 * Cancels a job identified by the job id.
	 * @param jobId the job id
	 * @throws Exception In case an error occurred.
	 */
	public abstract void cancel(JobID jobId) throws Exception;

	/**
	 * Cancels a job identified by the job id and triggers a savepoint.
	 * @param jobId the job id
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return path where the savepoint is located
	 * @throws Exception In case an error occurred.
	 */
	public abstract String cancelWithSavepoint(JobID jobId, @Nullable String savepointDirectory) throws Exception;

	/**
	 * Stops a program on Flink cluster whose job-manager is configured in this client's configuration.
	 * Stopping works only for streaming programs. Be aware, that the program might continue to run for
	 * a while after sending the stop command, because after sources stopped to emit data all operators
	 * need to finish processing.
	 *
	 * @param jobId the job ID of the streaming program to stop
	 * @param advanceToEndOfEventTime flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return a {@link CompletableFuture} containing the path where the savepoint is located
	 * @throws Exception
	 *             If the job ID is invalid (ie, is unknown or refers to a batch job) or if sending the stop signal
	 *             failed. That might be due to an I/O problem, ie, the job-manager is unreachable.
	 */
	public abstract String stopWithSavepoint(final JobID jobId, final boolean advanceToEndOfEventTime, @Nullable final String savepointDirectory) throws Exception;

	/**
	 * Triggers a savepoint for the job identified by the job id. The savepoint will be written to the given savepoint
	 * directory, or {@link org.apache.flink.configuration.CheckpointingOptions#SAVEPOINT_DIRECTORY} if it is null.
	 *
	 * @param jobId job id
	 * @param savepointDirectory directory the savepoint should be written to
	 * @return path future where the savepoint is located
	 * @throws FlinkException if no connection to the cluster could be established
	 */
	public abstract CompletableFuture<String> triggerSavepoint(JobID jobId, @Nullable String savepointDirectory) throws FlinkException;

	public abstract CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) throws FlinkException;

	/**
	 * Lists the currently running and finished jobs on the cluster.
	 *
	 * @return future collection of running and finished jobs
	 * @throws Exception if no connection to the cluster could be established
	 */
	public abstract CompletableFuture<Collection<JobStatusMessage>> listJobs() throws Exception;

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
	public abstract Map<String, OptionalFailure<Object>> getAccumulators(JobID jobID, ClassLoader loader) throws Exception;

	// ------------------------------------------------------------------------
	//  Internal translation methods
	// ------------------------------------------------------------------------

	/**
	 * Creates the optimized plan for a given program, using this client's compiler.
	 *
	 * @param prog The program to be compiled.
	 * @return The compiled and optimized plan, as returned by the compiler.
	 * @throws CompilerException Thrown, if the compiler encounters an illegal situation.
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
	//  Abstract methods to be implemented by the cluster specific Client
	// ------------------------------------------------------------------------

	/**
	 * Returns an URL (as a string) to the JobManager web interface.
	 */
	public abstract String getWebInterfaceURL();

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
	 * Calls the subclasses' submitJob method. It may decide to simply call one of the run methods or it may perform
	 * some custom job submission logic.
	 * @param jobGraph The JobGraph to be submitted
	 * @return JobSubmissionResult
	 */
	public abstract JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader)
		throws ProgramInvocationException;

	public void shutDownCluster() {
		throw new UnsupportedOperationException("The " + getClass().getSimpleName() + " does not support shutDownCluster.");
	}
}
