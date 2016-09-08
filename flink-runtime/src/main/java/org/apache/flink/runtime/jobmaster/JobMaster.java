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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointLoader;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStore;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.SimpleCheckpointStatsTracker;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link org.apache.flink.runtime.jobgraph.JobGraph}.
 * <p>
 * It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 * <ul>
 * <li>{@link #updateTaskExecutionState(TaskExecutionState)} updates the task execution state for
 * given task</li>
 * </ul>
 */
public class JobMaster extends RpcEndpoint<JobMasterGateway> {

	/** Gateway to connected resource manager, null iff not connected */
	private ResourceManagerGateway resourceManager = null;

	/** Logical representation of the job */
	private final JobGraph jobGraph;

	/** Configuration of the job */
	private final Configuration configuration;

	/** Service to contend for and retrieve the leadership of JM and RM */
	private final HighAvailabilityServices highAvailabilityServices;

	/** Blob cache manager used across jobs */
	private final BlobLibraryCacheManager libraryCacheManager;

	/** Factory to create restart strategy for this job */
	private final RestartStrategyFactory restartStrategyFactory;

	/** Store for save points */
	private final SavepointStore savepointStore;

	/** The timeout for this job */
	private final FiniteDuration timeout;

	/** The scheduler to use for scheduling new tasks as they are needed */
	private final Scheduler scheduler;

	/** The metrics group used across jobs */
	private final JobManagerMetricGroup jobManagerMetricGroup;

	/** The execution context which is used to execute futures */
	private final ExecutionContext executionContext;

	/** The execution graph of this job */
	private volatile ExecutionGraph executionGraph;

	/** The checkpoint recovery factory used by this job */
	private CheckpointRecoveryFactory checkpointRecoveryFactory;

	/** Store for all submitted job graphs */
	private SubmittedJobGraphStore submittedJobGraphs;

	public JobMaster(
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityService,
		BlobLibraryCacheManager libraryCacheManager,
		RestartStrategyFactory restartStrategyFactory,
		SavepointStore savepointStore,
		FiniteDuration timeout,
		Scheduler scheduler,
		JobManagerMetricGroup jobManagerMetricGroup)
	{
		super(rpcService);
		this.jobGraph = checkNotNull(jobGraph);
		this.configuration = checkNotNull(configuration);
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.libraryCacheManager = checkNotNull(libraryCacheManager);
		this.restartStrategyFactory = checkNotNull(restartStrategyFactory);
		this.savepointStore = checkNotNull(savepointStore);
		this.timeout = checkNotNull(timeout);
		this.scheduler = checkNotNull(scheduler);
		this.jobManagerMetricGroup = checkNotNull(jobManagerMetricGroup);
		this.executionContext = checkNotNull(rpcService.getExecutionContext());
		this.executionGraph = null;
		this.checkpointRecoveryFactory = null;
		this.submittedJobGraphs = null;
	}

	public ResourceManagerGateway getResourceManager() {
		return resourceManager;
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	@Override
	public void start() {
		super.start();

		try {
			checkpointRecoveryFactory = highAvailabilityServices.getCheckpointRecoveryFactory();
		} catch (Exception e) {
			log.error("Could not get the checkpoint recovery factory.", e);
			throw new RuntimeException("Could not get the checkpoint recovery factory.", e);
		}

		try {
			submittedJobGraphs = highAvailabilityServices.getSubmittedJobGraphStore();
		} catch (Exception e) {
			log.error("Could not start the JobManager because we cannot get the job graph store.", e);
			throw new RuntimeException("Could not get the job graph store.", e);
		}

	}

	@Override
	public void shutDown() {
		super.shutDown();

		suspendJob(new Exception("JobManager is shutting down."));
	}

	//----------------------------------------------------------------------------------------------
	// RPC methods
	//----------------------------------------------------------------------------------------------


	/**
	 * Submits a job to the job manager. The job is registered at the libraryCacheManager which
	 * creates the job's class loader. The job graph is appended to the corresponding execution
	 * graph and be prepared to run.
	 *
	 * @param isRecovery Flag indicating whether this is a recovery or initial submission
	 * @return Flag indicating whether this job has been accepted
	 */
	@RpcMethod
	public boolean submitJob(final boolean isRecovery) {
		log.info("Submitting job {} ({}) " + (isRecovery ? "(Recovery)" : ""), jobGraph.getJobID(), jobGraph.getName());

		try {
			// IMPORTANT: We need to make sure that the library registration is the first action,
			// because this makes sure that the uploaded jar files are removed in case of
			// unsuccessful
			try {
				libraryCacheManager.registerJob(jobGraph.getJobID(), jobGraph.getUserJarBlobKeys(),
					jobGraph.getClasspaths());
			} catch (Throwable t) {
				throw new JobSubmissionException(jobGraph.getJobID(),
					"Cannot set up the user code libraries: " + t.getMessage(), t);
			}

			final ClassLoader userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID());
			if (userCodeLoader == null) {
				throw new JobSubmissionException(jobGraph.getJobID(),
					"The user code class loader could not be initialized.");
			}

			if (jobGraph.getNumberOfVertices() == 0) {
				throw new JobSubmissionException(jobGraph.getJobID(), "The given job is empty");
			}

			final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
				jobGraph.getSerializedExecutionConfig()
					.deserializeValue(userCodeLoader)
					.getRestartStrategy();
			final RestartStrategy restartStrategy;
			if (restartStrategyConfiguration != null) {
				restartStrategy = RestartStrategyFactory.createRestartStrategy(restartStrategyConfiguration);
			} else {
				restartStrategy = restartStrategyFactory.createRestartStrategy();
			}
			log.info("Using restart strategy {} for {}.", restartStrategy, jobGraph.getJobID());

			MetricGroup jobMetrics = null;
			if (jobManagerMetricGroup != null) {
				jobMetrics = jobManagerMetricGroup.addJob(jobGraph);
			}
			if (jobMetrics == null) {
				jobMetrics = new UnregisteredMetricsGroup();
			}

			if (executionGraph != null) {
				executionGraph = new ExecutionGraph(
					executionContext,
					jobGraph.getJobID(),
					jobGraph.getName(),
					jobGraph.getJobConfiguration(),
					jobGraph.getSerializedExecutionConfig(),
					timeout,
					restartStrategy,
					jobGraph.getUserJarBlobKeys(),
					jobGraph.getClasspaths(),
					userCodeLoader,
					jobMetrics);
			} else {
				// TODO: update last active time in JobInfo
			}

			executionGraph.setScheduleMode(jobGraph.getScheduleMode());
			executionGraph.setQueuedSchedulingAllowed(jobGraph.getAllowQueuedScheduling());

			try {
				executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
			} catch (Exception e) {
				log.warn("Cannot create JSON plan for job {} ({})", jobGraph.getJobID(), jobGraph.getName(), e);
				executionGraph.setJsonPlan("{}");
			}

			// initialize the vertices that have a master initialization hook
			// file output formats create directories here, input formats create splits
			if (log.isDebugEnabled()) {
				log.debug("Running initialization on master for job {} ({}).", jobGraph.getJobID(), jobGraph.getName());
			}
			for (JobVertex vertex : jobGraph.getVertices()) {
				final String executableClass = vertex.getInvokableClassName();
				if (executableClass == null || executableClass.length() == 0) {
					throw new JobSubmissionException(jobGraph.getJobID(),
						"The vertex " + vertex.getID() + " (" + vertex.getName() + ") has no invokable class.");
				}
				if (vertex.getParallelism() == ExecutionConfig.PARALLELISM_AUTO_MAX) {
					vertex.setParallelism(scheduler.getTotalNumberOfSlots());
				}

				try {
					vertex.initializeOnMaster(userCodeLoader);
				} catch (Throwable t) {
					throw new JobExecutionException(jobGraph.getJobID(),
						"Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(), t);
				}
			}

			// topologically sort the job vertices and attach the graph to the existing one
			final List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
			if (log.isDebugEnabled()) {
				log.debug("Adding {} vertices from job graph {} ({}).", sortedTopology.size(),
					jobGraph.getJobID(), jobGraph.getName());
			}
			executionGraph.attachJobGraph(sortedTopology);

			if (log.isDebugEnabled()) {
				log.debug("Successfully created execution graph from job graph {} ({}).",
					jobGraph.getJobID(), jobGraph.getName());
			}

			final JobSnapshottingSettings snapshotSettings = jobGraph.getSnapshotSettings();
			if (snapshotSettings != null) {
				List<ExecutionJobVertex> triggerVertices = getExecutionJobVertexWithId(
					executionGraph, snapshotSettings.getVerticesToTrigger());

				List<ExecutionJobVertex> ackVertices = getExecutionJobVertexWithId(
					executionGraph, snapshotSettings.getVerticesToAcknowledge());

				List<ExecutionJobVertex> confirmVertices = getExecutionJobVertexWithId(
					executionGraph, snapshotSettings.getVerticesToConfirm());

				CompletedCheckpointStore completedCheckpoints = checkpointRecoveryFactory.createCheckpointStore(
					jobGraph.getJobID(), userCodeLoader);

				CheckpointIDCounter checkpointIdCounter = checkpointRecoveryFactory.createCheckpointIDCounter(
					jobGraph.getJobID());

				// Checkpoint stats tracker
				boolean isStatsDisabled = configuration.getBoolean(
					ConfigConstants.JOB_MANAGER_WEB_CHECKPOINTS_DISABLE,
					ConfigConstants.DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_DISABLE);

				final CheckpointStatsTracker checkpointStatsTracker;
				if (isStatsDisabled) {
					checkpointStatsTracker = new DisabledCheckpointStatsTracker();
				} else {
					int historySize = configuration.getInteger(
						ConfigConstants.JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE,
						ConfigConstants.DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE);
					checkpointStatsTracker = new SimpleCheckpointStatsTracker(historySize, ackVertices, jobMetrics);
				}

				executionGraph.enableSnapshotCheckpointing(
					snapshotSettings.getCheckpointInterval(),
					snapshotSettings.getCheckpointTimeout(),
					snapshotSettings.getMinPauseBetweenCheckpoints(),
					snapshotSettings.getMaxConcurrentCheckpoints(),
					triggerVertices,
					ackVertices,
					confirmVertices,
					checkpointIdCounter,
					completedCheckpoints,
					savepointStore,
					checkpointStatsTracker);
			}

			// TODO: register job status change listeners
			// TODO: register client listeners

			if (isRecovery) {
				// this is a recovery of a master failure (this master takes over)
				executionGraph.restoreLatestCheckpointedState();
			} else {
				if (snapshotSettings != null) {
					String savepointPath = snapshotSettings.getSavepointPath();
					if (savepointPath != null) {
						// got a savepoint
						log.info("Starting job from savepoint {}.", savepointPath);

						// load the savepoint as a checkpoint into the system
						final CompletedCheckpoint savepoint = SavepointLoader.loadAndValidateSavepoint(
							jobGraph.getJobID(), executionGraph.getAllVertices(), savepointStore, savepointPath);
						executionGraph.getCheckpointCoordinator().getCheckpointStore().addCheckpoint(savepoint);

						// Reset the checkpoint ID counter
						long nextCheckpointId = savepoint.getCheckpointID() + 1;
						log.info("Reset the checkpoint ID to " + nextCheckpointId);
						executionGraph.getCheckpointCoordinator().getCheckpointIdCounter().setCount(nextCheckpointId);

						executionGraph.restoreLatestCheckpointedState();
					}
				}

				// TODO: add this job to submitted job graph store

			}

			// TODO: notify client about job submit success

			return true;
		} catch (Throwable t) {
			log.error("Failed to submit job {} ({})", jobGraph.getJobID(), jobGraph.getName(), t);

			libraryCacheManager.unregisterJob(jobGraph.getJobID());

			if (executionGraph != null) {
				executionGraph.fail(t);
				executionGraph = null;
			}

			final Throwable rt;
			if (t instanceof JobExecutionException) {
				rt = (JobExecutionException) t;
			} else {
				rt = new JobExecutionException(jobGraph.getJobID(),
					"Failed to start job " + jobGraph.getJobID() + " (" + jobGraph.getName() + ")", t);
			}
			// TODO: notify client of this failure

			// Any error occurred during submission phase will make this job as rejected
			return false;
		}
	}

	/**
	 * Making this job begins to run.
	 */
	@RpcMethod
	public void startJob() {
		log.info("Starting job {} ({}).", jobGraph.getJobID(), jobGraph.getName());

		// start scheduling job in another thread
		getRpcService().getExecutionContext().execute(new Runnable() {
			@Override
			public void run() {
				if (executionGraph != null) {
					try {
						executionGraph.scheduleForExecution(scheduler);
					} catch (Throwable t) {
						executionGraph.fail(t);
					}
				}
			}
		});
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and runtime status will be cleared. Should re-submit
	 * the job before restarting it.
	 *
	 * @param cause The reason of why this job been suspended.
	 */
	@RpcMethod
	public void suspendJob(final Throwable cause) {
		if (executionGraph != null) {
			executionGraph.suspend(cause);
			executionGraph = null;
		}
	}

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Acknowledge the task execution state update
	 */
	@RpcMethod
	public Acknowledge updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		System.out.println("TaskExecutionState: " + taskExecutionState);
		return Acknowledge.get();
	}

	/**
	 * Triggers the registration of the job master at the resource manager.
	 *
	 * @param address Address of the resource manager
	 */
	@RpcMethod
	public void registerAtResourceManager(final String address) {
		//TODO:: register at the RM
	}

	//----------------------------------------------------------------------------------------------
	// Helper methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Converts JobVertexIDs to corresponding ExecutionJobVertexes
	 *
	 * @param executionGraph The execution graph that holds the relationship
	 * @param vertexIDs      The vertexIDs need to be converted
	 * @return The corresponding ExecutionJobVertexes
	 * @throws JobSubmissionException
	 */
	private static List<ExecutionJobVertex> getExecutionJobVertexWithId(
		final ExecutionGraph executionGraph, final List<JobVertexID> vertexIDs)
		throws JobSubmissionException
	{
		final List<ExecutionJobVertex> ret = new ArrayList<>(vertexIDs.size());
		for (JobVertexID vertexID : vertexIDs) {
			final ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(vertexID);
			if (executionJobVertex == null) {
				throw new JobSubmissionException(executionGraph.getJobID(),
					"The snapshot checkpointing settings refer to non-existent vertex " + vertexID);
			}
			ret.add(executionJobVertex);
		}
		return ret;
	}
}
