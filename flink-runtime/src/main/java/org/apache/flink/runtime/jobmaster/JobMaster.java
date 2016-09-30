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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStore;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.SimpleCheckpointStatsTracker;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmaster.message.NextInputSplit;
import org.apache.flink.runtime.jobmaster.message.PartitionState;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.util.SerializedThrowable;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;

import scala.concurrent.ExecutionContext$;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link JobGraph}.
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
	private final Time timeout;

	/** The scheduler to use for scheduling new tasks as they are needed */
	private final Scheduler scheduler;

	/** The metrics group used across jobs */
	private final JobManagerMetricGroup jobManagerMetricGroup;

	/** The execution context which is used to execute futures */
	private final Executor executionContext;

	private final OnCompletionActions jobCompletionActions;

	/** The execution graph of this job */
	private volatile ExecutionGraph executionGraph;

	/** The checkpoint recovery factory used by this job */
	private CheckpointRecoveryFactory checkpointRecoveryFactory;

	private ClassLoader userCodeLoader;

	private RestartStrategy restartStrategy;

	private MetricGroup jobMetrics;

	public JobMaster(
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityService,
		BlobLibraryCacheManager libraryCacheManager,
		RestartStrategyFactory restartStrategyFactory,
		SavepointStore savepointStore,
		Time timeout,
		Scheduler scheduler,
		JobManagerMetricGroup jobManagerMetricGroup,
		OnCompletionActions jobCompletionActions)
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
		this.executionContext = checkNotNull(rpcService.getExecutor());
		this.jobCompletionActions = checkNotNull(jobCompletionActions);
	}

	public ResourceManagerGateway getResourceManager() {
		return resourceManager;
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	/**
	 * Initializing the job execution environment, should be called before start. Any error occurred during
	 * initialization will be treated as job submission failure.
	 *
	 * @throws JobSubmissionException
	 */
	public void init() throws JobSubmissionException {
		log.info("Initializing job {} ({}).", jobGraph.getName(), jobGraph.getJobID());

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

			userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID());
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
			if (restartStrategyConfiguration != null) {
				restartStrategy = RestartStrategyFactory.createRestartStrategy(restartStrategyConfiguration);
			} else {
				restartStrategy = restartStrategyFactory.createRestartStrategy();
			}

			log.info("Using restart strategy {} for {} ({}).", restartStrategy, jobGraph.getName(), jobGraph.getJobID());

			if (jobManagerMetricGroup != null) {
				jobMetrics = jobManagerMetricGroup.addJob(jobGraph);
			}
			if (jobMetrics == null) {
				jobMetrics = new UnregisteredMetricsGroup();
			}

			try {
				checkpointRecoveryFactory = highAvailabilityServices.getCheckpointRecoveryFactory();
			} catch (Exception e) {
				log.error("Could not get the checkpoint recovery factory.", e);
				throw new JobSubmissionException(jobGraph.getJobID(), "Could not get the checkpoint recovery factory.", e);
			}

		} catch (Throwable t) {
			log.error("Failed to initializing job {} ({})", jobGraph.getName(), jobGraph.getJobID(), t);

			libraryCacheManager.unregisterJob(jobGraph.getJobID());

			if (t instanceof JobSubmissionException) {
				throw (JobSubmissionException) t;
			} else {
				throw new JobSubmissionException(jobGraph.getJobID(), "Failed to initialize job " +
					jobGraph.getName() + " (" + jobGraph.getJobID() + ")", t);
			}
		}
	}

	@Override
	public void start() {
		super.start();
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
	 * Start to run the job, runtime data structures like ExecutionGraph will be constructed now and checkpoint
	 * being recovered. After this, we will begin to schedule the job.
	 */
	@RpcMethod
	public void startJob() {
		log.info("Starting job {} ({}).", jobGraph.getName(), jobGraph.getJobID());

		if (executionGraph != null) {
			executionGraph = new ExecutionGraph(
				ExecutionContext$.MODULE$.fromExecutor(executionContext),
				jobGraph.getJobID(),
				jobGraph.getName(),
				jobGraph.getJobConfiguration(),
				jobGraph.getSerializedExecutionConfig(),
				new FiniteDuration(timeout.getSize(), timeout.getUnit()),
				restartStrategy,
				jobGraph.getUserJarBlobKeys(),
				jobGraph.getClasspaths(),
				userCodeLoader,
				jobMetrics);
		} else {
			// TODO: update last active time in JobInfo
		}

		try {
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
					throw new JobExecutionException(jobGraph.getJobID(),
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

			// TODO: register this class to execution graph as job status change listeners

			// TODO: register client as job / execution status change listeners if they are interested

			/*
			TODO: decide whether we should take the savepoint before recovery

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
			}
			*/

		} catch (Throwable t) {
			log.error("Failed to start job {} ({})", jobGraph.getName(), jobGraph.getJobID(), t);

			executionGraph.fail(t);
			executionGraph = null;

			final Throwable rt;
			if (t instanceof JobExecutionException) {
				rt = (JobExecutionException) t;
			} else {
				rt = new JobExecutionException(jobGraph.getJobID(),
					"Failed to start job " + jobGraph.getJobID() + " (" + jobGraph.getName() + ")", t);
			}

			// TODO: notify client about this failure

			jobCompletionActions.jobFailed(rt);
			return;
		}

		// start scheduling job in another thread
		executionContext.execute(new Runnable() {
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
	 * Suspending job, all the running tasks will be cancelled, and runtime data will be cleared.
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
	public boolean updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
		if (taskExecutionState == null) {
			return false;
		} else {
			return executionGraph.updateState(taskExecutionState);
		}
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

	@RpcMethod
	public NextInputSplit requestNextInputSplit(final JobVertexID vertexID, final ExecutionAttemptID executionAttempt) {
		final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
		if (execution == null) {
			// can happen when JobManager had already unregistered this execution upon on task failure,
			// but TaskManager get some delay to aware of that situation
			if (log.isDebugEnabled()) {
				log.debug("Can not find Execution for attempt {}.", executionAttempt);
			}
			return null;
		}

		final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
		if (vertex == null) {
			log.error("Cannot find execution vertex for vertex ID {}.", vertexID);
			return null;
		}

		final InputSplitAssigner splitAssigner = vertex.getSplitAssigner();
		if (splitAssigner == null) {
			log.error("No InputSplitAssigner for vertex ID {}.", vertexID);
			return null;
		}

		final Slot slot = execution.getAssignedResource();
		final int taskId = execution.getVertex().getParallelSubtaskIndex();
		final String host = slot != null ? slot.getTaskManagerLocation().getHostname() : null;
		final InputSplit nextInputSplit = splitAssigner.getNextInputSplit(host, taskId);

		if (log.isDebugEnabled()) {
			log.debug("Send next input split {}.", nextInputSplit);
		}
		try {
			final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
			return new NextInputSplit(serializedInputSplit);
		} catch (Exception ex) {
			log.error("Could not serialize the next input split of class {}.", nextInputSplit.getClass(), ex);
			vertex.fail(new RuntimeException("Could not serialize the next input split of class " +
				nextInputSplit.getClass() + ".", ex));
			return null;
		}
	}

	@RpcMethod
	public PartitionState requestPartitionState(
		final ResultPartitionID partitionId,
		final ExecutionAttemptID taskExecutionId,
		final IntermediateDataSetID taskResultId)
	{
		final Execution execution = executionGraph.getRegisteredExecutions().get(partitionId.getProducerId());
		final ExecutionState state = execution != null ? execution.getState() : null;
		return new PartitionState(taskExecutionId, taskResultId, partitionId.getPartitionId(), state);
	}

	@RpcMethod
	public void scheduleOrUpdateConsumers(final ResultPartitionID partitionID) {
		executionGraph.scheduleOrUpdateConsumers(partitionID);
	}

	//----------------------------------------------------------------------------------------------
	// Internal methods
	//----------------------------------------------------------------------------------------------

	// TODO - wrap this as StatusListenerMessenger's callback with rpc main thread
	private void jobStatusChanged(final JobStatus newJobStatus, long timestamp, final Throwable error) {
		final JobID jobID = executionGraph.getJobID();
		final String jobName = executionGraph.getJobName();
		log.info("Status of job {} ({}) changed to {}.", jobID, jobName, newJobStatus, error);

		if (newJobStatus.isGloballyTerminalState()) {
			// TODO set job end time in JobInfo

			/*
			  TODO
			  if (jobInfo.sessionAlive) {
                jobInfo.setLastActive()
                val lastActivity = jobInfo.lastActive
                context.system.scheduler.scheduleOnce(jobInfo.sessionTimeout seconds) {
                  // remove only if no activity occurred in the meantime
                  if (lastActivity == jobInfo.lastActive) {
                    self ! decorateMessage(RemoveJob(jobID, removeJobFromStateBackend = true))
                  }
                }(context.dispatcher)
              } else {
                self ! decorateMessage(RemoveJob(jobID, removeJobFromStateBackend = true))
              }
			 */

			if (newJobStatus == JobStatus.FINISHED) {
				try {
					final Map<String, SerializedValue<Object>> accumulatorResults =
						executionGraph.getAccumulatorsSerialized();
					final SerializedJobExecutionResult result = new SerializedJobExecutionResult(
						jobID, 0, accumulatorResults // TODO get correct job duration
					);
					jobCompletionActions.jobFinished(result.toJobExecutionResult(userCodeLoader));
				} catch (Exception e) {
					log.error("Cannot fetch final accumulators for job {} ({})", jobName, jobID, e);
					final JobExecutionException exception = new JobExecutionException(
						jobID, "Failed to retrieve accumulator results.", e);
					// TODO should we also notify client?
					jobCompletionActions.jobFailed(exception);
				}
			}
			else if (newJobStatus == JobStatus.CANCELED) {
				final Throwable unpackedError = SerializedThrowable.get(error, userCodeLoader);
				final JobExecutionException exception = new JobExecutionException(
					jobID, "Job was cancelled.", unpackedError);
				// TODO should we also notify client?
				jobCompletionActions.jobFailed(exception);
			}
			else if (newJobStatus == JobStatus.FAILED) {
				final Throwable unpackedError = SerializedThrowable.get(error, userCodeLoader);
				final JobExecutionException exception = new JobExecutionException(
					jobID, "Job execution failed.", unpackedError);
				// TODO should we also notify client?
				jobCompletionActions.jobFailed(exception);
			}
			else {
				final JobExecutionException exception = new JobExecutionException(
					jobID, newJobStatus + " is not a terminal state.");
				// TODO should we also notify client?
				jobCompletionActions.jobFailed(exception);
				throw new RuntimeException(exception);
			}
		}
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
	 * @throws JobExecutionException
	 */
	private static List<ExecutionJobVertex> getExecutionJobVertexWithId(
		final ExecutionGraph executionGraph, final List<JobVertexID> vertexIDs)
		throws JobExecutionException
	{
		final List<ExecutionJobVertex> ret = new ArrayList<>(vertexIDs.size());
		for (JobVertexID vertexID : vertexIDs) {
			final ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(vertexID);
			if (executionJobVertex == null) {
				throw new JobExecutionException(executionGraph.getJobID(),
					"The snapshot checkpointing settings refer to non-existent vertex " + vertexID);
			}
			ret.add(executionJobVertex);
		}
		return ret;
	}
}
