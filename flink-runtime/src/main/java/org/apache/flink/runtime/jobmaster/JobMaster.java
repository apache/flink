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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmaster.message.ClassloadingProps;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.StartStoppable;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.util.SerializedThrowable;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link JobGraph}.
 * <p>
 * It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 * <ul>
 * <li>{@link #updateTaskExecutionState} updates the task execution state for
 * given task</li>
 * </ul>
 */
public class JobMaster extends RpcEndpoint<JobMasterGateway> {

	private static final AtomicReferenceFieldUpdater<JobMaster, UUID> LEADER_ID_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(JobMaster.class, UUID.class, "leaderSessionID");

	// ------------------------------------------------------------------------

	/** Logical representation of the job */
	private final JobGraph jobGraph;

	/** Configuration of the JobManager */
	private final Configuration configuration;

	/** Service to contend for and retrieve the leadership of JM and RM */
	private final HighAvailabilityServices highAvailabilityServices;

	/** Blob cache manager used across jobs */
	private final BlobLibraryCacheManager libraryCacheManager;

	/** The metrics for the JobManager itself */
	private final MetricGroup jobManagerMetricGroup;

	/** The metrics for the job */
	private final MetricGroup jobMetricGroup;

	/** The execution context which is used to execute futures */
	private final ExecutorService executionContext;

	private final OnCompletionActions jobCompletionActions;

	private final FatalErrorHandler errorHandler;

	private final ClassLoader userCodeLoader;

	/** The execution graph of this job */
	private final ExecutionGraph executionGraph;


	private volatile UUID leaderSessionID;

	// --------- resource manager --------

	/** Leader retriever service used to locate ResourceManager's address */
	private LeaderRetrievalService resourceManagerLeaderRetriever;

	/** Connection with ResourceManager, null if not located address yet or we close it initiative */
	private ResourceManagerConnection resourceManagerConnection;

	// TODO - we need to replace this with the slot pool
	private final Scheduler scheduler;

	// ------------------------------------------------------------------------

	public JobMaster(
			JobGraph jobGraph,
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityService,
			ExecutorService executorService,
			BlobLibraryCacheManager libraryCacheManager,
			RestartStrategyFactory restartStrategyFactory,
			Time rpcAskTimeout,
			@Nullable JobManagerMetricGroup jobManagerMetricGroup,
			OnCompletionActions jobCompletionActions,
			FatalErrorHandler errorHandler,
			ClassLoader userCodeLoader) throws Exception
	{
		super(rpcService);

		this.jobGraph = checkNotNull(jobGraph);
		this.configuration = checkNotNull(configuration);
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.libraryCacheManager = checkNotNull(libraryCacheManager);
		this.executionContext = checkNotNull(executorService);
		this.jobCompletionActions = checkNotNull(jobCompletionActions);
		this.errorHandler = checkNotNull(errorHandler);
		this.userCodeLoader = checkNotNull(userCodeLoader);

		final String jobName = jobGraph.getName();
		final JobID jid = jobGraph.getJobID();

		if (jobManagerMetricGroup != null) {
			this.jobManagerMetricGroup = jobManagerMetricGroup;
			this.jobMetricGroup = jobManagerMetricGroup.addJob(jobGraph);
		} else {
			this.jobManagerMetricGroup = new UnregisteredMetricsGroup();
			this.jobMetricGroup = new UnregisteredMetricsGroup();
		}

		log.info("Initializing job {} ({}).", jobName, jid);

		final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
				jobGraph.getSerializedExecutionConfig()
						.deserializeValue(userCodeLoader)
						.getRestartStrategy();

		final RestartStrategy restartStrategy = (restartStrategyConfiguration != null) ?
				RestartStrategyFactory.createRestartStrategy(restartStrategyConfiguration) :
				restartStrategyFactory.createRestartStrategy();

		log.info("Using restart strategy {} for {} ({}).", restartStrategy, jobName, jid);

		CheckpointRecoveryFactory checkpointRecoveryFactory;
		try {
			checkpointRecoveryFactory = highAvailabilityServices.getCheckpointRecoveryFactory();
		} catch (Exception e) {
			log.error("Could not create the access to highly-available checkpoint storage.", e);
			throw new Exception("Could not create the access to highly-available checkpoint storage.", e);
		}

		try {
			resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();
		} catch (Exception e) {
			log.error("Could not get the resource manager leader retriever.", e);
			throw new JobSubmissionException(jobGraph.getJobID(),
					"Could not get the resource manager leader retriever.", e);
		}

		this.executionGraph = ExecutionGraphBuilder.buildGraph(
				null,
				jobGraph,
				configuration,
				executorService,
				executorService,
				userCodeLoader,
				checkpointRecoveryFactory,
				rpcAskTimeout,
				restartStrategy,
				jobMetricGroup,
				-1,
				log);

		// TODO - temp fix
		this.scheduler = new Scheduler(executorService);
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------


	@Override
	public void start() {
		throw new UnsupportedOperationException("Should never call start() without leader ID");
	}

	/**
	 * Start the rpc service and begin to run the job.
	 *
	 * @param leaderSessionID The necessary leader id for running the job.
	 */
	public void start(final UUID leaderSessionID) {
		if (LEADER_ID_UPDATER.compareAndSet(this, null, leaderSessionID)) {
			super.start();

			log.info("Starting JobManager for job {} ({})", jobGraph.getName(), jobGraph.getJobID());
			getSelf().startJobExecution();
		} else {
			log.warn("Job already started with leaderId {}, ignoring this start request.", leaderSessionID);
		}
	}

	/**
	 * Suspend the job and shutdown all other services including rpc.
	 */
	@Override
	public void shutDown() {
		// make sure there is a graceful exit
		getSelf().suspendExecution(new Exception("JobManager is shutting down."));
		super.shutDown();
	}

	//----------------------------------------------------------------------------------------------
	// RPC methods
	//----------------------------------------------------------------------------------------------

	//-- job starting and stopping  -----------------------------------------------------------------

	@RpcMethod
	public void startJobExecution() {
		log.info("Starting execution of job {} ({}) with leaderId {}.",
				jobGraph.getName(), jobGraph.getJobID(), leaderSessionID);

		try {
			// register self as job status change listener
			executionGraph.registerJobStatusListener(new JobStatusListener() {
				@Override
				public void jobStatusChanges(
						final JobID jobId, final JobStatus newJobStatus, final long timestamp, final Throwable error)
				{
					// run in rpc thread to avoid concurrency
					runAsync(new Runnable() {
						@Override
						public void run() {
							jobStatusChanged(newJobStatus, timestamp, error);
						}
					});
				}
			});

			// job is ready to go, try to establish connection with resource manager
			resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
		} catch (Throwable t) {

			// TODO - this should not result in a job failure, but another leader should take over
			// TODO - either this master should retry the execution, or it should relinquish leadership / terminate

			log.error("Failed to start job {} ({})", jobGraph.getName(), jobGraph.getJobID(), t);

			executionGraph.fail(t);

			final JobExecutionException rt;
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
				try {
					executionGraph.scheduleForExecution(scheduler);
				} catch (Throwable t) {
					executionGraph.fail(t);
				}
			}
		});
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(UUID)} method once we take the leadership back again.
	 *
	 * @param cause The reason of why this job been suspended.
	 */
	@RpcMethod
	public void suspendExecution(final Throwable cause) {
		if (leaderSessionID == null) {
			log.debug("Job has already been suspended or shutdown.");
			return;
		}

		// receive no more messages until started again, should be called before we clear self leader id
		((StartStoppable) getSelf()).stop();

		leaderSessionID = null;
		executionGraph.suspend(cause);

		// disconnect from resource manager:
		try {
			resourceManagerLeaderRetriever.stop();
		} catch (Exception e) {
			log.warn("Failed to stop resource manager leader retriever when suspending.");
		}
		closeResourceManagerConnection();

		// TODO: disconnect from all registered task managers

	}

	//----------------------------------------------------------------------------------------------

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Acknowledge the task execution state update
	 */
	@RpcMethod
	public Acknowledge updateTaskExecutionState(
			final UUID leaderSessionID,
			final TaskExecutionState taskExecutionState) throws Exception
	{
		if (taskExecutionState == null) {
			throw new NullPointerException("TaskExecutionState must not be null.");
		}

		if (!this.leaderSessionID.equals(leaderSessionID)) {
			throw new Exception("Leader id not match, expected: " + this.leaderSessionID
					+ ", actual: " + leaderSessionID);
		}

		if (executionGraph.updateState(taskExecutionState)) {
			return Acknowledge.get();
		} else {
			throw new ExecutionGraphException("The execution attempt " +
					taskExecutionState.getID() + " was not found.");
		}
	}

	@RpcMethod
	public SerializedInputSplit requestNextInputSplit(
			final UUID leaderSessionID,
			final JobVertexID vertexID,
			final ExecutionAttemptID executionAttempt) throws Exception
	{
		if (!this.leaderSessionID.equals(leaderSessionID)) {
			throw new Exception("Leader id not match, expected: " + this.leaderSessionID
					+ ", actual: " + leaderSessionID);
		}

		final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
		if (execution == null) {
			// can happen when JobManager had already unregistered this execution upon on task failure,
			// but TaskManager get some delay to aware of that situation
			if (log.isDebugEnabled()) {
				log.debug("Can not find Execution for attempt {}.", executionAttempt);
			}
			// but we should TaskManager be aware of this
			throw new Exception("Can not find Execution for attempt " + executionAttempt);
		}

		final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
		if (vertex == null) {
			log.error("Cannot find execution vertex for vertex ID {}.", vertexID);
			throw new Exception("Cannot find execution vertex for vertex ID " + vertexID);
		}

		final InputSplitAssigner splitAssigner = vertex.getSplitAssigner();
		if (splitAssigner == null) {
			log.error("No InputSplitAssigner for vertex ID {}.", vertexID);
			throw new Exception("No InputSplitAssigner for vertex ID " + vertexID);
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
			return new SerializedInputSplit(serializedInputSplit);
		} catch (Exception ex) {
			log.error("Could not serialize the next input split of class {}.", nextInputSplit.getClass(), ex);
			IOException reason = new IOException("Could not serialize the next input split of class " +
					nextInputSplit.getClass() + ".", ex);
			vertex.fail(reason);
			throw reason;
		}
	}

	@RpcMethod
	public ExecutionState requestPartitionState(
			final UUID leaderSessionID,
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID resultPartitionId) throws Exception {

		if (!this.leaderSessionID.equals(leaderSessionID)) {
			throw new Exception("Leader id not match, expected: " + this.leaderSessionID
					+ ", actual: " + leaderSessionID);
		}

		final Execution execution = executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
		if (execution != null) {
			return execution.getState();
		}
		else {
			final IntermediateResult intermediateResult =
					executionGraph.getAllIntermediateResults().get(intermediateResultId);

			if (intermediateResult != null) {
				// Try to find the producing execution
				Execution producerExecution = intermediateResult
						.getPartitionById(resultPartitionId.getPartitionId())
						.getProducer()
						.getCurrentExecutionAttempt();

				if (producerExecution.getAttemptId() == resultPartitionId.getProducerId()) {
					return producerExecution.getState();
				} else {
					throw new PartitionProducerDisposedException(resultPartitionId);
				}
			} else {
				throw new IllegalArgumentException("Intermediate data set with ID "
						+ intermediateResultId + " not found.");
			}
		}
	}

	@RpcMethod
	public Acknowledge scheduleOrUpdateConsumers(
			final UUID leaderSessionID,
			final ResultPartitionID partitionID) throws Exception
	{
		if (!this.leaderSessionID.equals(leaderSessionID)) {
			throw new Exception("Leader id not match, expected: " + this.leaderSessionID
					+ ", actual: " + leaderSessionID);
		}

		executionGraph.scheduleOrUpdateConsumers(partitionID);
		return Acknowledge.get();
	}

	@RpcMethod
	public void disconnectTaskManager(final ResourceID resourceID) {
		throw new UnsupportedOperationException();
	}

	@RpcMethod
	public void acknowledgeCheckpoint(
			final JobID jobID,
			final ExecutionAttemptID executionAttemptID,
			final CheckpointMetaData checkpointInfo,
			final SubtaskState checkpointState) throws CheckpointException {

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		final AcknowledgeCheckpoint ackMessage = 
				new AcknowledgeCheckpoint(jobID, executionAttemptID, checkpointInfo, checkpointState);

		if (checkpointCoordinator != null) {
			getRpcService().execute(new Runnable() {
				@Override
				public void run() {
					try {
						checkpointCoordinator.receiveAcknowledgeMessage(ackMessage);
					} catch (Throwable t) {
						log.warn("Error while processing checkpoint acknowledgement message");
					}
				}
			});
		} else {
			log.error("Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator",
					jobGraph.getJobID());
		}
	}

	@RpcMethod
	public void declineCheckpoint(
			final JobID jobID,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointID,
			final Throwable reason)
	{
		final DeclineCheckpoint decline = new DeclineCheckpoint(
				jobID, executionAttemptID, checkpointID, reason);
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			getRpcService().execute(new Runnable() {
				@Override
				public void run() {
					try {
						checkpointCoordinator.receiveDeclineMessage(decline);
					} catch (Exception e) {
						log.error("Error in CheckpointCoordinator while processing {}", decline, e);
					}
				}
			});
		} else {
			log.error("Received DeclineCheckpoint message for job {} with no CheckpointCoordinator",
					jobGraph.getJobID());
		}
	}

	@RpcMethod
	public KvStateLocation lookupKvStateLocation(final String registrationName) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("Lookup key-value state for job {} with registration " +
					"name {}.", jobGraph.getJobID(), registrationName);
		}

		final KvStateLocationRegistry registry = executionGraph.getKvStateLocationRegistry();
		final KvStateLocation location = registry.getKvStateLocation(registrationName);
		if (location != null) {
			return location;
		} else {
			throw new UnknownKvStateLocation(registrationName);
		}
	}

	@RpcMethod
	public void notifyKvStateRegistered(
			final JobVertexID jobVertexId,
			final KeyGroupRange keyGroupRange,
			final String registrationName,
			final KvStateID kvStateId,
			final KvStateServerAddress kvStateServerAddress)
	{
		if (log.isDebugEnabled()) {
			log.debug("Key value state registered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
		}

		try {
			executionGraph.getKvStateLocationRegistry().notifyKvStateRegistered(
					jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress);
		} catch (Exception e) {
			log.error("Failed to notify KvStateRegistry about registration {}.", registrationName);
		}
	}

	@RpcMethod
	public void notifyKvStateUnregistered(
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName)
	{
		if (log.isDebugEnabled()) {
			log.debug("Key value state unregistered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
		}

		try {
			executionGraph.getKvStateLocationRegistry().notifyKvStateUnregistered(
					jobVertexId, keyGroupRange, registrationName);
		} catch (Exception e) {
			log.error("Failed to notify KvStateRegistry about registration {}.", registrationName);
		}
	}

	@RpcMethod
	public ClassloadingProps requestClassloadingProps() throws Exception {
		return new ClassloadingProps(libraryCacheManager.getBlobServerPort(),
				executionGraph.getRequiredJarFiles(),
				executionGraph.getRequiredClasspaths());
	}

	@RpcMethod
	public Iterable<AllocationID> offerSlots(final Iterable<AllocationID> slots, UUID leaderId) {
		throw new UnsupportedOperationException("Has to be implemented.");
	}

	@RpcMethod
	public void failSlot(final AllocationID allocationId, UUID leaderId, Exception cause) {
		throw new UnsupportedOperationException("Has to be implemented.");
	}

	@RpcMethod
	public RegistrationResponse registerTaskManager(
		final String taskManagerAddress,
		final ResourceID taskManagerProcessId,
		final UUID leaderId) {
		throw new UnsupportedOperationException("Has to be implemented.");
	}

	//----------------------------------------------------------------------------------------------
	// Internal methods
	//----------------------------------------------------------------------------------------------

	private void handleFatalError(final Throwable cause) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.error("Fatal error occurred on JobManager, cause: {}", cause.getMessage(), cause);
				shutDown();
				errorHandler.onFatalError(cause);
			}
		});
	}

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
			} else if (newJobStatus == JobStatus.CANCELED) {
				final Throwable unpackedError = SerializedThrowable.get(error, userCodeLoader);
				final JobExecutionException exception = new JobExecutionException(
						jobID, "Job was cancelled.", unpackedError);
				// TODO should we also notify client?
				jobCompletionActions.jobFailed(exception);
			} else if (newJobStatus == JobStatus.FAILED) {
				final Throwable unpackedError = SerializedThrowable.get(error, userCodeLoader);
				final JobExecutionException exception = new JobExecutionException(
						jobID, "Job execution failed.", unpackedError);
				// TODO should we also notify client?
				jobCompletionActions.jobFailed(exception);
			} else {
				final JobExecutionException exception = new JobExecutionException(
						jobID, newJobStatus + " is not a terminal state.");
				// TODO should we also notify client?
				jobCompletionActions.jobFailed(exception);
				throw new RuntimeException(exception);
			}
		}
	}

	private void notifyOfNewResourceManagerLeader(
			final String resourceManagerAddress, final UUID resourceManagerLeaderId)
	{
		// IMPORTANT: executed by main thread to avoid concurrence
		runAsync(new Runnable() {
			@Override
			public void run() {
				if (resourceManagerConnection != null) {
					if (resourceManagerAddress != null) {
						if (resourceManagerAddress.equals(resourceManagerConnection.getTargetAddress())
								&& resourceManagerLeaderId.equals(resourceManagerConnection.getTargetLeaderId())) {
							// both address and leader id are not changed, we can keep the old connection
							return;
						}
						log.info("ResourceManager leader changed from {} to {}. Registering at new leader.",
								resourceManagerConnection.getTargetAddress(), resourceManagerAddress);
					} else {
						log.info("Current ResourceManager {} lost leader status. Waiting for new ResourceManager leader.",
								resourceManagerConnection.getTargetAddress());
					}
				}

				closeResourceManagerConnection();

				if (resourceManagerAddress != null) {
					log.info("Attempting to register at ResourceManager {}", resourceManagerAddress);
					resourceManagerConnection = new ResourceManagerConnection(
							log, jobGraph.getJobID(), leaderSessionID,
							resourceManagerAddress, resourceManagerLeaderId, executionContext);
					resourceManagerConnection.start();
				}
			}
		});
	}

	private void onResourceManagerRegistrationSuccess(final JobMasterRegistrationSuccess success) {
		getRpcService().execute(new Runnable() {
			@Override
			public void run() {
				// TODO - add tests for comment in https://github.com/apache/flink/pull/2565
				// verify the response with current connection
				if (resourceManagerConnection != null
						&& resourceManagerConnection.getTargetLeaderId().equals(success.getResourceManagerLeaderId())) {
					log.info("JobManager successfully registered at ResourceManager, leader id: {}.",
							success.getResourceManagerLeaderId());
				}
			}
		});
	}

	private void closeResourceManagerConnection() {
		if (resourceManagerConnection != null) {
			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}
	}

	//----------------------------------------------------------------------------------------------
	// Utility classes
	//----------------------------------------------------------------------------------------------

	private class ResourceManagerLeaderListener implements LeaderRetrievalListener {
		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			notifyOfNewResourceManagerLeader(leaderAddress, leaderSessionID);
		}

		@Override
		public void handleError(final Exception exception) {
			handleFatalError(exception);
		}
	}

	private class ResourceManagerConnection
			extends RegisteredRpcConnection<ResourceManagerGateway, JobMasterRegistrationSuccess>
	{
		private final JobID jobID;

		private final UUID jobManagerLeaderID;

		ResourceManagerConnection(
				final Logger log,
				final JobID jobID,
				final UUID jobManagerLeaderID,
				final String resourceManagerAddress,
				final UUID resourceManagerLeaderID,
				final Executor executor)
		{
			super(log, resourceManagerAddress, resourceManagerLeaderID, executor);
			this.jobID = checkNotNull(jobID);
			this.jobManagerLeaderID = checkNotNull(jobManagerLeaderID);
		}

		@Override
		protected RetryingRegistration<ResourceManagerGateway, JobMasterRegistrationSuccess> generateRegistration() {
			return new RetryingRegistration<ResourceManagerGateway, JobMasterRegistrationSuccess>(
					log, getRpcService(), "ResourceManager", ResourceManagerGateway.class,
					getTargetAddress(), getTargetLeaderId())
			{
				@Override
				protected Future<RegistrationResponse> invokeRegistration(ResourceManagerGateway gateway, UUID leaderId,
						long timeoutMillis) throws Exception
				{
					Time timeout = Time.milliseconds(timeoutMillis);
					return gateway.registerJobMaster(leaderId, jobManagerLeaderID, getAddress(), jobID, timeout);
				}
			};
		}

		@Override
		protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
			onResourceManagerRegistrationSuccess(success);
		}

		@Override
		protected void onRegistrationFailure(final Throwable failure) {
			handleFatalError(failure);
		}
	}
}
