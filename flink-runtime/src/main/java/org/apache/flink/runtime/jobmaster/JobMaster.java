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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistContext;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatReceiver;
import org.apache.flink.runtime.heartbeat.HeartbeatSender;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.BlocklistDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.JobShuffleContextImpl;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation.ResolutionMode;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.TaskStateSnapshot.deserializeTaskStateSnapshot;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single {@link
 * ExecutionPlan}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 *
 * <ul>
 *   <li>{@link #updateTaskExecutionState} updates the task execution state for given task
 * </ul>
 */
public class JobMaster extends FencedRpcEndpoint<JobMasterId>
        implements JobMasterGateway, JobMasterService {

    /** Default names for Flink's distributed components. */
    public static final String JOB_MANAGER_NAME = "jobmanager";

    // ------------------------------------------------------------------------

    private final JobMasterConfiguration jobMasterConfiguration;

    private final ResourceID resourceId;

    private final ExecutionPlan executionPlan;

    private final Duration rpcTimeout;

    private final HighAvailabilityServices highAvailabilityServices;

    private final BlobWriter blobWriter;

    private final HeartbeatServices heartbeatServices;

    private final ScheduledExecutorService futureExecutor;

    private final Executor ioExecutor;

    private final OnCompletionActions jobCompletionActions;

    private final FatalErrorHandler fatalErrorHandler;

    private final ClassLoader userCodeLoader;

    private final SlotPoolService slotPoolService;

    private final long initializationTimestamp;

    private final boolean retrieveTaskManagerHostName;

    // --------- ResourceManager --------

    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // --------- TaskManagers --------

    private final Map<ResourceID, TaskManagerRegistration> registeredTaskManagers;

    private final ShuffleMaster<?> shuffleMaster;

    // --------- Scheduler --------

    private final SchedulerNG schedulerNG;

    private final JobManagerJobStatusListener jobStatusListener;

    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    // -------- Misc ---------

    private final Map<String, Object> accumulators;

    private final JobMasterPartitionTracker partitionTracker;

    private final ExecutionDeploymentTracker executionDeploymentTracker;
    private final ExecutionDeploymentReconciler executionDeploymentReconciler;
    private final Collection<FailureEnricher> failureEnrichers;

    // -------- Mutable fields ---------

    @Nullable private ResourceManagerAddress resourceManagerAddress;

    @Nullable private ResourceManagerConnection resourceManagerConnection;

    @Nullable private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            taskManagerHeartbeatManager;

    private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

    private final BlocklistHandler blocklistHandler;

    private final Map<ResultPartitionID, PartitionWithMetrics> fetchedPartitionsWithMetrics =
            new HashMap<>();

    /**
     * A flag that indicates whether to fetch and retain partitions on task managers. This will
     * apply to future TaskManager registrations as well as already registered TaskManagers. The
     * flag will be set to true when starting batch job recovery and set to false after all required
     * partitions, as defined in {@code requireToFetchPartitions}, are either fetched or when a
     * timeout occurs.
     */
    private boolean fetchAndRetainPartitions = false;

    private Set<ResultPartitionID> partitionsToFetch;

    private CompletableFuture<Collection<PartitionWithMetrics>> fetchPartitionsFuture;

    // ------------------------------------------------------------------------

    public JobMaster(
            RpcService rpcService,
            JobMasterId jobMasterId,
            JobMasterConfiguration jobMasterConfiguration,
            ResourceID resourceId,
            ExecutionPlan executionPlan,
            HighAvailabilityServices highAvailabilityService,
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            JobManagerSharedServices jobManagerSharedServices,
            HeartbeatServices heartbeatServices,
            JobManagerJobMetricGroupFactory jobMetricGroupFactory,
            OnCompletionActions jobCompletionActions,
            FatalErrorHandler fatalErrorHandler,
            ClassLoader userCodeLoader,
            ShuffleMaster<?> shuffleMaster,
            PartitionTrackerFactory partitionTrackerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ExecutionDeploymentReconciler.Factory executionDeploymentReconcilerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            Collection<FailureEnricher> failureEnrichers,
            long initializationTimestamp)
            throws Exception {

        super(
                rpcService,
                RpcServiceUtils.createRandomName(JOB_MANAGER_NAME),
                jobMasterId,
                MdcUtils.asContextData(executionPlan.getJobID()));

        final ExecutionDeploymentReconciliationHandler executionStateReconciliationHandler =
                new ExecutionDeploymentReconciliationHandler() {

                    @Override
                    public void onMissingDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Failing deployments {} due to no longer being deployed.",
                                executionAttemptIds);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            schedulerNG.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            executionAttemptId,
                                            ExecutionState.FAILED,
                                            new FlinkException(
                                                    String.format(
                                                            "Execution %s is unexpectedly no longer running on task executor %s.",
                                                            executionAttemptId, host))));
                        }
                    }

                    @Override
                    public void onUnknownDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Canceling left-over deployments {} on task executor {}.",
                                executionAttemptIds,
                                host);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            TaskManagerRegistration taskManagerRegistration =
                                    registeredTaskManagers.get(host);
                            if (taskManagerRegistration != null) {
                                taskManagerRegistration
                                        .getTaskExecutorGateway()
                                        .cancelTask(executionAttemptId, rpcTimeout);
                            }
                        }
                    }
                };
        final String jobName = executionPlan.getName();
        final JobID jid = executionPlan.getJobID();

        this.executionPlan = checkNotNull(executionPlan);

        log.info("Initializing job '{}' ({}).", jobName, jid);

        this.executionDeploymentTracker = executionDeploymentTracker;
        this.executionDeploymentReconciler =
                executionDeploymentReconcilerFactory.create(executionStateReconciliationHandler);

        this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
        this.resourceId = checkNotNull(resourceId);
        this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
        this.highAvailabilityServices = checkNotNull(highAvailabilityService);
        this.blobWriter = jobManagerSharedServices.getBlobWriter();
        this.futureExecutor =
                MdcUtils.scopeToJob(jid, jobManagerSharedServices.getFutureExecutor());
        this.ioExecutor = MdcUtils.scopeToJob(jid, jobManagerSharedServices.getIoExecutor());
        this.jobCompletionActions = checkNotNull(jobCompletionActions);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.initializationTimestamp = initializationTimestamp;
        this.retrieveTaskManagerHostName =
                jobMasterConfiguration
                        .getConfiguration()
                        .get(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME);

        resourceManagerLeaderRetriever =
                highAvailabilityServices.getResourceManagerLeaderRetriever();

        this.registeredTaskManagers = new HashMap<>();
        this.blocklistHandler =
                blocklistHandlerFactory.create(
                        new JobMasterBlocklistContext(),
                        this::getNodeIdOfTaskManager,
                        getMainThreadExecutor(),
                        log);

        this.slotPoolService =
                checkNotNull(slotPoolServiceSchedulerFactory)
                        .createSlotPoolService(
                                jid,
                                createDeclarativeSlotPoolFactory(
                                        jobMasterConfiguration.getConfiguration()),
                                getMainThreadExecutor());

        this.partitionTracker =
                checkNotNull(partitionTrackerFactory)
                        .create(
                                resourceID -> {
                                    return Optional.ofNullable(
                                                    registeredTaskManagers.get(resourceID))
                                            .map(TaskManagerRegistration::getTaskExecutorGateway);
                                });

        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(executionPlan);
        this.jobStatusListener = new JobManagerJobStatusListener();

        this.failureEnrichers = checkNotNull(failureEnrichers);

        this.schedulerNG =
                createScheduler(
                        slotPoolServiceSchedulerFactory,
                        executionDeploymentTracker,
                        jobManagerJobMetricGroup,
                        jobStatusListener);

        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

        this.resourceManagerConnection = null;
        this.establishedResourceManagerConnection = null;

        this.accumulators = new HashMap<>();
    }

    private SchedulerNG createScheduler(
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            JobStatusListener jobStatusListener)
            throws Exception {
        final SchedulerNG scheduler =
                slotPoolServiceSchedulerFactory.createScheduler(
                        log,
                        executionPlan,
                        ioExecutor,
                        jobMasterConfiguration.getConfiguration(),
                        slotPoolService,
                        futureExecutor,
                        userCodeLoader,
                        highAvailabilityServices.getCheckpointRecoveryFactory(),
                        rpcTimeout,
                        blobWriter,
                        jobManagerJobMetricGroup,
                        jobMasterConfiguration.getSlotRequestTimeout(),
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentTracker,
                        initializationTimestamp,
                        getMainThreadExecutor(),
                        fatalErrorHandler,
                        jobStatusListener,
                        failureEnrichers,
                        blocklistHandler::addNewBlockedNodes);

        return scheduler;
    }

    private HeartbeatManager<Void, Void> createResourceManagerHeartbeatManager(
            HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            createTaskManagerHeartbeatManager(HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManagerSender(
                resourceId, new TaskManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private DeclarativeSlotPoolFactory createDeclarativeSlotPoolFactory(
            Configuration configuration) {
        if (BlocklistUtils.isBlocklistEnabled(configuration)) {
            return new BlocklistDeclarativeSlotPoolFactory(blocklistHandler::isBlockedTaskManager);
        } else {
            return new DefaultDeclarativeSlotPoolFactory();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------------------------------------

    @Override
    protected void onStart() throws JobMasterException {
        try {
            startJobExecution();
        } catch (Exception e) {
            final JobMasterException jobMasterException =
                    new JobMasterException("Could not start the JobMaster.", e);
            handleJobMasterError(jobMasterException);
            throw jobMasterException;
        }
    }

    /** Suspend the job and shutdown all other services including rpc. */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info(
                "Stopping the JobMaster for job '{}' ({}).",
                executionPlan.getName(),
                executionPlan.getJobID());

        // make sure there is a graceful exit
        return stopJobExecution(
                        new FlinkException(
                                String.format(
                                        "Stopping JobMaster for job '%s' (%s).",
                                        executionPlan.getName(), executionPlan.getJobID())))
                .exceptionally(
                        exception -> {
                            throw new CompletionException(
                                    new JobMasterException(
                                            "Could not properly stop the JobMaster.", exception));
                        });
    }

    // ----------------------------------------------------------------------------------------------
    // RPC methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> cancel(Duration timeout) {
        schedulerNG.cancel();

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * Updates the task execution state for a given task.
     *
     * @param taskExecutionState New task execution state for a given task
     * @return Acknowledge the task execution state update
     */
    @Override
    public CompletableFuture<Acknowledge> updateTaskExecutionState(
            final TaskExecutionState taskExecutionState) {
        FlinkException taskExecutionException;
        try {
            checkNotNull(taskExecutionState, "taskExecutionState");

            if (schedulerNG.updateTaskExecutionState(taskExecutionState)) {
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                taskExecutionException =
                        new ExecutionGraphException(
                                "The execution attempt "
                                        + taskExecutionState.getID()
                                        + " was not found.");
            }
        } catch (Exception e) {
            taskExecutionException =
                    new JobMasterException(
                            "Could not update the state of task execution for JobMaster.", e);
            handleJobMasterError(taskExecutionException);
        }
        return FutureUtils.completedExceptionally(taskExecutionException);
    }

    @Override
    public void notifyEndOfData(final ExecutionAttemptID executionAttempt) {
        schedulerNG.notifyEndOfData(executionAttempt);
    }

    @Override
    public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
            final JobVertexID vertexID, final ExecutionAttemptID executionAttempt) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestNextInputSplit(vertexID, executionAttempt));
        } catch (IOException e) {
            log.warn("Error while requesting next input split", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<ExecutionState> requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestPartitionState(intermediateResultId, resultPartitionId));
        } catch (PartitionProducerDisposedException e) {
            log.info("Error while requesting partition state", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> disconnectTaskManager(
            final ResourceID resourceID, final Exception cause) {

        taskManagerHeartbeatManager.unmonitorTarget(resourceID);
        slotPoolService.releaseTaskManager(resourceID, cause);
        partitionTracker.stopTrackingPartitionsFor(resourceID);

        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.remove(resourceID);

        if (taskManagerRegistration != null) {
            log.info(
                    "Disconnect TaskExecutor {} because: {}",
                    resourceID.getStringWithMetadata(),
                    cause.getMessage(),
                    ExceptionUtils.returnExceptionIfUnexpected(cause.getCause()));
            ExceptionUtils.logExceptionIfExcepted(cause.getCause(), log);

            taskManagerRegistration
                    .getTaskExecutorGateway()
                    .disconnectJobManager(executionPlan.getJobID(), cause);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // TODO: This method needs a leader session ID
    @Override
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            @Nullable final SerializedValue<TaskStateSnapshot> checkpointState) {
        schedulerNG.acknowledgeCheckpoint(
                jobID,
                executionAttemptID,
                checkpointId,
                checkpointMetrics,
                deserializeTaskStateSnapshot(checkpointState, getClass().getClassLoader()));
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {

        schedulerNG.reportCheckpointMetrics(
                jobID, executionAttemptID, checkpointId, checkpointMetrics);
    }

    @Override
    public void reportInitializationMetrics(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics) {
        schedulerNG.reportInitializationMetrics(jobId, executionAttemptId, initializationMetrics);
    }

    // TODO: This method needs a leader session ID
    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        schedulerNG.declineCheckpoint(decline);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
            final ExecutionAttemptID task,
            final OperatorID operatorID,
            final SerializedValue<OperatorEvent> serializedEvent) {

        try {
            final OperatorEvent evt = serializedEvent.deserializeValue(userCodeLoader);
            schedulerNG.deliverOperatorEventToCoordinator(task, operatorID, evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operatorID, SerializedValue<CoordinationRequest> serializedRequest) {
        try {
            final CoordinationRequest request = serializedRequest.deserializeValue(userCodeLoader);
            return schedulerNG.deliverCoordinationRequestToCoordinator(operatorID, request);
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<KvStateLocation> requestKvStateLocation(
            final JobID jobId, final String registrationName) {
        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestKvStateLocation(jobId, registrationName));
        } catch (UnknownKvStateLocation | FlinkJobNotFoundException e) {
            log.info("Error while request key-value state location", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress) {

        try {
            schedulerNG.notifyKvStateRegistered(
                    jobId,
                    jobVertexId,
                    keyGroupRange,
                    registrationName,
                    kvStateId,
                    kvStateServerAddress);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName) {
        try {
            schedulerNG.notifyKvStateUnregistered(
                    jobId, jobVertexId, keyGroupRange, registrationName);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state de-registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Collection<SlotOffer>> offerSlots(
            final ResourceID taskManagerId,
            final Collection<SlotOffer> slots,
            final Duration timeout) {

        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration == null) {
            return FutureUtils.completedExceptionally(
                    new Exception("Unknown TaskManager " + taskManagerId));
        }

        final RpcTaskManagerGateway rpcTaskManagerGateway =
                new RpcTaskManagerGateway(
                        taskManagerRegistration.getTaskExecutorGateway(), getFencingToken());

        return CompletableFuture.completedFuture(
                slotPoolService.offerSlots(
                        taskManagerRegistration.getTaskManagerLocation(),
                        rpcTaskManagerGateway,
                        slots));
    }

    @Override
    public void failSlot(
            final ResourceID taskManagerId,
            final AllocationID allocationId,
            final Exception cause) {

        if (registeredTaskManagers.containsKey(taskManagerId)) {
            internalFailAllocation(taskManagerId, allocationId, cause);
        } else {
            log.warn(
                    "Cannot fail slot "
                            + allocationId
                            + " because the TaskManager "
                            + taskManagerId
                            + " is unknown.");
        }
    }

    private void internalFailAllocation(
            @Nullable ResourceID resourceId, AllocationID allocationId, Exception cause) {
        final Optional<ResourceID> resourceIdOptional =
                slotPoolService.failAllocation(resourceId, allocationId, cause);
        resourceIdOptional.ifPresent(
                taskManagerId -> {
                    if (!partitionTracker.isTrackingPartitionsFor(taskManagerId)) {
                        releaseEmptyTaskManager(taskManagerId);
                    }
                });
    }

    private void releaseEmptyTaskManager(ResourceID resourceId) {
        disconnectTaskManager(
                resourceId,
                new FlinkException(
                        String.format(
                                "No more slots registered at JobMaster %s.",
                                resourceId.getStringWithMetadata())));
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerTaskManager(
            final JobID jobId,
            final TaskManagerRegistrationInformation taskManagerRegistrationInformation,
            final Duration timeout) {

        if (!executionPlan.getJobID().equals(jobId)) {
            log.debug(
                    "Rejecting TaskManager registration attempt because of wrong job id {}.",
                    jobId);
            return CompletableFuture.completedFuture(
                    new JMTMRegistrationRejection(
                            String.format(
                                    "The JobManager is not responsible for job %s. Maybe the TaskManager used outdated connection information.",
                                    jobId)));
        }

        final TaskManagerLocation taskManagerLocation;
        try {
            taskManagerLocation =
                    resolveTaskManagerLocation(
                            taskManagerRegistrationInformation.getUnresolvedTaskManagerLocation());
        } catch (FlinkException exception) {
            log.error("Could not accept TaskManager registration.", exception);
            return CompletableFuture.completedFuture(new RegistrationResponse.Failure(exception));
        }

        final ResourceID taskManagerId = taskManagerLocation.getResourceID();
        final UUID sessionId = taskManagerRegistrationInformation.getTaskManagerSession();
        final TaskManagerRegistration taskManagerRegistration =
                registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration != null) {
            if (taskManagerRegistration.getSessionId().equals(sessionId)) {
                log.debug(
                        "Ignoring registration attempt of TaskManager {} with the same session id {}.",
                        taskManagerId,
                        sessionId);
                final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
                return CompletableFuture.completedFuture(response);
            } else {
                disconnectTaskManager(
                        taskManagerId,
                        new FlinkException(
                                String.format(
                                        "A registered TaskManager %s re-registered with a new session id. This indicates a restart of the TaskManager. Closing the old connection.",
                                        taskManagerId)));
            }
        }

        CompletableFuture<RegistrationResponse> registrationResponseFuture =
                getRpcService()
                        .connect(
                                taskManagerRegistrationInformation.getTaskManagerRpcAddress(),
                                TaskExecutorGateway.class)
                        .handleAsync(
                                (TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
                                    if (throwable != null) {
                                        return new RegistrationResponse.Failure(throwable);
                                    }

                                    slotPoolService.registerTaskManager(taskManagerId);
                                    registeredTaskManagers.put(
                                            taskManagerId,
                                            TaskManagerRegistration.create(
                                                    taskManagerLocation,
                                                    taskExecutorGateway,
                                                    sessionId));

                                    // monitor the task manager as heartbeat target
                                    taskManagerHeartbeatManager.monitorTarget(
                                            taskManagerId,
                                            new TaskExecutorHeartbeatSender(taskExecutorGateway));

                                    return new JMTMRegistrationSuccess(resourceId);
                                },
                                getMainThreadExecutor());

        if (fetchAndRetainPartitions) {
            registrationResponseFuture.whenComplete(
                    (ignored, throwable) ->
                            fetchAndRetainPartitionWithMetricsOnTaskManager(taskManagerId));
        }

        return registrationResponseFuture;
    }

    @Nonnull
    private TaskManagerLocation resolveTaskManagerLocation(
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation) throws FlinkException {
        try {
            if (retrieveTaskManagerHostName) {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.RETRIEVE_HOST_NAME);
            } else {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.USE_IP_ONLY);
            }
        } catch (Throwable throwable) {
            final String errMsg =
                    String.format(
                            "TaskManager address %s cannot be resolved. %s",
                            unresolvedTaskManagerLocation.getExternalAddress(),
                            throwable.getMessage());
            throw new FlinkException(errMsg, throwable);
        }
    }

    @Override
    public void disconnectResourceManager(
            final ResourceManagerId resourceManagerId, final Exception cause) {

        if (isConnectingToResourceManager(resourceManagerId)) {
            reconnectToResourceManager(cause);
        }
    }

    private boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
        return resourceManagerAddress != null
                && resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromTaskManager(
            final ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
        return taskManagerHeartbeatManager.receiveHeartbeat(resourceID, payload);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(final ResourceID resourceID) {
        return resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Duration timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJobStatus());
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Duration timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJob());
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(Duration timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestCheckpointStats());
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            final CheckpointType checkpointType, final Duration timeout) {
        return schedulerNG.triggerCheckpoint(checkpointType);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            final Duration timeout) {

        return schedulerNG.triggerSavepoint(targetDirectory, cancelJob, formatType);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final SavepointFormatType formatType,
            final boolean terminate,
            final Duration timeout) {

        return schedulerNG.stopWithSavepoint(targetDirectory, terminate, formatType);
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        slotPoolService.notifyNotEnoughResourcesAvailable(acquiredResources);
    }

    @Override
    public CompletableFuture<Object> updateGlobalAggregate(
            String aggregateName, Object aggregand, byte[] serializedAggregateFunction) {

        AggregateFunction aggregateFunction = null;
        try {
            aggregateFunction =
                    InstantiationUtil.deserializeObject(
                            serializedAggregateFunction, userCodeLoader);
        } catch (Exception e) {
            log.error("Error while attempting to deserialize user AggregateFunction.");
            return FutureUtils.completedExceptionally(e);
        }

        Object accumulator = accumulators.get(aggregateName);
        if (null == accumulator) {
            accumulator = aggregateFunction.createAccumulator();
        }
        accumulator = aggregateFunction.add(aggregand, accumulator);
        accumulators.put(aggregateName, accumulator);
        return CompletableFuture.completedFuture(aggregateFunction.getResult(accumulator));
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Duration timeout) {
        return this.sendRequestToCoordinator(operatorId, serializedRequest);
    }

    @Override
    public CompletableFuture<?> stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> partitionIds) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            partitionTracker.stopTrackingAndReleasePartitions(partitionIds, false);
            future.complete(null);
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    @Override
    public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        this.partitionsToFetch = expectedPartitions;
        this.fetchPartitionsFuture = new CompletableFuture<>();

        // check already fetched partitions
        checkPartitionOnTaskManagerReportFinished();

        return FutureUtils.orTimeout(
                        fetchPartitionsFuture, timeout.toMillis(), TimeUnit.MILLISECONDS, null)
                .handleAsync(
                        (metrics, throwable) -> {
                            stopFetchAndRetainPartitionWithMetricsOnTaskManager();

                            if (throwable != null) {
                                if (throwable instanceof TimeoutException) {
                                    log.warn(
                                            "Timeout occurred after {} ms "
                                                    + "while fetching partition(s) ({}) from task managers.",
                                            timeout.toMillis(),
                                            expectedPartitions);

                                    return new ArrayList<>(fetchedPartitionsWithMetrics.values());
                                }
                                throw new CompletionException(throwable);
                            }

                            return new ArrayList<>(fetchedPartitionsWithMetrics.values());
                        },
                        getMainThreadExecutor());
    }

    @VisibleForTesting
    Map<ResultPartitionID, PartitionWithMetrics> getPartitionWithMetricsOnTaskManagers() {
        return fetchedPartitionsWithMetrics;
    }

    @Override
    public void startFetchAndRetainPartitionWithMetricsOnTaskManager() {
        fetchAndRetainPartitions = true;

        // process all already registered task managers
        registeredTaskManagers
                .keySet()
                .forEach(this::fetchAndRetainPartitionWithMetricsOnTaskManager);
    }

    private void fetchAndRetainPartitionWithMetricsOnTaskManager(ResourceID resourceId) {
        TaskManagerRegistration taskManager = registeredTaskManagers.get(resourceId);
        checkNotNull(taskManager);

        taskManager
                .getTaskExecutorGateway()
                .getAndRetainPartitionWithMetrics(executionPlan.getJobID())
                .thenAccept(
                        partitionWithMetrics -> {
                            if (fetchAndRetainPartitions) {
                                for (PartitionWithMetrics partitionWithMetric :
                                        partitionWithMetrics) {
                                    log.debug(
                                            "Received partition metrics for {} from Task Manager {}.",
                                            partitionWithMetric
                                                    .getPartition()
                                                    .getResultPartitionID(),
                                            resourceId);
                                    fetchedPartitionsWithMetrics.put(
                                            partitionWithMetric
                                                    .getPartition()
                                                    .getResultPartitionID(),
                                            partitionWithMetric);
                                }
                                checkPartitionOnTaskManagerReportFinished();
                            } else {
                                log.info(
                                        "Received late report of partition metrics from {}. Release the partitions.",
                                        resourceId);

                                taskManager
                                        .getTaskExecutorGateway()
                                        .releasePartitions(
                                                executionPlan.getJobID(),
                                                partitionWithMetrics.stream()
                                                        .map(PartitionWithMetrics::getPartition)
                                                        .map(
                                                                ShuffleDescriptor
                                                                        ::getResultPartitionID)
                                                        .collect(Collectors.toSet()));
                            }
                        });
    }

    private void stopFetchAndRetainPartitionWithMetricsOnTaskManager() {
        fetchAndRetainPartitions = false;
    }

    private void checkPartitionOnTaskManagerReportFinished() {
        if (fetchPartitionsFuture != null) {
            if (fetchedPartitionsWithMetrics.keySet().containsAll(partitionsToFetch)
                    && !fetchPartitionsFuture.isDone()) {
                fetchPartitionsFuture.complete(fetchedPartitionsWithMetrics.values());
            }
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
        blocklistHandler.addNewBlockedNodes(newNodes);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<JobResourceRequirements> requestJobResourceRequirements() {
        return CompletableFuture.completedFuture(schedulerNG.requestJobResourceRequirements());
    }

    @Override
    public CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobResourceRequirements jobResourceRequirements) {
        schedulerNG.updateJobResourceRequirements(jobResourceRequirements);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------------------------------
    // Internal methods
    // ----------------------------------------------------------------------------------------------

    // -- job starting and stopping
    // -----------------------------------------------------------------

    private void startJobExecution() throws Exception {
        validateRunsInMainThread();

        JobShuffleContext context = new JobShuffleContextImpl(executionPlan.getJobID(), this);
        shuffleMaster.registerJob(context);

        startJobMasterServices();

        log.info(
                "Starting execution of job '{}' ({}) under job master id {}.",
                executionPlan.getName(),
                executionPlan.getJobID(),
                getFencingToken());

        startScheduling();
    }

    private void startJobMasterServices() throws Exception {
        try {
            this.taskManagerHeartbeatManager = createTaskManagerHeartbeatManager(heartbeatServices);
            this.resourceManagerHeartbeatManager =
                    createResourceManagerHeartbeatManager(heartbeatServices);

            // start the slot pool make sure the slot pool now accepts messages for this leader
            slotPoolService.start(getFencingToken(), getAddress());

            // job is ready to go, try to establish connection with resource manager
            //   - activate leader retrieval for the resource manager
            //   - on notification of the leader, the connection will be established and
            //     the slot pool will start requesting slots
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
        } catch (Exception e) {
            handleStartJobMasterServicesError(e);
        }
    }

    private void handleStartJobMasterServicesError(Exception e) throws Exception {
        try {
            stopJobMasterServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    private void stopJobMasterServices() throws Exception {
        Exception resultingException = null;

        try {
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            resultingException = e;
        }

        // TODO: Distinguish between job termination which should free all slots and a loss of
        // leadership which should keep the slots
        slotPoolService.close();

        stopHeartbeatServices();

        ExceptionUtils.tryRethrowException(resultingException);
    }

    private CompletableFuture<Void> stopJobExecution(final Exception cause) {
        validateRunsInMainThread();

        final CompletableFuture<Void> terminationFuture = stopScheduling();

        return FutureUtils.runAfterwards(
                terminationFuture,
                () -> {
                    shuffleMaster.unregisterJob(executionPlan.getJobID());
                    disconnectTaskManagerResourceManagerConnections(cause);
                    stopJobMasterServices();
                });
    }

    private void disconnectTaskManagerResourceManagerConnections(Exception cause) {
        // disconnect from all registered TaskExecutors
        final Set<ResourceID> taskManagerResourceIds =
                new HashSet<>(registeredTaskManagers.keySet());

        for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
            disconnectTaskManager(taskManagerResourceId, cause);
        }

        // disconnect from resource manager:
        closeResourceManagerConnection(cause);
    }

    private void stopHeartbeatServices() {
        taskManagerHeartbeatManager.stop();
        resourceManagerHeartbeatManager.stop();
    }

    private void startScheduling() {
        schedulerNG.startScheduling();
    }

    private CompletableFuture<Void> stopScheduling() {
        jobManagerJobMetricGroup.close();
        jobStatusListener.stop();

        return schedulerNG.closeAsync();
    }

    // ----------------------------------------------------------------------------------------------

    private void handleJobMasterError(final Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            log.error("Fatal error occurred on JobManager.", cause);
            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        } else {
            jobCompletionActions.jobMasterFailed(cause);
        }
    }

    private void jobStatusChanged(final JobStatus newJobStatus) {
        validateRunsInMainThread();
        if (newJobStatus.isGloballyTerminalState()) {
            CompletableFuture<Void> partitionPromoteFuture;
            if (newJobStatus == JobStatus.FINISHED) {
                Collection<ResultPartitionID> jobPartitions =
                        partitionTracker.getAllTrackedNonClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionTracker.stopTrackingAndReleasePartitions(jobPartitions);
                Collection<ResultPartitionID> clusterPartitions =
                        partitionTracker.getAllTrackedClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionPromoteFuture =
                        partitionTracker.stopTrackingAndPromotePartitions(clusterPartitions);
            } else {
                Collection<ResultPartitionID> allTracked =
                        partitionTracker.getAllTrackedPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionTracker.stopTrackingAndReleasePartitions(allTracked);
                partitionPromoteFuture = CompletableFuture.completedFuture(null);
            }

            final ExecutionGraphInfo executionGraphInfo = schedulerNG.requestJob();

            futureExecutor.execute(
                    () -> {
                        try {
                            partitionPromoteFuture.get();
                        } catch (Throwable e) {
                            // We do not want to fail the job in case of partition releasing and
                            // promoting fail. The TaskExecutors will release the partitions
                            // eventually when they find out the JobMaster is closed.
                            log.warn("Fail to release or promote partitions", e);
                        }
                        jobCompletionActions.jobReachedGloballyTerminalState(executionGraphInfo);
                    });
        }
    }

    private void notifyOfNewResourceManagerLeader(
            final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
        resourceManagerAddress =
                createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);

        reconnectToResourceManager(
                new FlinkException(
                        String.format(
                                "ResourceManager leader changed to new address %s",
                                resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(
            @Nullable String newResourceManagerAddress,
            @Nullable ResourceManagerId resourceManagerId) {
        if (newResourceManagerAddress != null) {
            // the contract is: address == null <=> id == null
            checkNotNull(resourceManagerId);
            return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        } else {
            return null;
        }
    }

    private void reconnectToResourceManager(Exception cause) {
        closeResourceManagerConnection(cause);
        tryConnectToResourceManager();
    }

    private void tryConnectToResourceManager() {
        if (resourceManagerAddress != null) {
            connectToResourceManager();
        }
    }

    private void connectToResourceManager() {
        assert (resourceManagerAddress != null);
        assert (resourceManagerConnection == null);
        assert (establishedResourceManagerConnection == null);

        log.info("Connecting to ResourceManager {}", resourceManagerAddress);

        resourceManagerConnection =
                new ResourceManagerConnection(
                        log,
                        executionPlan.getJobID(),
                        resourceId,
                        getAddress(),
                        getFencingToken(),
                        resourceManagerAddress.getAddress(),
                        resourceManagerAddress.getResourceManagerId(),
                        futureExecutor);

        resourceManagerConnection.start();
    }

    private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
        final ResourceManagerId resourceManagerId = success.getResourceManagerId();

        // verify the response with current connection
        if (resourceManagerConnection != null
                && Objects.equals(
                        resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

            log.info(
                    "JobManager successfully registered at ResourceManager, leader id: {}.",
                    resourceManagerId);

            final ResourceManagerGateway resourceManagerGateway =
                    resourceManagerConnection.getTargetGateway();

            final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

            establishedResourceManagerConnection =
                    new EstablishedResourceManagerConnection(
                            resourceManagerGateway, resourceManagerResourceId);

            blocklistHandler.registerBlocklistListener(resourceManagerGateway);
            slotPoolService.connectToResourceManager(resourceManagerGateway);
            partitionTracker.connectToResourceManager(resourceManagerGateway);

            resourceManagerHeartbeatManager.monitorTarget(
                    resourceManagerResourceId,
                    new ResourceManagerHeartbeatReceiver(resourceManagerGateway));
        } else {
            log.debug(
                    "Ignoring resource manager connection to {} because it's duplicated or outdated.",
                    resourceManagerId);
        }
    }

    private void closeResourceManagerConnection(Exception cause) {
        if (establishedResourceManagerConnection != null) {
            dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
            establishedResourceManagerConnection = null;
        }

        if (resourceManagerConnection != null) {
            // stop a potentially ongoing registration process
            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void dissolveResourceManagerConnection(
            EstablishedResourceManagerConnection establishedResourceManagerConnection,
            Exception cause) {
        final ResourceID resourceManagerResourceID =
                establishedResourceManagerConnection.getResourceManagerResourceID();

        if (log.isDebugEnabled()) {
            log.debug(
                    "Close ResourceManager connection {}.",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause);
        } else {
            log.info(
                    "Close ResourceManager connection {}: {}",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause.getMessage());
        }

        resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

        ResourceManagerGateway resourceManagerGateway =
                establishedResourceManagerConnection.getResourceManagerGateway();
        resourceManagerGateway.disconnectJobManager(
                executionPlan.getJobID(), schedulerNG.requestJobStatus(), cause);
        blocklistHandler.deregisterBlocklistListener(resourceManagerGateway);
        slotPoolService.disconnectResourceManager();
    }

    private String getNodeIdOfTaskManager(ResourceID taskManagerId) {
        checkState(registeredTaskManagers.containsKey(taskManagerId));
        return registeredTaskManagers.get(taskManagerId).getTaskManagerLocation().getNodeId();
    }

    // ----------------------------------------------------------------------------------------------
    // Service methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public JobMasterGateway getGateway() {
        return getSelfGateway(JobMasterGateway.class);
    }

    // ----------------------------------------------------------------------------------------------
    // Utility classes
    // ----------------------------------------------------------------------------------------------

    private static final class TaskExecutorHeartbeatSender
            extends HeartbeatSender<AllocatedSlotReport> {
        private final TaskExecutorGateway taskExecutorGateway;

        private TaskExecutorHeartbeatSender(TaskExecutorGateway taskExecutorGateway) {
            this.taskExecutorGateway = taskExecutorGateway;
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(
                ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
            return taskExecutorGateway.heartbeatFromJobManager(resourceID, allocatedSlotReport);
        }
    }

    private static final class ResourceManagerHeartbeatReceiver extends HeartbeatReceiver<Void> {
        private final ResourceManagerGateway resourceManagerGateway;

        private ResourceManagerHeartbeatReceiver(ResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID, Void payload) {
            return resourceManagerGateway.heartbeatFromJobManager(resourceID);
        }
    }

    private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
            runAsync(
                    () ->
                            notifyOfNewResourceManagerLeader(
                                    leaderAddress,
                                    ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        @Override
        public void handleError(final Exception exception) {
            handleJobMasterError(
                    new Exception("Fatal error in the ResourceManager leader service", exception));
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class ResourceManagerConnection
            extends RegisteredRpcConnection<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection> {
        private final JobID jobID;

        private final ResourceID jobManagerResourceID;

        private final String jobManagerRpcAddress;

        private final JobMasterId jobMasterId;

        ResourceManagerConnection(
                final Logger log,
                final JobID jobID,
                final ResourceID jobManagerResourceID,
                final String jobManagerRpcAddress,
                final JobMasterId jobMasterId,
                final String resourceManagerAddress,
                final ResourceManagerId resourceManagerId,
                final Executor executor) {
            super(log, resourceManagerAddress, resourceManagerId, executor);
            this.jobID = checkNotNull(jobID);
            this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
            this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
            this.jobMasterId = checkNotNull(jobMasterId);
        }

        @Override
        protected RetryingRegistration<
                        ResourceManagerId,
                        ResourceManagerGateway,
                        JobMasterRegistrationSuccess,
                        RegistrationResponse.Rejection>
                generateRegistration() {
            return new RetryingRegistration<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection>(
                    log,
                    getRpcService(),
                    "ResourceManager",
                    ResourceManagerGateway.class,
                    getTargetAddress(),
                    getTargetLeaderId(),
                    jobMasterConfiguration.getRetryingRegistrationConfiguration()) {

                @Override
                protected CompletableFuture<RegistrationResponse> invokeRegistration(
                        ResourceManagerGateway gateway,
                        ResourceManagerId fencingToken,
                        long timeoutMillis) {
                    Duration timeout = Duration.ofMillis(timeoutMillis);

                    return gateway.registerJobMaster(
                            jobMasterId,
                            jobManagerResourceID,
                            jobManagerRpcAddress,
                            jobID,
                            timeout);
                }
            };
        }

        @Override
        protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (this == resourceManagerConnection) {
                            establishResourceManagerConnection(success);
                        }
                    });
        }

        @Override
        protected void onRegistrationRejection(RegistrationResponse.Rejection rejection) {
            handleJobMasterError(
                    new IllegalStateException(
                            "The ResourceManager should never reject a JobMaster registration."));
        }

        @Override
        protected void onRegistrationFailure(final Throwable failure) {
            handleJobMasterError(failure);
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class JobManagerJobStatusListener implements JobStatusListener {

        private volatile boolean running = true;

        @Override
        public void jobStatusChanges(
                final JobID jobId, final JobStatus newJobStatus, final long timestamp) {

            if (running) {
                // run in rpc thread to avoid concurrency
                runAsync(() -> jobStatusChanged(newJobStatus));
            }
        }

        private void stop() {
            running = false;
        }
    }

    private class TaskManagerHeartbeatListener
            implements HeartbeatListener<
                    TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport> {

        @Override
        public void notifyHeartbeatTimeout(ResourceID resourceID) {
            final String message =
                    String.format(
                            "Heartbeat of TaskManager with id %s timed out.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        private void handleTaskManagerConnectionLoss(ResourceID resourceID, Exception cause) {
            validateRunsInMainThread();
            disconnectTaskManager(resourceID, cause);
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "TaskManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(
                ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
            validateRunsInMainThread();
            executionDeploymentReconciler.reconcileExecutionDeployments(
                    resourceID,
                    payload.getExecutionDeploymentReport(),
                    executionDeploymentTracker.getExecutionsOn(resourceID));
            for (AccumulatorSnapshot snapshot :
                    payload.getAccumulatorReport().getAccumulatorSnapshots()) {
                schedulerNG.updateAccumulators(snapshot);
            }
        }

        @Override
        public AllocatedSlotReport retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return slotPoolService.createAllocatedSlotReport(resourceID);
        }
    }

    private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            final String message =
                    String.format(
                            "The heartbeat of ResourceManager with id %s timed out.",
                            resourceId.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceId, new TimeoutException(message));
        }

        private void handleResourceManagerConnectionLoss(ResourceID resourceId, Exception cause) {
            validateRunsInMainThread();
            if (establishedResourceManagerConnection != null
                    && establishedResourceManagerConnection
                            .getResourceManagerResourceID()
                            .equals(resourceId)) {
                reconnectToResourceManager(cause);
            }
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "ResourceManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    private class JobMasterBlocklistContext implements BlocklistContext {

        @Override
        public void blockResources(Collection<BlockedNode> blockedNodes) {
            Set<String> blockedNodeIds =
                    blockedNodes.stream().map(BlockedNode::getNodeId).collect(Collectors.toSet());

            Collection<ResourceID> blockedTaskMangers =
                    registeredTaskManagers.keySet().stream()
                            .filter(
                                    taskManagerId ->
                                            blockedNodeIds.contains(
                                                    getNodeIdOfTaskManager(taskManagerId)))
                            .collect(Collectors.toList());

            blockedTaskMangers.forEach(
                    taskManagerId -> {
                        Exception cause =
                                new FlinkRuntimeException(
                                        String.format(
                                                "TaskManager %s is blocked.",
                                                taskManagerId.getStringWithMetadata()));
                        slotPoolService.releaseFreeSlotsOnTaskManager(taskManagerId, cause);
                    });
        }

        @Override
        public void unblockResources(Collection<BlockedNode> unblockedNodes) {}
    }
}
