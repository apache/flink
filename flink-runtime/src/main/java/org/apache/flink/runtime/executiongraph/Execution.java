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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.CheckpointType.PostCheckpointAction;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELING;
import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.DEPLOYING;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.runtime.execution.ExecutionState.INITIALIZING;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.runtime.execution.ExecutionState.SCHEDULED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times
 * (for recovery, re-computation, re-configuration), this class tracks the state of a single
 * execution of that vertex and the resources.
 *
 * <h2>Lock free state transitions</h2>
 *
 * <p>In several points of the code, we need to deal with possible concurrent state changes and
 * actions. For example, while the call to deploy a task (send it to the TaskManager) happens, the
 * task gets cancelled.
 *
 * <p>We could lock the entire portion of the code (decision to deploy, deploy, set state to
 * running) such that it is guaranteed that any "cancel command" will only pick up after deployment
 * is done and that the "cancel command" call will never overtake the deploying call.
 *
 * <p>This blocks the threads big time, because the remote calls may take long. Depending of their
 * locking behavior, it may even result in distributed deadlocks (unless carefully avoided). We
 * therefore use atomic state updates and occasional double-checking to ensure that the state after
 * a completed call is as expected, and trigger correcting actions if it is not. Many actions are
 * also idempotent (like canceling).
 */
public class Execution
        implements AccessExecution, Archiveable<ArchivedExecution>, LogicalSlot.Payload {

    private static final Logger LOG = DefaultExecutionGraph.LOG;

    private static final int NUM_CANCEL_CALL_TRIES = 3;

    // --------------------------------------------------------------------------------------------

    /** The executor which is used to execute futures. */
    private final Executor executor;

    /** The execution vertex whose task this execution executes. */
    private final ExecutionVertex vertex;

    /** The unique ID marking the specific execution instant of the task. */
    private final ExecutionAttemptID attemptId;

    /**
     * The timestamps when state transitions occurred, indexed by {@link ExecutionState#ordinal()}.
     */
    private final long[] stateTimestamps;

    private final int attemptNumber;

    private final Time rpcTimeout;

    private final Collection<PartitionInfo> partitionInfos;

    /** A future that completes once the Execution reaches a terminal ExecutionState. */
    private final CompletableFuture<ExecutionState> terminalStateFuture;

    private final CompletableFuture<?> releaseFuture;

    private final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture;

    /**
     * Gets completed successfully when the task switched to {@link ExecutionState#INITIALIZING} or
     * {@link ExecutionState#RUNNING}. If the task never switches to those state, but fails
     * immediately, then this future never completes.
     */
    private final CompletableFuture<?> initializingOrRunningFuture;

    private volatile ExecutionState state = CREATED;

    private LogicalSlot assignedResource;

    private Optional<ErrorInfo> failureCause =
            Optional.empty(); // once an ErrorInfo is set, never changes

    /**
     * Information to restore the task on recovery, such as checkpoint id and task state snapshot.
     */
    @Nullable private JobManagerTaskRestore taskRestore;

    /** This field holds the allocation id once it was assigned successfully. */
    @Nullable private AllocationID assignedAllocationID;

    // ------------------------ Accumulators & Metrics ------------------------

    /**
     * Lock for updating the accumulators atomically. Prevents final accumulators to be overwritten
     * by partial accumulators on a late heartbeat.
     */
    private final Object accumulatorLock = new Object();

    /* Continuously updated map of user-defined accumulators */
    private Map<String, Accumulator<?, ?>> userAccumulators;

    private IOMetrics ioMetrics;

    private Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>
            producedPartitions;

    // --------------------------------------------------------------------------------------------

    /**
     * Creates a new Execution attempt.
     *
     * @param executor The executor used to dispatch callbacks from futures and asynchronous RPC
     *     calls.
     * @param vertex The execution vertex to which this Execution belongs
     * @param attemptNumber The execution attempt number.
     * @param startTimestamp The timestamp that marks the creation of this Execution
     * @param rpcTimeout The rpcTimeout for RPC calls like deploy/cancel/stop.
     */
    public Execution(
            Executor executor,
            ExecutionVertex vertex,
            int attemptNumber,
            long startTimestamp,
            Time rpcTimeout) {

        this.executor = checkNotNull(executor);
        this.vertex = checkNotNull(vertex);
        this.attemptId = new ExecutionAttemptID();
        this.rpcTimeout = checkNotNull(rpcTimeout);

        this.attemptNumber = attemptNumber;

        this.stateTimestamps = new long[ExecutionState.values().length];
        markTimestamp(CREATED, startTimestamp);

        this.partitionInfos = new ArrayList<>(16);
        this.producedPartitions = Collections.emptyMap();
        this.terminalStateFuture = new CompletableFuture<>();
        this.releaseFuture = new CompletableFuture<>();
        this.taskManagerLocationFuture = new CompletableFuture<>();
        this.initializingOrRunningFuture = new CompletableFuture<>();

        this.assignedResource = null;
    }

    // --------------------------------------------------------------------------------------------
    //   Properties
    // --------------------------------------------------------------------------------------------

    public ExecutionVertex getVertex() {
        return vertex;
    }

    @Override
    public ExecutionAttemptID getAttemptId() {
        return attemptId;
    }

    @Override
    public int getAttemptNumber() {
        return attemptNumber;
    }

    @Override
    public ExecutionState getState() {
        return state;
    }

    @Nullable
    public AllocationID getAssignedAllocationID() {
        return assignedAllocationID;
    }

    public CompletableFuture<TaskManagerLocation> getTaskManagerLocationFuture() {
        return taskManagerLocationFuture;
    }

    public LogicalSlot getAssignedResource() {
        return assignedResource;
    }

    public Optional<ResultPartitionDeploymentDescriptor> getResultPartitionDeploymentDescriptor(
            IntermediateResultPartitionID id) {
        return Optional.ofNullable(producedPartitions.get(id));
    }

    /**
     * Tries to assign the given slot to the execution. The assignment works only if the Execution
     * is in state SCHEDULED. Returns true, if the resource could be assigned.
     *
     * @param logicalSlot to assign to this execution
     * @return true if the slot could be assigned to the execution, otherwise false
     */
    public boolean tryAssignResource(final LogicalSlot logicalSlot) {

        assertRunningInJobMasterMainThread();

        checkNotNull(logicalSlot);

        // only allow to set the assigned resource in state SCHEDULED or CREATED
        // note: we also accept resource assignment when being in state CREATED for testing purposes
        if (state == SCHEDULED || state == CREATED) {
            if (assignedResource == null) {
                assignedResource = logicalSlot;
                if (logicalSlot.tryAssignPayload(this)) {
                    // check for concurrent modification (e.g. cancelling call)
                    if ((state == SCHEDULED || state == CREATED)
                            && !taskManagerLocationFuture.isDone()) {
                        taskManagerLocationFuture.complete(logicalSlot.getTaskManagerLocation());
                        assignedAllocationID = logicalSlot.getAllocationId();
                        return true;
                    } else {
                        // free assigned resource and return false
                        assignedResource = null;
                        return false;
                    }
                } else {
                    assignedResource = null;
                    return false;
                }
            } else {
                // the slot already has another slot assigned
                return false;
            }
        } else {
            // do not allow resource assignment if we are not in state SCHEDULED
            return false;
        }
    }

    public InputSplit getNextInputSplit() {
        final LogicalSlot slot = this.getAssignedResource();
        final String host = slot != null ? slot.getTaskManagerLocation().getHostname() : null;
        return this.vertex.getNextInputSplit(host);
    }

    @Override
    public TaskManagerLocation getAssignedResourceLocation() {
        // returns non-null only when a location is already assigned
        final LogicalSlot currentAssignedResource = assignedResource;
        return currentAssignedResource != null
                ? currentAssignedResource.getTaskManagerLocation()
                : null;
    }

    @Override
    public Optional<ErrorInfo> getFailureInfo() {
        return failureCause;
    }

    @Override
    public long[] getStateTimestamps() {
        return stateTimestamps;
    }

    @Override
    public long getStateTimestamp(ExecutionState state) {
        return this.stateTimestamps[state.ordinal()];
    }

    public boolean isFinished() {
        return state.isTerminal();
    }

    @Nullable
    public JobManagerTaskRestore getTaskRestore() {
        return taskRestore;
    }

    /**
     * Sets the initial state for the execution. The serialized state is then shipped via the {@link
     * TaskDeploymentDescriptor} to the TaskManagers.
     *
     * @param taskRestore information to restore the state
     */
    public void setInitialState(JobManagerTaskRestore taskRestore) {
        this.taskRestore = taskRestore;
    }

    /**
     * Gets a future that completes once the task execution reaches one of the states {@link
     * ExecutionState#INITIALIZING} or {@link ExecutionState#RUNNING}. If this task never reaches
     * these states (for example because the task is cancelled before it was properly deployed and
     * restored), then this future will never complete.
     *
     * <p>The future is completed already in the {@link ExecutionState#INITIALIZING} state, because
     * various running actions are already possible in that state (the task already accepts and
     * sends events and network data for task recovery). (Note that in earlier versions, the
     * INITIALIZING state was not separate but part of the RUNNING state).
     *
     * <p>This future is always completed from the job master's main thread.
     */
    public CompletableFuture<?> getInitializingOrRunningFuture() {
        return initializingOrRunningFuture;
    }

    /**
     * Gets a future that completes once the task execution reaches a terminal state. The future
     * will be completed with specific state that the execution reached. This future is always
     * completed from the job master's main thread.
     *
     * @return A future which is completed once the execution reaches a terminal state
     */
    @Override
    public CompletableFuture<ExecutionState> getTerminalStateFuture() {
        return terminalStateFuture;
    }

    /**
     * Gets the release future which is completed once the execution reaches a terminal state and
     * the assigned resource has been released. This future is always completed from the job
     * master's main thread.
     *
     * @return A future which is completed once the assigned resource has been released
     */
    public CompletableFuture<?> getReleaseFuture() {
        return releaseFuture;
    }

    // --------------------------------------------------------------------------------------------
    //  Actions
    // --------------------------------------------------------------------------------------------

    public CompletableFuture<Void> registerProducedPartitions(
            TaskManagerLocation location, boolean notifyPartitionDataAvailable) {

        assertRunningInJobMasterMainThread();

        return FutureUtils.thenApplyAsyncIfNotDone(
                registerProducedPartitions(
                        vertex, location, attemptId, notifyPartitionDataAvailable),
                vertex.getExecutionGraphAccessor().getJobMasterMainThreadExecutor(),
                producedPartitionsCache -> {
                    producedPartitions = producedPartitionsCache;

                    if (getState() == SCHEDULED) {
                        startTrackingPartitions(
                                location.getResourceID(), producedPartitionsCache.values());
                    } else {
                        LOG.info(
                                "Discarding late registered partitions for {} task {}.",
                                getState(),
                                attemptId);
                        for (ResultPartitionDeploymentDescriptor desc :
                                producedPartitionsCache.values()) {
                            getVertex()
                                    .getExecutionGraphAccessor()
                                    .getShuffleMaster()
                                    .releasePartitionExternally(desc.getShuffleDescriptor());
                        }
                    }
                    return null;
                });
    }

    @VisibleForTesting
    static CompletableFuture<
                    Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>>
            registerProducedPartitions(
                    ExecutionVertex vertex,
                    TaskManagerLocation location,
                    ExecutionAttemptID attemptId,
                    boolean notifyPartitionDataAvailable) {

        ProducerDescriptor producerDescriptor = ProducerDescriptor.create(location, attemptId);

        Collection<IntermediateResultPartition> partitions =
                vertex.getProducedPartitions().values();
        Collection<CompletableFuture<ResultPartitionDeploymentDescriptor>> partitionRegistrations =
                new ArrayList<>(partitions.size());

        for (IntermediateResultPartition partition : partitions) {
            PartitionDescriptor partitionDescriptor = PartitionDescriptor.from(partition);
            int maxParallelism =
                    getPartitionMaxParallelism(
                            partition,
                            vertex.getExecutionGraphAccessor()::getExecutionVertexOrThrow);
            CompletableFuture<? extends ShuffleDescriptor> shuffleDescriptorFuture =
                    vertex.getExecutionGraphAccessor()
                            .getShuffleMaster()
                            .registerPartitionWithProducer(
                                    vertex.getJobId(), partitionDescriptor, producerDescriptor);

            CompletableFuture<ResultPartitionDeploymentDescriptor> partitionRegistration =
                    shuffleDescriptorFuture.thenApply(
                            shuffleDescriptor ->
                                    new ResultPartitionDeploymentDescriptor(
                                            partitionDescriptor,
                                            shuffleDescriptor,
                                            maxParallelism,
                                            notifyPartitionDataAvailable));
            partitionRegistrations.add(partitionRegistration);
        }

        return FutureUtils.combineAll(partitionRegistrations)
                .thenApply(
                        rpdds -> {
                            Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>
                                    producedPartitions = new LinkedHashMap<>(partitions.size());
                            rpdds.forEach(
                                    rpdd -> producedPartitions.put(rpdd.getPartitionId(), rpdd));
                            return producedPartitions;
                        });
    }

    private static int getPartitionMaxParallelism(
            IntermediateResultPartition partition,
            Function<ExecutionVertexID, ExecutionVertex> getVertexById) {
        final List<ConsumerVertexGroup> consumerVertexGroups = partition.getConsumerVertexGroups();
        Preconditions.checkArgument(
                consumerVertexGroups.size() == 1,
                "Currently there has to be exactly one consumer in real jobs");
        final ConsumerVertexGroup consumerVertexGroup = consumerVertexGroups.get(0);
        return getVertexById
                .apply(consumerVertexGroup.getFirst())
                .getJobVertex()
                .getMaxParallelism();
    }

    /**
     * Deploys the execution to the previously assigned resource.
     *
     * @throws JobException if the execution cannot be deployed to the assigned resource
     */
    public void deploy() throws JobException {
        assertRunningInJobMasterMainThread();

        final LogicalSlot slot = assignedResource;

        checkNotNull(
                slot,
                "In order to deploy the execution we first have to assign a resource via tryAssignResource.");

        // Check if the TaskManager died in the meantime
        // This only speeds up the response to TaskManagers failing concurrently to deployments.
        // The more general check is the rpcTimeout of the deployment call
        if (!slot.isAlive()) {
            throw new JobException("Target slot (TaskManager) for deployment is no longer alive.");
        }

        // make sure exactly one deployment call happens from the correct state
        // note: the transition from CREATED to DEPLOYING is for testing purposes only
        ExecutionState previous = this.state;
        if (previous == SCHEDULED || previous == CREATED) {
            if (!transitionState(previous, DEPLOYING)) {
                // race condition, someone else beat us to the deploying call.
                // this should actually not happen and indicates a race somewhere else
                throw new IllegalStateException(
                        "Cannot deploy task: Concurrent deployment call race.");
            }
        } else {
            // vertex may have been cancelled, or it was already scheduled
            throw new IllegalStateException(
                    "The vertex must be in CREATED or SCHEDULED state to be deployed. Found state "
                            + previous);
        }

        if (this != slot.getPayload()) {
            throw new IllegalStateException(
                    String.format(
                            "The execution %s has not been assigned to the assigned slot.", this));
        }

        try {

            // race double check, did we fail/cancel and do we need to release the slot?
            if (this.state != DEPLOYING) {
                slot.releaseSlot(
                        new FlinkException(
                                "Actual state of execution "
                                        + this
                                        + " ("
                                        + state
                                        + ") does not match expected state DEPLOYING."));
                return;
            }

            LOG.info(
                    "Deploying {} (attempt #{}) with attempt id {} to {} with allocation id {}",
                    vertex.getTaskNameWithSubtaskIndex(),
                    attemptNumber,
                    vertex.getCurrentExecutionAttempt().getAttemptId(),
                    getAssignedResourceLocation(),
                    slot.getAllocationId());

            final TaskDeploymentDescriptor deployment =
                    TaskDeploymentDescriptorFactory.fromExecutionVertex(vertex, attemptNumber)
                            .createDeploymentDescriptor(
                                    slot.getAllocationId(),
                                    taskRestore,
                                    producedPartitions.values());

            // null taskRestore to let it be GC'ed
            taskRestore = null;

            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            final ComponentMainThreadExecutor jobMasterMainThreadExecutor =
                    vertex.getExecutionGraphAccessor().getJobMasterMainThreadExecutor();

            getVertex().notifyPendingDeployment(this);
            // We run the submission in the future executor so that the serialization of large TDDs
            // does not block
            // the main thread and sync back to the main thread once submission is completed.
            CompletableFuture.supplyAsync(
                            () -> taskManagerGateway.submitTask(deployment, rpcTimeout), executor)
                    .thenCompose(Function.identity())
                    .whenCompleteAsync(
                            (ack, failure) -> {
                                if (failure == null) {
                                    vertex.notifyCompletedDeployment(this);
                                } else {
                                    final Throwable actualFailure =
                                            ExceptionUtils.stripCompletionException(failure);

                                    if (actualFailure instanceof TimeoutException) {
                                        String taskname =
                                                vertex.getTaskNameWithSubtaskIndex()
                                                        + " ("
                                                        + attemptId
                                                        + ')';

                                        markFailed(
                                                new Exception(
                                                        "Cannot deploy task "
                                                                + taskname
                                                                + " - TaskManager ("
                                                                + getAssignedResourceLocation()
                                                                + ") not responding after a rpcTimeout of "
                                                                + rpcTimeout,
                                                        actualFailure));
                                    } else {
                                        markFailed(actualFailure);
                                    }
                                }
                            },
                            jobMasterMainThreadExecutor);

        } catch (Throwable t) {
            markFailed(t);
        }
    }

    public void cancel() {
        // depending on the previous state, we go directly to cancelled (no cancel call necessary)
        // -- or to canceling (cancel call needs to be sent to the task manager)

        // because of several possibly previous states, we need to again loop until we make a
        // successful atomic state transition
        assertRunningInJobMasterMainThread();
        while (true) {

            ExecutionState current = this.state;

            if (current == CANCELING || current == CANCELED) {
                // already taken care of, no need to cancel again
                return;
            }

            // these two are the common cases where we need to send a cancel call
            else if (current == INITIALIZING || current == RUNNING || current == DEPLOYING) {
                // try to transition to canceling, if successful, send the cancel call
                if (startCancelling(NUM_CANCEL_CALL_TRIES)) {
                    return;
                }
                // else: fall through the loop
            } else if (current == FINISHED) {
                // finished before it could be cancelled.
                // in any case, the task is removed from the TaskManager already

                // a pipelined partition whose consumer has never been deployed could still be
                // buffered on the TM
                // release it here since pipelined partitions for FINISHED executions aren't handled
                // elsewhere
                // covers the following cases:
                // 		a) restarts of this vertex
                // 		b) a global failure (which may result in a FAILED job state)
                sendReleaseIntermediateResultPartitionsRpcCall();

                return;
            } else if (current == FAILED) {
                // failed before it could be cancelled.
                // in any case, the task is removed from the TaskManager already

                return;
            } else if (current == CREATED || current == SCHEDULED) {
                // from here, we can directly switch to cancelled, because no task has been deployed
                if (cancelAtomically()) {
                    return;
                }
                // else: fall through the loop
            } else {
                throw new IllegalStateException(current.name());
            }
        }
    }

    public CompletableFuture<?> suspend() {
        switch (state) {
            case RUNNING:
            case INITIALIZING:
            case DEPLOYING:
            case CREATED:
            case SCHEDULED:
                if (!cancelAtomically()) {
                    throw new IllegalStateException(
                            String.format(
                                    "Could not directly go to %s from %s.",
                                    CANCELED.name(), state.name()));
                }
                break;
            case CANCELING:
                completeCancelling();
                break;
            case FINISHED:
                // a pipelined partition whose consumer has never been deployed could still be
                // buffered on the TM
                // release it here since pipelined partitions for FINISHED executions aren't handled
                // elsewhere
                // most notably, the TaskExecutor does not release pipelined partitions when
                // disconnecting from the JM
                sendReleaseIntermediateResultPartitionsRpcCall();
                break;
            case FAILED:
            case CANCELED:
                break;
            default:
                throw new IllegalStateException(state.name());
        }

        return releaseFuture;
    }

    private void updatePartitionConsumers(final IntermediateResultPartition partition) {

        final List<ConsumerVertexGroup> consumerVertexGroups = partition.getConsumerVertexGroups();

        if (consumerVertexGroups.size() == 0) {
            return;
        }
        if (consumerVertexGroups.size() > 1) {
            fail(
                    new IllegalStateException(
                            "Currently, only a single consumer group per partition is supported."));
            return;
        }

        for (ExecutionVertexID consumerVertexId : consumerVertexGroups.get(0)) {
            final ExecutionVertex consumerVertex =
                    vertex.getExecutionGraphAccessor().getExecutionVertexOrThrow(consumerVertexId);
            final Execution consumer = consumerVertex.getCurrentExecutionAttempt();
            final ExecutionState consumerState = consumer.getState();

            // ----------------------------------------------------------------
            // Consumer is recovering or running => send update message now
            // Consumer is deploying => cache the partition info which would be
            // sent after switching to running
            // ----------------------------------------------------------------
            if (consumerState == DEPLOYING
                    || consumerState == RUNNING
                    || consumerState == INITIALIZING) {
                final PartitionInfo partitionInfo = createPartitionInfo(partition);

                if (consumerState == DEPLOYING) {
                    consumerVertex.cachePartitionInfo(partitionInfo);
                } else {
                    consumer.sendUpdatePartitionInfoRpcCall(Collections.singleton(partitionInfo));
                }
            }
        }
    }

    private static PartitionInfo createPartitionInfo(
            IntermediateResultPartition consumedPartition) {
        IntermediateDataSetID intermediateDataSetID =
                consumedPartition.getIntermediateResult().getId();
        ShuffleDescriptor shuffleDescriptor =
                getConsumedPartitionShuffleDescriptor(
                        consumedPartition,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.MUST_BE_KNOWN);
        return new PartitionInfo(intermediateDataSetID, shuffleDescriptor);
    }

    /**
     * This method fails the vertex due to an external condition. The task will move to state
     * FAILED. If the task was in state RUNNING or DEPLOYING before, it will send a cancel call to
     * the TaskManager.
     *
     * @param t The exception that caused the task to fail.
     */
    @Override
    public void fail(Throwable t) {
        processFail(t, true);
    }

    /**
     * Notify the task of this execution about a completed checkpoint.
     *
     * @param checkpointId of the completed checkpoint
     * @param timestamp of the completed checkpoint
     */
    public void notifyCheckpointComplete(long checkpointId, long timestamp) {
        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            taskManagerGateway.notifyCheckpointComplete(
                    attemptId, getVertex().getJobId(), checkpointId, timestamp);
        } else {
            LOG.debug(
                    "The execution has no slot assigned. This indicates that the execution is "
                            + "no longer running.");
        }
    }

    /**
     * Notify the task of this execution about a aborted checkpoint.
     *
     * @param abortCheckpointId of the subsumed checkpoint
     * @param latestCompletedCheckpointId of the latest completed checkpoint
     * @param timestamp of the subsumed checkpoint
     */
    public void notifyCheckpointAborted(
            long abortCheckpointId, long latestCompletedCheckpointId, long timestamp) {
        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            taskManagerGateway.notifyCheckpointAborted(
                    attemptId,
                    getVertex().getJobId(),
                    abortCheckpointId,
                    latestCompletedCheckpointId,
                    timestamp);
        } else {
            LOG.debug(
                    "The execution has no slot assigned. This indicates that the execution is "
                            + "no longer running.");
        }
    }

    /**
     * Trigger a new checkpoint on the task of this execution.
     *
     * @param checkpointId of th checkpoint to trigger
     * @param timestamp of the checkpoint to trigger
     * @param checkpointOptions of the checkpoint to trigger
     * @return Future acknowledge which is returned once the checkpoint has been triggered
     */
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
        return triggerCheckpointHelper(checkpointId, timestamp, checkpointOptions);
    }

    /**
     * Trigger a new checkpoint on the task of this execution.
     *
     * @param checkpointId of th checkpoint to trigger
     * @param timestamp of the checkpoint to trigger
     * @param checkpointOptions of the checkpoint to trigger
     * @return Future acknowledge which is returned once the checkpoint has been triggered
     */
    public CompletableFuture<Acknowledge> triggerSynchronousSavepoint(
            long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
        return triggerCheckpointHelper(checkpointId, timestamp, checkpointOptions);
    }

    private CompletableFuture<Acknowledge> triggerCheckpointHelper(
            long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {

        final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
        if (checkpointType.getPostCheckpointAction() == PostCheckpointAction.TERMINATE
                && !(checkpointType.isSynchronous() && checkpointType.isSavepoint())) {
            throw new IllegalArgumentException(
                    "Only synchronous savepoints are allowed to advance the watermark to MAX.");
        }

        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            return taskManagerGateway.triggerCheckpoint(
                    attemptId, getVertex().getJobId(), checkpointId, timestamp, checkpointOptions);
        }
        LOG.debug(
                "The execution has no slot assigned. This indicates that the execution is no longer running.");
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * Sends the operator event to the Task on the Task Executor.
     *
     * @return True, of the message was sent, false is the task is currently not running.
     */
    public CompletableFuture<Acknowledge> sendOperatorEvent(
            OperatorID operatorId, SerializedValue<OperatorEvent> event) {

        assertRunningInJobMasterMainThread();
        final LogicalSlot slot = assignedResource;

        if (slot != null && (getState() == RUNNING || getState() == INITIALIZING)) {
            final TaskExecutorOperatorEventGateway eventGateway = slot.getTaskManagerGateway();
            return eventGateway.sendOperatorEventToTask(getAttemptId(), operatorId, event);
        } else {
            return FutureUtils.completedExceptionally(
                    new TaskNotRunningException(
                            '"'
                                    + vertex.getTaskNameWithSubtaskIndex()
                                    + "\" is not running, but in state "
                                    + getState()));
        }
    }

    // --------------------------------------------------------------------------------------------
    //   Callbacks
    // --------------------------------------------------------------------------------------------

    /**
     * This method marks the task as failed, but will make no attempt to remove task execution from
     * the task manager. It is intended for cases where the task is known not to be running, or then
     * the TaskManager reports failure (in which case it has already removed the task).
     *
     * @param t The exception that caused the task to fail.
     */
    void markFailed(Throwable t) {
        processFail(t, false);
    }

    void markFailed(
            Throwable t,
            boolean cancelTask,
            Map<String, Accumulator<?, ?>> userAccumulators,
            IOMetrics metrics,
            boolean releasePartitions,
            boolean fromSchedulerNg) {
        processFail(t, cancelTask, userAccumulators, metrics, releasePartitions, fromSchedulerNg);
    }

    @VisibleForTesting
    public void markFinished() {
        markFinished(null, null);
    }

    void markFinished(Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {

        assertRunningInJobMasterMainThread();

        // this call usually comes during RUNNING, but may also come while still in deploying (very
        // fast tasks!)
        while (true) {
            ExecutionState current = this.state;

            if (current == INITIALIZING || current == RUNNING || current == DEPLOYING) {

                if (transitionState(current, FINISHED)) {
                    try {
                        finishPartitionsAndUpdateConsumers();
                        updateAccumulatorsAndMetrics(userAccumulators, metrics);
                        releaseAssignedResource(null);
                        vertex.getExecutionGraphAccessor().deregisterExecution(this);
                    } finally {
                        vertex.executionFinished(this);
                    }
                    return;
                }
            } else if (current == CANCELING) {
                // we sent a cancel call, and the task manager finished before it arrived. We
                // will never get a CANCELED call back from the job manager
                completeCancelling(userAccumulators, metrics, true);
                return;
            } else if (current == CANCELED || current == FAILED) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Task FINISHED, but concurrently went to state " + state);
                }
                return;
            } else {
                // this should not happen, we need to fail this
                markFailed(
                        new Exception(
                                "Vertex received FINISHED message while being in state " + state));
                return;
            }
        }
    }

    private void finishPartitionsAndUpdateConsumers() {
        final List<IntermediateResultPartition> finishedPartitions =
                getVertex().finishAllBlockingPartitions();

        for (IntermediateResultPartition partition : finishedPartitions) {
            updatePartitionConsumers(partition);
        }
    }

    private boolean cancelAtomically() {
        if (startCancelling(0)) {
            completeCancelling();
            return true;
        } else {
            return false;
        }
    }

    private boolean startCancelling(int numberCancelRetries) {
        if (transitionState(state, CANCELING)) {
            taskManagerLocationFuture.cancel(false);
            sendCancelRpcCall(numberCancelRetries);
            return true;
        } else {
            return false;
        }
    }

    void completeCancelling() {
        completeCancelling(null, null, true);
    }

    void completeCancelling(
            Map<String, Accumulator<?, ?>> userAccumulators,
            IOMetrics metrics,
            boolean releasePartitions) {

        // the taskmanagers can themselves cancel tasks without an external trigger, if they find
        // that the
        // network stack is canceled (for example by a failing / canceling receiver or sender
        // this is an artifact of the old network runtime, but for now we need to support task
        // transitions
        // from running directly to canceled

        while (true) {
            ExecutionState current = this.state;

            if (current == CANCELED) {
                return;
            } else if (current == CANCELING
                    || current == RUNNING
                    || current == INITIALIZING
                    || current == DEPLOYING) {

                updateAccumulatorsAndMetrics(userAccumulators, metrics);

                if (transitionState(current, CANCELED)) {
                    finishCancellation(releasePartitions);
                    return;
                }

                // else fall through the loop
            } else {
                // failing in the meantime may happen and is no problem.
                // anything else is a serious problem !!!
                if (current != FAILED) {
                    String message =
                            String.format(
                                    "Asynchronous race: Found %s in state %s after successful cancel call.",
                                    vertex.getTaskNameWithSubtaskIndex(), state);
                    LOG.error(message);
                    vertex.getExecutionGraphAccessor().failGlobal(new Exception(message));
                }
                return;
            }
        }
    }

    private void finishCancellation(boolean releasePartitions) {
        releaseAssignedResource(new FlinkException("Execution " + this + " was cancelled."));
        vertex.getExecutionGraphAccessor().deregisterExecution(this);
        handlePartitionCleanup(releasePartitions, releasePartitions);
    }

    void cachePartitionInfo(PartitionInfo partitionInfo) {
        partitionInfos.add(partitionInfo);
    }

    private void sendPartitionInfos() {
        if (!partitionInfos.isEmpty()) {
            sendUpdatePartitionInfoRpcCall(new ArrayList<>(partitionInfos));

            partitionInfos.clear();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Internal Actions
    // --------------------------------------------------------------------------------------------

    private void processFail(Throwable t, boolean cancelTask) {
        processFail(t, cancelTask, null, null, true, false);
    }

    /**
     * Process a execution failure. The failure can be fired by JobManager or reported by
     * TaskManager. If it is fired by JobManager and the execution is already deployed, it needs to
     * send a PRC call to remove the task from TaskManager. It also needs to release the produced
     * partitions if it fails before deployed (because the partitions are possibly already created
     * in external shuffle service) or JobManager proactively fails it (in case that it finishes in
     * TaskManager when JobManager tries to fail it). The failure will be notified to SchedulerNG if
     * it is from within the ExecutionGraph. This is to trigger the failure handling of SchedulerNG
     * to recover this failed execution.
     *
     * @param t Failure cause
     * @param cancelTask Indicating whether to send a PRC call to remove task from TaskManager. True
     *     if the failure is fired by JobManager and the execution is already deployed. Otherwise it
     *     should be false.
     * @param userAccumulators User accumulators
     * @param metrics IO metrics
     * @param releasePartitions Indicating whether to release result partitions produced by this
     *     execution. False if the task is FAILED in TaskManager, otherwise true.
     * @param fromSchedulerNg Indicating whether the failure is from the SchedulerNg. It should be
     *     false if it is from within the ExecutionGraph.
     */
    private void processFail(
            Throwable t,
            boolean cancelTask,
            Map<String, Accumulator<?, ?>> userAccumulators,
            IOMetrics metrics,
            boolean releasePartitions,
            boolean fromSchedulerNg) {

        assertRunningInJobMasterMainThread();

        ExecutionState current = this.state;

        if (current == FAILED) {
            // already failed. It is enough to remember once that we failed (its sad enough)
            return;
        }

        if (current == CANCELED || current == FINISHED) {
            // we are already aborting or are already aborted or we are already finished
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Ignoring transition of vertex {} to {} while being {}.",
                        getVertexWithAttempt(),
                        FAILED,
                        current);
            }
            return;
        }

        if (current == CANCELING) {
            completeCancelling(userAccumulators, metrics, true);
            return;
        }

        if (!fromSchedulerNg) {
            vertex.getExecutionGraphAccessor()
                    .notifySchedulerNgAboutInternalTaskFailure(
                            attemptId, t, cancelTask, releasePartitions);
            return;
        }

        checkState(transitionState(current, FAILED, t));

        // success (in a manner of speaking)
        this.failureCause =
                Optional.of(
                        ErrorInfo.createErrorInfoWithNullableCause(t, getStateTimestamp(FAILED)));

        updateAccumulatorsAndMetrics(userAccumulators, metrics);

        releaseAssignedResource(t);
        vertex.getExecutionGraphAccessor().deregisterExecution(this);

        maybeReleasePartitionsAndSendCancelRpcCall(current, cancelTask, releasePartitions);
    }

    private void maybeReleasePartitionsAndSendCancelRpcCall(
            final ExecutionState stateBeforeFailed,
            final boolean cancelTask,
            final boolean releasePartitions) {

        handlePartitionCleanup(releasePartitions, releasePartitions);

        if (cancelTask
                && (stateBeforeFailed == RUNNING
                        || stateBeforeFailed == INITIALIZING
                        || stateBeforeFailed == DEPLOYING)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending out cancel request, to remove task execution from TaskManager.");
            }

            try {
                if (assignedResource != null) {
                    sendCancelRpcCall(NUM_CANCEL_CALL_TRIES);
                }
            } catch (Throwable tt) {
                // no reason this should ever happen, but log it to be safe
                LOG.error(
                        "Error triggering cancel call while marking task {} as failed.",
                        getVertex().getTaskNameWithSubtaskIndex(),
                        tt);
            }
        }
    }

    boolean switchToRecovering() {
        if (switchTo(DEPLOYING, INITIALIZING)) {
            sendPartitionInfos();
            return true;
        }

        return false;
    }

    boolean switchToRunning() {
        return switchTo(INITIALIZING, RUNNING);
    }

    private boolean switchTo(ExecutionState from, ExecutionState to) {

        if (transitionState(from, to)) {
            return true;
        } else {
            // something happened while the call was in progress.
            // it can mean:
            //  - canceling, while deployment was in progress. state is now canceling, or canceled,
            // if the response overtook
            //  - finishing (execution and finished call overtook the deployment answer, which is
            // possible and happens for fast tasks)
            //  - failed (execution, failure, and failure message overtook the deployment answer)

            ExecutionState currentState = this.state;

            if (currentState == FINISHED || currentState == CANCELED) {
                // do nothing, the task was really fast (nice)
                // or it was canceled really fast
            } else if (currentState == CANCELING || currentState == FAILED) {
                if (LOG.isDebugEnabled()) {
                    // this log statement is guarded because the 'getVertexWithAttempt()' method
                    // performs string concatenations
                    LOG.debug(
                            "Concurrent canceling/failing of {} while deployment was in progress.",
                            getVertexWithAttempt());
                }
                sendCancelRpcCall(NUM_CANCEL_CALL_TRIES);
            } else {
                String message =
                        String.format(
                                "Concurrent unexpected state transition of task %s to %s while deployment was in progress.",
                                getVertexWithAttempt(), currentState);

                LOG.debug(message);

                // undo the deployment
                sendCancelRpcCall(NUM_CANCEL_CALL_TRIES);

                // record the failure
                markFailed(new Exception(message));
            }

            return false;
        }
    }

    /**
     * This method sends a CancelTask message to the instance of the assigned slot.
     *
     * <p>The sending is tried up to NUM_CANCEL_CALL_TRIES times.
     */
    private void sendCancelRpcCall(int numberRetries) {
        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
            final ComponentMainThreadExecutor jobMasterMainThreadExecutor =
                    getVertex().getExecutionGraphAccessor().getJobMasterMainThreadExecutor();

            CompletableFuture<Acknowledge> cancelResultFuture =
                    FutureUtils.retry(
                            () -> taskManagerGateway.cancelTask(attemptId, rpcTimeout),
                            numberRetries,
                            jobMasterMainThreadExecutor);

            cancelResultFuture.whenComplete(
                    (ack, failure) -> {
                        if (failure != null) {
                            fail(new Exception("Task could not be canceled.", failure));
                        }
                    });
        }
    }

    private void startTrackingPartitions(
            final ResourceID taskExecutorId,
            final Collection<ResultPartitionDeploymentDescriptor> partitions) {
        JobMasterPartitionTracker partitionTracker =
                vertex.getExecutionGraphAccessor().getPartitionTracker();
        for (ResultPartitionDeploymentDescriptor partition : partitions) {
            partitionTracker.startTrackingPartition(taskExecutorId, partition);
        }
    }

    void handlePartitionCleanup(
            boolean releasePipelinedPartitions, boolean releaseBlockingPartitions) {
        if (releasePipelinedPartitions) {
            sendReleaseIntermediateResultPartitionsRpcCall();
        }

        final Collection<ResultPartitionID> partitionIds = getPartitionIds();
        final JobMasterPartitionTracker partitionTracker =
                getVertex().getExecutionGraphAccessor().getPartitionTracker();

        if (!partitionIds.isEmpty()) {
            if (releaseBlockingPartitions) {
                LOG.info("Discarding the results produced by task execution {}.", attemptId);
                partitionTracker.stopTrackingAndReleasePartitions(partitionIds);
            } else {
                partitionTracker.stopTrackingPartitions(partitionIds);
            }
        }
    }

    private Collection<ResultPartitionID> getPartitionIds() {
        return producedPartitions.values().stream()
                .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
                .map(ShuffleDescriptor::getResultPartitionID)
                .collect(Collectors.toList());
    }

    private void sendReleaseIntermediateResultPartitionsRpcCall() {
        LOG.info("Discarding the results produced by task execution {}.", attemptId);
        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

            final ShuffleMaster<?> shuffleMaster =
                    getVertex().getExecutionGraphAccessor().getShuffleMaster();

            Set<ResultPartitionID> partitionIds =
                    producedPartitions.values().stream()
                            .filter(
                                    resultPartitionDeploymentDescriptor ->
                                            resultPartitionDeploymentDescriptor
                                                    .getPartitionType()
                                                    .isPipelined())
                            .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
                            .peek(shuffleMaster::releasePartitionExternally)
                            .map(ShuffleDescriptor::getResultPartitionID)
                            .collect(Collectors.toSet());

            if (!partitionIds.isEmpty()) {
                // TODO For some tests this could be a problem when querying too early if all
                // resources were released
                taskManagerGateway.releasePartitions(getVertex().getJobId(), partitionIds);
            }
        }
    }

    /**
     * Update the partition infos on the assigned resource.
     *
     * @param partitionInfos for the remote task
     */
    private void sendUpdatePartitionInfoRpcCall(final Iterable<PartitionInfo> partitionInfos) {

        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
            final TaskManagerLocation taskManagerLocation = slot.getTaskManagerLocation();

            CompletableFuture<Acknowledge> updatePartitionsResultFuture =
                    taskManagerGateway.updatePartitions(attemptId, partitionInfos, rpcTimeout);

            updatePartitionsResultFuture.whenCompleteAsync(
                    (ack, failure) -> {
                        // fail if there was a failure
                        if (failure != null) {
                            fail(
                                    new IllegalStateException(
                                            "Update to task ["
                                                    + getVertexWithAttempt()
                                                    + "] on TaskManager "
                                                    + taskManagerLocation
                                                    + " failed",
                                            failure));
                        }
                    },
                    getVertex().getExecutionGraphAccessor().getJobMasterMainThreadExecutor());
        }
    }

    /**
     * Releases the assigned resource and completes the release future once the assigned resource
     * has been successfully released.
     *
     * @param cause for the resource release, null if none
     */
    private void releaseAssignedResource(@Nullable Throwable cause) {

        assertRunningInJobMasterMainThread();

        final LogicalSlot slot = assignedResource;

        if (slot != null) {
            ComponentMainThreadExecutor jobMasterMainThreadExecutor =
                    getVertex().getExecutionGraphAccessor().getJobMasterMainThreadExecutor();

            slot.releaseSlot(cause)
                    .whenComplete(
                            (Object ignored, Throwable throwable) -> {
                                jobMasterMainThreadExecutor.assertRunningInMainThread();
                                if (throwable != null) {
                                    releaseFuture.completeExceptionally(throwable);
                                } else {
                                    releaseFuture.complete(null);
                                }
                            });
        } else {
            // no assigned resource --> we can directly complete the release future
            releaseFuture.complete(null);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Miscellaneous
    // --------------------------------------------------------------------------------------------

    public void transitionState(ExecutionState targetState) {
        transitionState(state, targetState);
    }

    private boolean transitionState(ExecutionState currentState, ExecutionState targetState) {
        return transitionState(currentState, targetState, null);
    }

    private boolean transitionState(
            ExecutionState currentState, ExecutionState targetState, Throwable error) {
        // sanity check
        if (currentState.isTerminal()) {
            throw new IllegalStateException(
                    "Cannot leave terminal state "
                            + currentState
                            + " to transition to "
                            + targetState
                            + '.');
        }

        if (state == currentState) {
            state = targetState;
            markTimestamp(targetState);

            if (error == null) {
                LOG.info(
                        "{} ({}) switched from {} to {}.",
                        getVertex().getTaskNameWithSubtaskIndex(),
                        getAttemptId(),
                        currentState,
                        targetState);
            } else if (LOG.isInfoEnabled()) {
                LOG.info(
                        "{} ({}) switched from {} to {} on {}.",
                        getVertex().getTaskNameWithSubtaskIndex(),
                        getAttemptId(),
                        currentState,
                        targetState,
                        getLocationInformation(),
                        ExceptionUtils.stripCompletionException(error));
            }

            if (targetState == INITIALIZING || targetState == RUNNING) {
                initializingOrRunningFuture.complete(null);
            } else if (targetState.isTerminal()) {
                // complete the terminal state future
                terminalStateFuture.complete(targetState);
            }

            // make sure that the state transition completes normally.
            // potential errors (in listeners may not affect the main logic)
            try {
                vertex.notifyStateTransition(this, targetState);
            } catch (Throwable t) {
                LOG.error(
                        "Error while notifying execution graph of execution state transition.", t);
            }
            return true;
        } else {
            return false;
        }
    }

    private String getLocationInformation() {
        if (assignedResource != null) {
            return assignedResource.getTaskManagerLocation().toString();
        } else {
            return "[unassigned resource]";
        }
    }

    private void markTimestamp(ExecutionState state) {
        markTimestamp(state, System.currentTimeMillis());
    }

    private void markTimestamp(ExecutionState state, long timestamp) {
        this.stateTimestamps[state.ordinal()] = timestamp;
    }

    public String getVertexWithAttempt() {
        return vertex.getTaskNameWithSubtaskIndex() + " - execution #" + attemptNumber;
    }

    // ------------------------------------------------------------------------
    //  Accumulators
    // ------------------------------------------------------------------------

    /**
     * Update accumulators (discarded when the Execution has already been terminated).
     *
     * @param userAccumulators the user accumulators
     */
    public void setAccumulators(Map<String, Accumulator<?, ?>> userAccumulators) {
        synchronized (accumulatorLock) {
            if (!state.isTerminal()) {
                this.userAccumulators = userAccumulators;
            }
        }
    }

    public Map<String, Accumulator<?, ?>> getUserAccumulators() {
        return userAccumulators;
    }

    @Override
    public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
        Map<String, OptionalFailure<Accumulator<?, ?>>> accumulators =
                userAccumulators == null
                        ? null
                        : userAccumulators.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey,
                                                entry -> OptionalFailure.of(entry.getValue())));
        return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulators);
    }

    @Override
    public int getParallelSubtaskIndex() {
        return getVertex().getParallelSubtaskIndex();
    }

    @Override
    public IOMetrics getIOMetrics() {
        return ioMetrics;
    }

    private void updateAccumulatorsAndMetrics(
            Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {
        if (userAccumulators != null) {
            synchronized (accumulatorLock) {
                this.userAccumulators = userAccumulators;
            }
        }
        if (metrics != null) {
            this.ioMetrics = metrics;
        }
    }

    // ------------------------------------------------------------------------
    //  Standard utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        final LogicalSlot slot = assignedResource;

        return String.format(
                "Attempt #%d (%s) @ %s - [%s]",
                attemptNumber,
                vertex.getTaskNameWithSubtaskIndex(),
                (slot == null ? "(unassigned)" : slot),
                state);
    }

    @Override
    public ArchivedExecution archive() {
        return new ArchivedExecution(this);
    }

    private void assertRunningInJobMasterMainThread() {
        vertex.getExecutionGraphAccessor()
                .getJobMasterMainThreadExecutor()
                .assertRunningInMainThread();
    }
}
