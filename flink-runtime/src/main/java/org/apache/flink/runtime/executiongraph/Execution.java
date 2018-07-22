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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartialInputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.concurrent.FutureConsumerWithException;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELING;
import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.DEPLOYING;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.runtime.execution.ExecutionState.SCHEDULED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times
 * (for recovery, re-computation, re-configuration), this class tracks the state of a single execution
 * of that vertex and the resources.
 *
 * <h2>Lock free state transitions</h2>
 *
 * <p>In several points of the code, we need to deal with possible concurrent state changes and actions.
 * For example, while the call to deploy a task (send it to the TaskManager) happens, the task gets cancelled.
 *
 * <p>We could lock the entire portion of the code (decision to deploy, deploy, set state to running) such that
 * it is guaranteed that any "cancel command" will only pick up after deployment is done and that the "cancel
 * command" call will never overtake the deploying call.
 *
 * <p>This blocks the threads big time, because the remote calls may take long. Depending of their locking behavior, it
 * may even result in distributed deadlocks (unless carefully avoided). We therefore use atomic state updates and
 * occasional double-checking to ensure that the state after a completed call is as expected, and trigger correcting
 * actions if it is not. Many actions are also idempotent (like canceling).
 */
public class Execution implements AccessExecution, Archiveable<ArchivedExecution>, LogicalSlot.Payload {

	private static final AtomicReferenceFieldUpdater<Execution, ExecutionState> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(Execution.class, ExecutionState.class, "state");

	private static final AtomicReferenceFieldUpdater<Execution, LogicalSlot> ASSIGNED_SLOT_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
		Execution.class,
		LogicalSlot.class,
		"assignedResource");

	private static final Logger LOG = ExecutionGraph.LOG;

	private static final int NUM_CANCEL_CALL_TRIES = 3;

	private static final int NUM_STOP_CALL_TRIES = 3;

	// --------------------------------------------------------------------------------------------

	/** The executor which is used to execute futures. */
	private final Executor executor;

	/** The execution vertex whose task this execution executes. */
	private final ExecutionVertex vertex;

	/** The unique ID marking the specific execution instant of the task. */
	private final ExecutionAttemptID attemptId;

	/** Gets the global modification version of the execution graph when this execution was created.
	 * This version is bumped in the ExecutionGraph whenever a global failover happens. It is used
	 * to resolve conflicts between concurrent modification by global and local failover actions. */
	private final long globalModVersion;

	/** The timestamps when state transitions occurred, indexed by {@link ExecutionState#ordinal()}. */
	private final long[] stateTimestamps;

	private final int attemptNumber;

	private final Time rpcTimeout;

	private final ConcurrentLinkedQueue<PartialInputChannelDeploymentDescriptor> partialInputChannelDeploymentDescriptors;

	/** A future that completes once the Execution reaches a terminal ExecutionState. */
	private final CompletableFuture<ExecutionState> terminalStateFuture;

	private final CompletableFuture<?> releaseFuture;

	private final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture;

	private volatile ExecutionState state = CREATED;

	private volatile LogicalSlot assignedResource;

	private volatile Throwable failureCause;          // once assigned, never changes

	/** Information to restore the task on recovery, such as checkpoint id and task state snapshot. */
	@Nullable
	private volatile JobManagerTaskRestore taskRestore;

	/** This field holds the allocation id once it was assigned successfully. */
	@Nullable
	private volatile AllocationID assignedAllocationID;

	// ------------------------ Accumulators & Metrics ------------------------

	/** Lock for updating the accumulators atomically.
	 * Prevents final accumulators to be overwritten by partial accumulators on a late heartbeat. */
	private final Object accumulatorLock = new Object();

	/* Continuously updated map of user-defined accumulators */
	private volatile Map<String, Accumulator<?, ?>> userAccumulators;

	private volatile IOMetrics ioMetrics;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new Execution attempt.
	 *
	 * @param executor
	 *             The executor used to dispatch callbacks from futures and asynchronous RPC calls.
	 * @param vertex
	 *             The execution vertex to which this Execution belongs
	 * @param attemptNumber
	 *             The execution attempt number.
	 * @param globalModVersion
	 *             The global modification version of the execution graph when this execution was created
	 * @param startTimestamp
	 *             The timestamp that marks the creation of this Execution
	 * @param rpcTimeout
	 *             The rpcTimeout for RPC calls like deploy/cancel/stop.
	 */
	public Execution(
			Executor executor,
			ExecutionVertex vertex,
			int attemptNumber,
			long globalModVersion,
			long startTimestamp,
			Time rpcTimeout) {

		this.executor = checkNotNull(executor);
		this.vertex = checkNotNull(vertex);
		this.attemptId = new ExecutionAttemptID();
		this.rpcTimeout = checkNotNull(rpcTimeout);

		this.globalModVersion = globalModVersion;
		this.attemptNumber = attemptNumber;

		this.stateTimestamps = new long[ExecutionState.values().length];
		markTimestamp(ExecutionState.CREATED, startTimestamp);

		this.partialInputChannelDeploymentDescriptors = new ConcurrentLinkedQueue<>();
		this.terminalStateFuture = new CompletableFuture<>();
		this.releaseFuture = new CompletableFuture<>();
		this.taskManagerLocationFuture = new CompletableFuture<>();

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

	/**
	 * Gets the global modification version of the execution graph when this execution was created.
	 *
	 * <p>This version is bumped in the ExecutionGraph whenever a global failover happens. It is used
	 * to resolve conflicts between concurrent modification by global and local failover actions.
	 */
	public long getGlobalModVersion() {
		return globalModVersion;
	}

	public CompletableFuture<TaskManagerLocation> getTaskManagerLocationFuture() {
		return taskManagerLocationFuture;
	}

	public LogicalSlot getAssignedResource() {
		return assignedResource;
	}

	/**
	 * Tries to assign the given slot to the execution. The assignment works only if the
	 * Execution is in state SCHEDULED. Returns true, if the resource could be assigned.
	 *
	 * @param logicalSlot to assign to this execution
	 * @return true if the slot could be assigned to the execution, otherwise false
	 */
	@VisibleForTesting
	boolean tryAssignResource(final LogicalSlot logicalSlot) {
		checkNotNull(logicalSlot);

		// only allow to set the assigned resource in state SCHEDULED or CREATED
		// note: we also accept resource assignment when being in state CREATED for testing purposes
		if (state == SCHEDULED || state == CREATED) {
			if (ASSIGNED_SLOT_UPDATER.compareAndSet(this, null, logicalSlot)) {
				if (logicalSlot.tryAssignPayload(this)) {
					// check for concurrent modification (e.g. cancelling call)
					if ((state == SCHEDULED || state == CREATED) && !taskManagerLocationFuture.isDone()) {
						taskManagerLocationFuture.complete(logicalSlot.getTaskManagerLocation());
						assignedAllocationID = logicalSlot.getAllocationId();
						return true;
					} else {
						// free assigned resource and return false
						ASSIGNED_SLOT_UPDATER.set(this, null);
						return false;
					}
				} else {
					ASSIGNED_SLOT_UPDATER.set(this, null);
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

	@Override
	public TaskManagerLocation getAssignedResourceLocation() {
		// returns non-null only when a location is already assigned
		final LogicalSlot currentAssignedResource = assignedResource;
		return currentAssignedResource != null ? currentAssignedResource.getTaskManagerLocation() : null;
	}

	public Throwable getFailureCause() {
		return failureCause;
	}

	@Override
	public String getFailureCauseAsString() {
		return ExceptionUtils.stringifyException(getFailureCause());
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
	 * Sets the initial state for the execution. The serialized state is then shipped via the
	 * {@link TaskDeploymentDescriptor} to the TaskManagers.
	 *
	 * @param taskRestore information to restore the state
	 */
	public void setInitialState(@Nullable JobManagerTaskRestore taskRestore) {
		checkState(state == CREATED, "Can only assign operator state when execution attempt is in CREATED");
		this.taskRestore = taskRestore;
	}

	/**
	 * Gets a future that completes once the task execution reaches a terminal state.
	 * The future will be completed with specific state that the execution reached.
	 *
	 * @return A future which is completed once the execution reaches a terminal state
	 */
	@Override
	public CompletableFuture<ExecutionState> getTerminalStateFuture() {
		return terminalStateFuture;
	}

	/**
	 * Gets the release future which is completed once the execution reaches a terminal
	 * state and the assigned resource has been released.
	 *
	 * @return A future which is completed once the assigned resource has been released
	 */
	public CompletableFuture<?> getReleaseFuture() {
		return releaseFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public CompletableFuture<Void> scheduleForExecution() {
		final ExecutionGraph executionGraph = getVertex().getExecutionGraph();
		final SlotProvider resourceProvider = executionGraph.getSlotProvider();
		final boolean allowQueued = executionGraph.isQueuedSchedulingAllowed();
		return scheduleForExecution(
			resourceProvider,
			allowQueued,
			LocationPreferenceConstraint.ANY);
	}

	/**
	 * NOTE: This method only throws exceptions if it is in an illegal state to be scheduled, or if the tasks needs
	 *       to be scheduled immediately and no resource is available. If the task is accepted by the schedule, any
	 *       error sets the vertex state to failed and triggers the recovery logic.
	 *
	 * @param slotProvider The slot provider to use to allocate slot for this execution attempt.
	 * @param queued Flag to indicate whether the scheduler may queue this task if it cannot
	 *               immediately deploy it.
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @return Future which is completed once the Execution has been deployed
	 */
	public CompletableFuture<Void> scheduleForExecution(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint) {
		final Time allocationTimeout = vertex.getExecutionGraph().getAllocationTimeout();
		try {
			final CompletableFuture<Execution> allocationFuture = allocateAndAssignSlotForExecution(
				slotProvider,
				queued,
				locationPreferenceConstraint,
				allocationTimeout);

			// IMPORTANT: We have to use the synchronous handle operation (direct executor) here so
			// that we directly deploy the tasks if the slot allocation future is completed. This is
			// necessary for immediate deployment.
			final CompletableFuture<Void> deploymentFuture = allocationFuture.thenAccept(
				(FutureConsumerWithException<Execution, Exception>) value -> deploy());

			deploymentFuture.whenComplete(
				(Void ignored, Throwable failure) -> {
					if (failure != null) {
						markFailed(ExceptionUtils.stripCompletionException(failure));
					}
				});

			// if tasks have to scheduled immediately check that the task has been deployed
			if (!queued && !deploymentFuture.isDone()) {
				deploymentFuture.completeExceptionally(new IllegalArgumentException("The slot allocation future has not been completed yet."));
			}

			return deploymentFuture;
		} catch (IllegalExecutionStateException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	/**
	 * Allocates and assigns a slot obtained from the slot provider to the execution.
	 *
	 * @param slotProvider to obtain a new slot from
	 * @param queued if the allocation can be queued
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allocationTimeout rpcTimeout for allocating a new slot
	 * @return Future which is completed with this execution once the slot has been assigned
	 * 			or with an exception if an error occurred.
	 * @throws IllegalExecutionStateException if this method has been called while not being in the CREATED state
	 */
	public CompletableFuture<Execution> allocateAndAssignSlotForExecution(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint,
			Time allocationTimeout) throws IllegalExecutionStateException {

		checkNotNull(slotProvider);

		final SlotSharingGroup sharingGroup = vertex.getJobVertex().getSlotSharingGroup();
		final CoLocationConstraint locationConstraint = vertex.getLocationConstraint();

		// sanity check
		if (locationConstraint != null && sharingGroup == null) {
			throw new IllegalStateException(
					"Trying to schedule with co-location constraint but without slot sharing allowed.");
		}

		// this method only works if the execution is in the state 'CREATED'
		if (transitionState(CREATED, SCHEDULED)) {

			final SlotSharingGroupId slotSharingGroupId = sharingGroup != null ? sharingGroup.getSlotSharingGroupId() : null;

			ScheduledUnit toSchedule = locationConstraint == null ?
					new ScheduledUnit(this, slotSharingGroupId) :
					new ScheduledUnit(this, slotSharingGroupId, locationConstraint);

			// try to extract previous allocation ids, if applicable, so that we can reschedule to the same slot
			ExecutionVertex executionVertex = getVertex();
			AllocationID lastAllocation = executionVertex.getLatestPriorAllocation();

			Collection<AllocationID> previousAllocationIDs =
				lastAllocation != null ? Collections.singletonList(lastAllocation) : Collections.emptyList();

			// calculate the preferred locations
			final CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture =
				calculatePreferredLocations(locationPreferenceConstraint);

			final SlotRequestId slotRequestId = new SlotRequestId();

			final CompletableFuture<LogicalSlot> logicalSlotFuture = preferredLocationsFuture
				.thenCompose(
					(Collection<TaskManagerLocation> preferredLocations) ->
						slotProvider.allocateSlot(
							slotRequestId,
							toSchedule,
							queued,
							new SlotProfile(
								ResourceProfile.UNKNOWN,
								preferredLocations,
								previousAllocationIDs),
							allocationTimeout));

			// register call back to cancel slot request in case that the execution gets canceled
			releaseFuture.whenComplete(
				(Object ignored, Throwable throwable) -> {
					if (logicalSlotFuture.cancel(false)) {
						slotProvider.cancelSlotRequest(
							slotRequestId,
							slotSharingGroupId,
							new FlinkException("Execution " + this + " was released."));
					}
				});

			return logicalSlotFuture.thenApply(
				(LogicalSlot logicalSlot) -> {
					if (tryAssignResource(logicalSlot)) {
						return this;
					} else {
						// release the slot
						logicalSlot.releaseSlot(new FlinkException("Could not assign logical slot to execution " + this + '.'));

						throw new CompletionException(new FlinkException("Could not assign slot " + logicalSlot + " to execution " + this + " because it has already been assigned "));
					}
				});
		}
		else {
			// call race, already deployed, or already done
			throw new IllegalExecutionStateException(this, CREATED, state);
		}
	}

	/**
	 * Deploys the execution to the previously assigned resource.
	 *
	 * @throws JobException if the execution cannot be deployed to the assigned resource
	 */
	public void deploy() throws JobException {
		final LogicalSlot slot  = assignedResource;

		checkNotNull(slot, "In order to deploy the execution we first have to assign a resource via tryAssignResource.");

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
				throw new IllegalStateException("Cannot deploy task: Concurrent deployment call race.");
			}
		}
		else {
			// vertex may have been cancelled, or it was already scheduled
			throw new IllegalStateException("The vertex must be in CREATED or SCHEDULED state to be deployed. Found state " + previous);
		}

		if (this != slot.getPayload()) {
			throw new IllegalStateException(
				String.format("The execution %s has not been assigned to the assigned slot.", this));
		}

		try {

			// race double check, did we fail/cancel and do we need to release the slot?
			if (this.state != DEPLOYING) {
				slot.releaseSlot(new FlinkException("Actual state of execution " + this + " (" + state + ") does not match expected state DEPLOYING."));
				return;
			}

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Deploying %s (attempt #%d) to %s", vertex.getTaskNameWithSubtaskIndex(),
						attemptNumber, getAssignedResourceLocation().getHostname()));
			}

			final TaskDeploymentDescriptor deployment = vertex.createDeploymentDescriptor(
				attemptId,
				slot,
				taskRestore,
				attemptNumber);

			// null taskRestore to let it be GC'ed
			taskRestore = null;

			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			final CompletableFuture<Acknowledge> submitResultFuture = taskManagerGateway.submitTask(deployment, rpcTimeout);

			submitResultFuture.whenCompleteAsync(
				(ack, failure) -> {
					// only respond to the failure case
					if (failure != null) {
						if (failure instanceof TimeoutException) {
							String taskname = vertex.getTaskNameWithSubtaskIndex() + " (" + attemptId + ')';

							markFailed(new Exception(
								"Cannot deploy task " + taskname + " - TaskManager (" + getAssignedResourceLocation()
									+ ") not responding after a rpcTimeout of " + rpcTimeout, failure));
						} else {
							markFailed(failure);
						}
					}
				},
				executor);
		}
		catch (Throwable t) {
			markFailed(t);
			ExceptionUtils.rethrow(t);
		}
	}

	/**
	 * Sends stop RPC call.
	 */
	public void stop() {
		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			CompletableFuture<Acknowledge> stopResultFuture = FutureUtils.retry(
				() -> taskManagerGateway.stopTask(attemptId, rpcTimeout),
				NUM_STOP_CALL_TRIES,
				executor);

			stopResultFuture.exceptionally(
				failure -> {
					LOG.info("Stopping task was not successful.", failure);
					return null;
				});
		}
	}

	public void cancel() {
		// depending on the previous state, we go directly to cancelled (no cancel call necessary)
		// -- or to canceling (cancel call needs to be sent to the task manager)

		// because of several possibly previous states, we need to again loop until we make a
		// successful atomic state transition
		while (true) {

			ExecutionState current = this.state;

			if (current == CANCELING || current == CANCELED) {
				// already taken care of, no need to cancel again
				return;
			}

			// these two are the common cases where we need to send a cancel call
			else if (current == RUNNING || current == DEPLOYING) {
				// try to transition to canceling, if successful, send the cancel call
				if (transitionState(current, CANCELING)) {
					sendCancelRpcCall();
					return;
				}
				// else: fall through the loop
			}

			else if (current == FINISHED || current == FAILED) {
				// nothing to do any more. finished failed before it could be cancelled.
				// in any case, the task is removed from the TaskManager already
				sendFailIntermediateResultPartitionsRpcCall();

				return;
			}
			else if (current == CREATED || current == SCHEDULED) {
				// from here, we can directly switch to cancelled, because no task has been deployed
				if (transitionState(current, CANCELED)) {

					// we skip the canceling state. set the timestamp, for a consistent appearance
					markTimestamp(CANCELING, getStateTimestamp(CANCELED));

					// cancel the future in order to fail depending scheduling operations
					taskManagerLocationFuture.cancel(false);

					try {
						vertex.getExecutionGraph().deregisterExecution(this);

						releaseAssignedResource(new FlinkException("Execution " + this + " was cancelled."));
					}
					finally {
						vertex.executionCanceled(this);
					}
					return;
				}
				// else: fall through the loop
			}
			else {
				throw new IllegalStateException(current.name());
			}
		}
	}

	void scheduleOrUpdateConsumers(List<List<ExecutionEdge>> allConsumers) {
		final int numConsumers = allConsumers.size();

		if (numConsumers > 1) {
			fail(new IllegalStateException("Currently, only a single consumer group per partition is supported."));
		}
		else if (numConsumers == 0) {
			return;
		}

		for (ExecutionEdge edge : allConsumers.get(0)) {
			final ExecutionVertex consumerVertex = edge.getTarget();

			final Execution consumer = consumerVertex.getCurrentExecutionAttempt();
			final ExecutionState consumerState = consumer.getState();

			final IntermediateResultPartition partition = edge.getSource();

			// ----------------------------------------------------------------
			// Consumer is created => try to deploy and cache input channel
			// descriptors if there is a deployment race
			// ----------------------------------------------------------------
			if (consumerState == CREATED) {
				final Execution partitionExecution = partition.getProducer()
						.getCurrentExecutionAttempt();

				consumerVertex.cachePartitionInfo(PartialInputChannelDeploymentDescriptor.fromEdge(
						partition, partitionExecution));

				// When deploying a consuming task, its task deployment descriptor will contain all
				// deployment information available at the respective time. It is possible that some
				// of the partitions to be consumed have not been created yet. These are updated
				// runtime via the update messages.
				//
				// TODO The current approach may send many update messages even though the consuming
				// task has already been deployed with all necessary information. We have to check
				// whether this is a problem and fix it, if it is.
				CompletableFuture.supplyAsync(
					() -> {
						try {
							final ExecutionGraph executionGraph = consumerVertex.getExecutionGraph();
							consumerVertex.scheduleForExecution(
								executionGraph.getSlotProvider(),
								executionGraph.isQueuedSchedulingAllowed(),
								LocationPreferenceConstraint.ANY); // there must be at least one known location
						} catch (Throwable t) {
							consumerVertex.fail(new IllegalStateException("Could not schedule consumer " +
									"vertex " + consumerVertex, t));
						}

						return null;
					},
					executor);

				// double check to resolve race conditions
				if (consumerVertex.getExecutionState() == RUNNING) {
					consumerVertex.sendPartitionInfos();
				}
			}
			// ----------------------------------------------------------------
			// Consumer is running => send update message now
			// ----------------------------------------------------------------
			else {
				if (consumerState == RUNNING) {
					final LogicalSlot consumerSlot = consumer.getAssignedResource();

					if (consumerSlot == null) {
						// The consumer has been reset concurrently
						continue;
					}

					final TaskManagerLocation partitionTaskManagerLocation = partition.getProducer()
							.getCurrentAssignedResource().getTaskManagerLocation();
					final ResourceID partitionTaskManager = partitionTaskManagerLocation.getResourceID();

					final ResourceID consumerTaskManager = consumerSlot.getTaskManagerLocation().getResourceID();

					final ResultPartitionID partitionId = new ResultPartitionID(partition.getPartitionId(), attemptId);

					final ResultPartitionLocation partitionLocation;

					if (consumerTaskManager.equals(partitionTaskManager)) {
						// Consuming task is deployed to the same instance as the partition => local
						partitionLocation = ResultPartitionLocation.createLocal();
					}
					else {
						// Different instances => remote
						final ConnectionID connectionId = new ConnectionID(
								partitionTaskManagerLocation,
								partition.getIntermediateResult().getConnectionIndex());

						partitionLocation = ResultPartitionLocation.createRemote(connectionId);
					}

					final InputChannelDeploymentDescriptor descriptor = new InputChannelDeploymentDescriptor(
							partitionId, partitionLocation);

					consumer.sendUpdatePartitionInfoRpcCall(
						Collections.singleton(
							new PartitionInfo(
								partition.getIntermediateResult().getId(),
								descriptor)));
				}
				// ----------------------------------------------------------------
				// Consumer is scheduled or deploying => cache input channel
				// deployment descriptors and send update message later
				// ----------------------------------------------------------------
				else if (consumerState == SCHEDULED || consumerState == DEPLOYING) {
					final Execution partitionExecution = partition.getProducer()
							.getCurrentExecutionAttempt();

					consumerVertex.cachePartitionInfo(PartialInputChannelDeploymentDescriptor
							.fromEdge(partition, partitionExecution));

					// double check to resolve race conditions
					if (consumerVertex.getExecutionState() == RUNNING) {
						consumerVertex.sendPartitionInfos();
					}
				}
			}
		}
	}

	/**
	 * This method fails the vertex due to an external condition. The task will move to state FAILED.
	 * If the task was in state RUNNING or DEPLOYING before, it will send a cancel call to the TaskManager.
	 *
	 * @param t The exception that caused the task to fail.
	 */
	@Override
	public void fail(Throwable t) {
		processFail(t, false);
	}

	/**
	 * Request a stack trace sample from the task of this execution.
	 *
	 * @param sampleId of the stack trace sample
	 * @param numSamples the sample should contain
	 * @param delayBetweenSamples to wait
	 * @param maxStackTraceDepth of the samples
	 * @param timeout until the request times out
	 * @return Future stack trace sample response
	 */
	public CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStackTraceDepth,
			Time timeout) {

		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			return taskManagerGateway.requestStackTraceSample(
				attemptId,
				sampleId,
				numSamples,
				delayBetweenSamples,
				maxStackTraceDepth,
				timeout);
		} else {
			return FutureUtils.completedExceptionally(new Exception("The execution has no slot assigned."));
		}
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

			taskManagerGateway.notifyCheckpointComplete(attemptId, getVertex().getJobId(), checkpointId, timestamp);
		} else {
			LOG.debug("The execution has no slot assigned. This indicates that the execution is " +
				"no longer running.");
		}
	}

	/**
	 * Trigger a new checkpoint on the task of this execution.
	 *
	 * @param checkpointId of th checkpoint to trigger
	 * @param timestamp of the checkpoint to trigger
	 * @param checkpointOptions of the checkpoint to trigger
	 */
	public void triggerCheckpoint(long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			taskManagerGateway.triggerCheckpoint(attemptId, getVertex().getJobId(), checkpointId, timestamp, checkpointOptions);
		} else {
			LOG.debug("The execution has no slot assigned. This indicates that the execution is " +
				"no longer running.");
		}
	}

	// --------------------------------------------------------------------------------------------
	//   Callbacks
	// --------------------------------------------------------------------------------------------

	/**
	 * This method marks the task as failed, but will make no attempt to remove task execution from the task manager.
	 * It is intended for cases where the task is known not to be running, or then the TaskManager reports failure
	 * (in which case it has already removed the task).
	 *
	 * @param t The exception that caused the task to fail.
	 */
	void markFailed(Throwable t) {
		processFail(t, true);
	}

	void markFailed(Throwable t, Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {
		processFail(t, true, userAccumulators, metrics);
	}

	void markFinished() {
		markFinished(null, null);
	}

	void markFinished(Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {

		// this call usually comes during RUNNING, but may also come while still in deploying (very fast tasks!)
		while (true) {
			ExecutionState current = this.state;

			if (current == RUNNING || current == DEPLOYING) {

				if (transitionState(current, FINISHED)) {
					try {
						for (IntermediateResultPartition finishedPartition
								: getVertex().finishAllBlockingPartitions()) {

							IntermediateResultPartition[] allPartitions = finishedPartition
									.getIntermediateResult().getPartitions();

							for (IntermediateResultPartition partition : allPartitions) {
								scheduleOrUpdateConsumers(partition.getConsumers());
							}
						}

						updateAccumulatorsAndMetrics(userAccumulators, metrics);

						releaseAssignedResource(null);

						vertex.getExecutionGraph().deregisterExecution(this);
					}
					finally {
						vertex.executionFinished(this);
					}
					return;
				}
			}
			else if (current == CANCELING) {
				// we sent a cancel call, and the task manager finished before it arrived. We
				// will never get a CANCELED call back from the job manager
				cancelingComplete(userAccumulators, metrics);
				return;
			}
			else if (current == CANCELED || current == FAILED) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Task FINISHED, but concurrently went to state " + state);
				}
				return;
			}
			else {
				// this should not happen, we need to fail this
				markFailed(new Exception("Vertex received FINISHED message while being in state " + state));
				return;
			}
		}
	}

	void cancelingComplete() {
		cancelingComplete(null, null);
	}

	void cancelingComplete(Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {

		// the taskmanagers can themselves cancel tasks without an external trigger, if they find that the
		// network stack is canceled (for example by a failing / canceling receiver or sender
		// this is an artifact of the old network runtime, but for now we need to support task transitions
		// from running directly to canceled

		while (true) {
			ExecutionState current = this.state;

			if (current == CANCELED) {
				return;
			}
			else if (current == CANCELING || current == RUNNING || current == DEPLOYING) {

				updateAccumulatorsAndMetrics(userAccumulators, metrics);

				if (transitionState(current, CANCELED)) {
					try {
						releaseAssignedResource(new FlinkException("Execution " + this + " was cancelled."));

						vertex.getExecutionGraph().deregisterExecution(this);
					}
					finally {
						vertex.executionCanceled(this);
					}
					return;
				}

				// else fall through the loop
			}
			else {
				// failing in the meantime may happen and is no problem.
				// anything else is a serious problem !!!
				if (current != FAILED) {
					String message = String.format("Asynchronous race: Found %s in state %s after successful cancel call.", vertex.getTaskNameWithSubtaskIndex(), state);
					LOG.error(message);
					vertex.getExecutionGraph().failGlobal(new Exception(message));
				}
				return;
			}
		}
	}

	void cachePartitionInfo(PartialInputChannelDeploymentDescriptor partitionInfo) {
		partialInputChannelDeploymentDescriptors.add(partitionInfo);
	}

	void sendPartitionInfos() {
		// check if the ExecutionVertex has already been archived and thus cleared the
		// partial partition infos queue
		if (partialInputChannelDeploymentDescriptors != null && !partialInputChannelDeploymentDescriptors.isEmpty()) {

			PartialInputChannelDeploymentDescriptor partialInputChannelDeploymentDescriptor;

			List<PartitionInfo> partitionInfos = new ArrayList<>(partialInputChannelDeploymentDescriptors.size());

			while ((partialInputChannelDeploymentDescriptor = partialInputChannelDeploymentDescriptors.poll()) != null) {
				partitionInfos.add(
					new PartitionInfo(
						partialInputChannelDeploymentDescriptor.getResultId(),
						partialInputChannelDeploymentDescriptor.createInputChannelDeploymentDescriptor(this)));
			}

			sendUpdatePartitionInfoRpcCall(partitionInfos);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Internal Actions
	// --------------------------------------------------------------------------------------------

	private boolean processFail(Throwable t, boolean isCallback) {
		return processFail(t, isCallback, null, null);
	}

	private boolean processFail(Throwable t, boolean isCallback, Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {

		// damn, we failed. This means only that we keep our books and notify our parent JobExecutionVertex
		// the actual computation on the task manager is cleaned up by the TaskManager that noticed the failure

		// we may need to loop multiple times (in the presence of concurrent calls) in order to
		// atomically switch to failed
		while (true) {
			ExecutionState current = this.state;

			if (current == FAILED) {
				// already failed. It is enough to remember once that we failed (its sad enough)
				return false;
			}

			if (current == CANCELED || current == FINISHED) {
				// we are already aborting or are already aborted or we are already finished
				if (LOG.isDebugEnabled()) {
					LOG.debug("Ignoring transition of vertex {} to {} while being {}.", getVertexWithAttempt(), FAILED, current);
				}
				return false;
			}

			if (current == CANCELING) {
				cancelingComplete(userAccumulators, metrics);
				return false;
			}

			if (transitionState(current, FAILED, t)) {
				// success (in a manner of speaking)
				this.failureCause = t;

				updateAccumulatorsAndMetrics(userAccumulators, metrics);

				try {
					releaseAssignedResource(t);
					vertex.getExecutionGraph().deregisterExecution(this);
				}
				finally {
					vertex.executionFailed(this, t);
				}

				if (!isCallback && (current == RUNNING || current == DEPLOYING)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Sending out cancel request, to remove task execution from TaskManager.");
					}

					try {
						if (assignedResource != null) {
							sendCancelRpcCall();
						}
					} catch (Throwable tt) {
						// no reason this should ever happen, but log it to be safe
						LOG.error("Error triggering cancel call while marking task {} as failed.", getVertex().getTaskNameWithSubtaskIndex(), tt);
					}
				}

				// leave the loop
				return true;
			}
		}
	}

	boolean switchToRunning() {

		if (transitionState(DEPLOYING, RUNNING)) {
			sendPartitionInfos();
			return true;
		}
		else {
			// something happened while the call was in progress.
			// it can mean:
			//  - canceling, while deployment was in progress. state is now canceling, or canceled, if the response overtook
			//  - finishing (execution and finished call overtook the deployment answer, which is possible and happens for fast tasks)
			//  - failed (execution, failure, and failure message overtook the deployment answer)

			ExecutionState currentState = this.state;

			if (currentState == FINISHED || currentState == CANCELED) {
				// do nothing, the task was really fast (nice)
				// or it was canceled really fast
			}
			else if (currentState == CANCELING || currentState == FAILED) {
				if (LOG.isDebugEnabled()) {
					// this log statement is guarded because the 'getVertexWithAttempt()' method
					// performs string concatenations
					LOG.debug("Concurrent canceling/failing of {} while deployment was in progress.", getVertexWithAttempt());
				}
				sendCancelRpcCall();
			}
			else {
				String message = String.format("Concurrent unexpected state transition of task %s to %s while deployment was in progress.",
						getVertexWithAttempt(), currentState);

				if (LOG.isDebugEnabled()) {
					LOG.debug(message);
				}

				// undo the deployment
				sendCancelRpcCall();

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
	private void sendCancelRpcCall() {
		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			CompletableFuture<Acknowledge> cancelResultFuture = FutureUtils.retry(
				() -> taskManagerGateway.cancelTask(attemptId, rpcTimeout),
				NUM_CANCEL_CALL_TRIES,
				executor);

			cancelResultFuture.whenCompleteAsync(
				(ack, failure) -> {
					if (failure != null) {
						fail(new Exception("Task could not be canceled.", failure));
					}
				},
				executor);
		}
	}

	private void sendFailIntermediateResultPartitionsRpcCall() {
		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			// TODO For some tests this could be a problem when querying too early if all resources were released
			taskManagerGateway.failPartition(attemptId);
		}
	}

	/**
	 * Update the partition infos on the assigned resource.
	 *
	 * @param partitionInfos for the remote task
	 */
	private void sendUpdatePartitionInfoRpcCall(
			final Iterable<PartitionInfo> partitionInfos) {

		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
			final TaskManagerLocation taskManagerLocation = slot.getTaskManagerLocation();

			CompletableFuture<Acknowledge> updatePartitionsResultFuture = taskManagerGateway.updatePartitions(attemptId, partitionInfos, rpcTimeout);

			updatePartitionsResultFuture.whenCompleteAsync(
				(ack, failure) -> {
					// fail if there was a failure
					if (failure != null) {
						fail(new IllegalStateException("Update task on TaskManager " + taskManagerLocation +
							" failed due to:", failure));
					}
				}, executor);
		}
	}

	/**
	 * Releases the assigned resource and completes the release future
	 * once the assigned resource has been successfully released.
	 *
	 * @param cause for the resource release, null if none
	 */
	private void releaseAssignedResource(@Nullable Throwable cause) {
		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			slot.releaseSlot(cause).whenComplete(
				(Object ignored, Throwable throwable) -> {
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

	/**
	 * Calculates the preferred locations based on the location preference constraint.
	 *
	 * @param locationPreferenceConstraint constraint for the location preference
	 * @return Future containing the collection of preferred locations. This might not be completed if not all inputs
	 * 		have been a resource assigned.
	 */
	@VisibleForTesting
	public CompletableFuture<Collection<TaskManagerLocation>> calculatePreferredLocations(LocationPreferenceConstraint locationPreferenceConstraint) {
		final Collection<CompletableFuture<TaskManagerLocation>> preferredLocationFutures = getVertex().getPreferredLocations();
		final CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture;

		switch(locationPreferenceConstraint) {
			case ALL:
				preferredLocationsFuture = FutureUtils.combineAll(preferredLocationFutures);
				break;
			case ANY:
				final ArrayList<TaskManagerLocation> completedTaskManagerLocations = new ArrayList<>(preferredLocationFutures.size());

				for (CompletableFuture<TaskManagerLocation> preferredLocationFuture : preferredLocationFutures) {
					if (preferredLocationFuture.isDone() && !preferredLocationFuture.isCompletedExceptionally()) {
						final TaskManagerLocation taskManagerLocation = preferredLocationFuture.getNow(null);

						if (taskManagerLocation == null) {
							throw new FlinkRuntimeException("TaskManagerLocationFuture was completed with null. This indicates a programming bug.");
						}

						completedTaskManagerLocations.add(taskManagerLocation);
					}
				}

				preferredLocationsFuture = CompletableFuture.completedFuture(completedTaskManagerLocations);
				break;
			default:
				throw new RuntimeException("Unknown LocationPreferenceConstraint " + locationPreferenceConstraint + '.');
		}

		return preferredLocationsFuture;
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState) {
		return transitionState(currentState, targetState, null);
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState, Throwable error) {
		// sanity check
		if (currentState.isTerminal()) {
			throw new IllegalStateException("Cannot leave terminal state " + currentState + " to transition to " + targetState + '.');
		}

		if (STATE_UPDATER.compareAndSet(this, currentState, targetState)) {
			markTimestamp(targetState);

			if (error == null) {
				LOG.info("{} ({}) switched from {} to {}.", getVertex().getTaskNameWithSubtaskIndex(), getAttemptId(), currentState, targetState);
			} else {
				LOG.info("{} ({}) switched from {} to {}.", getVertex().getTaskNameWithSubtaskIndex(), getAttemptId(), currentState, targetState, error);
			}

			if (targetState.isTerminal()) {
				// complete the terminal state future
				terminalStateFuture.complete(targetState);
			}

			// make sure that the state transition completes normally.
			// potential errors (in listeners may not affect the main logic)
			try {
				vertex.notifyStateTransition(this, targetState, error);
			}
			catch (Throwable t) {
				LOG.error("Error while notifying execution graph of execution state transition.", t);
			}
			return true;
		} else {
			return false;
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
			userAccumulators == null ?
				null :
				userAccumulators.entrySet()
					.stream()
					.collect(Collectors.toMap(Map.Entry::getKey, entry -> OptionalFailure.of(entry.getValue())));
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

	private void updateAccumulatorsAndMetrics(Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {
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

		return String.format("Attempt #%d (%s) @ %s - [%s]", attemptNumber, vertex.getTaskNameWithSubtaskIndex(),
				(slot == null ? "(unassigned)" : slot), state);
	}

	@Override
	public ArchivedExecution archive() {
		return new ArchivedExecution(this);
	}
}
