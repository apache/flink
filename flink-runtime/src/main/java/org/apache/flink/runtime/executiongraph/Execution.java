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

import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartialInputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELING;
import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.DEPLOYING;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.runtime.execution.ExecutionState.SCHEDULED;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times (for recovery,
 * or other re-computation), this class tracks the state of a single execution of that vertex and the resources.
 * 
 * <p>NOTE ABOUT THE DESIGN RATIONAL:
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
public class Execution implements AccessExecution, Archiveable<ArchivedExecution> {

	private static final AtomicReferenceFieldUpdater<Execution, ExecutionState> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(Execution.class, ExecutionState.class, "state");

	private static final Logger LOG = ExecutionGraph.LOG;

	private static final int NUM_CANCEL_CALL_TRIES = 3;

	private static final int NUM_STOP_CALL_TRIES = 3;

	// --------------------------------------------------------------------------------------------

	private final ExecutionVertex vertex;

	private final ExecutionAttemptID attemptId;

	private final long[] stateTimestamps;

	private final int attemptNumber;

	private final Time timeout;

	private ConcurrentLinkedQueue<PartialInputChannelDeploymentDescriptor> partialInputChannelDeploymentDescriptors;

	private volatile ExecutionState state = CREATED;

	private volatile SimpleSlot assignedResource;     // once assigned, never changes until the execution is archived

	private volatile Throwable failureCause;          // once assigned, never changes

	private volatile TaskManagerLocation assignedResourceLocation; // for the archived execution

	private TaskStateHandles taskStateHandles;

	/** The executor which is used to execute futures. */
	private Executor executor;

	// ------------------------- Accumulators ---------------------------------
	
	/* Lock for updating the accumulators atomically. Prevents final accumulators to be overwritten
	* by partial accumulators on a late heartbeat*/
	private final Object accumulatorLock = new Object();

	/* Continuously updated map of user-defined accumulators */
	private volatile Map<String, Accumulator<?, ?>> userAccumulators;
	private IOMetrics ioMetrics;

	// --------------------------------------------------------------------------------------------
	
	public Execution(
			Executor executor,
			ExecutionVertex vertex,
			int attemptNumber,
			long startTimestamp,
			Time timeout) {
		this.executor = checkNotNull(executor);

		this.vertex = checkNotNull(vertex);
		this.attemptId = new ExecutionAttemptID();

		this.attemptNumber = attemptNumber;

		this.stateTimestamps = new long[ExecutionState.values().length];
		markTimestamp(ExecutionState.CREATED, startTimestamp);

		this.timeout = checkNotNull(timeout);

		this.partialInputChannelDeploymentDescriptors = new ConcurrentLinkedQueue<>();
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

	public SimpleSlot getAssignedResource() {
		return assignedResource;
	}

	@Override
	public TaskManagerLocation getAssignedResourceLocation() {
		return assignedResourceLocation;
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

	public TaskStateHandles getTaskStateHandles() {
		return taskStateHandles;
	}

	/**
	 * Sets the initial state for the execution. The serialized state is then shipped via the
	 * {@link TaskDeploymentDescriptor} to the TaskManagers.
	 *
	 * @param checkpointStateHandles all checkpointed operator state
	 */
	public void setInitialState(TaskStateHandles checkpointStateHandles) {

		if (state != ExecutionState.CREATED) {
			throw new IllegalArgumentException("Can only assign operator state when execution attempt is in CREATED");
		}

		this.taskStateHandles = checkpointStateHandles;
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	/**
	 * NOTE: This method only throws exceptions if it is in an illegal state to be scheduled, or if the tasks needs
	 *       to be scheduled immediately and no resource is available. If the task is accepted by the schedule, any
	 *       error sets the vertex state to failed and triggers the recovery logic.
	 * 
	 * @param slotProvider The slot provider to use to allocate slot for this execution attempt.
	 * @param queued Flag to indicate whether the scheduler may queue this task if it cannot
	 *               immediately deploy it.
	 * 
	 * @throws IllegalStateException Thrown, if the vertex is not in CREATED state, which is the only state that permits scheduling.
	 * @throws NoResourceAvailableException Thrown is no queued scheduling is allowed and no resources are currently available.
	 */
	public boolean scheduleForExecution(SlotProvider slotProvider, boolean queued) throws NoResourceAvailableException {
		if (slotProvider == null) {
			throw new IllegalArgumentException("Cannot send null Scheduler when scheduling execution.");
		}

		final SlotSharingGroup sharingGroup = vertex.getJobVertex().getSlotSharingGroup();
		final CoLocationConstraint locationConstraint = vertex.getLocationConstraint();

		// sanity check
		if (locationConstraint != null && sharingGroup == null) {
			throw new RuntimeException("Trying to schedule with co-location constraint but without slot sharing allowed.");
		}

		if (transitionState(CREATED, SCHEDULED)) {

			ScheduledUnit toSchedule = locationConstraint == null ?
				new ScheduledUnit(this, sharingGroup) :
				new ScheduledUnit(this, sharingGroup, locationConstraint);

			// IMPORTANT: To prevent leaks of cluster resources, we need to make sure that slots are returned
			//     in all cases where the deployment failed. we use many try {} finally {} clauses to assure that
			final Future<SimpleSlot> slotAllocationFuture = slotProvider.allocateSlot(toSchedule, queued);

			// IMPORTANT: We have to use the synchronous handle operation (direct executor) here so
			// that we directly deploy the tasks if the slot allocation future is completed. This is
			// necessary for immediate deployment.
			final Future<Void> deploymentFuture = slotAllocationFuture.handle(new BiFunction<SimpleSlot, Throwable, Void>() {
				@Override
				public Void apply(SimpleSlot simpleSlot, Throwable throwable) {
					if (simpleSlot != null) {
						try {
							deployToSlot(simpleSlot);
						} catch (Throwable t) {
							try {
								simpleSlot.releaseSlot();
							} finally {
								markFailed(t);
							}
						}
					}
					else {
						markFailed(throwable);
					}
					return null;
				}
			});

			// if tasks have to scheduled immediately check that the task has been deployed
			if (!queued) {
				if (!deploymentFuture.isDone()) {
					markFailed(new IllegalArgumentException("The slot allocation future has not been completed yet."));
				}
			}
			
			return true;
		}
		else {
			// call race, already deployed, or already done
			return false;
		}
	}

	public void deployToSlot(final SimpleSlot slot) throws JobException {
		// sanity checks
		if (slot == null) {
			throw new NullPointerException();
		}
		if (!slot.isAlive()) {
			throw new JobException("Target slot for deployment is not alive.");
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

		try {
			// good, we are allowed to deploy
			if (!slot.setExecutedVertex(this)) {
				throw new JobException("Could not assign the ExecutionVertex to the slot " + slot);
			}
			this.assignedResource = slot;
			this.assignedResourceLocation = slot.getTaskManagerLocation();

			// race double check, did we fail/cancel and do we need to release the slot?
			if (this.state != DEPLOYING) {
				slot.releaseSlot();
				return;
			}

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Deploying %s (attempt #%d) to %s", vertex.getSimpleName(),
						attemptNumber, assignedResourceLocation.getHostname()));
			}

			final TaskDeploymentDescriptor deployment = vertex.createDeploymentDescriptor(
				attemptId,
				slot,
				taskStateHandles,
				attemptNumber);

			// register this execution at the execution graph, to receive call backs
			vertex.getExecutionGraph().registerExecution(this);
			
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			final Future<Acknowledge> submitResultFuture = taskManagerGateway.submitTask(deployment, timeout);

			submitResultFuture.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
				@Override
				public Void apply(Throwable failure) {
					if (failure instanceof TimeoutException) {
						String taskname = deployment.getTaskInfo().getTaskNameWithSubtasks() + " (" + attemptId + ')';

						markFailed(new Exception(
							"Cannot deploy task " + taskname + " - TaskManager (" + assignedResourceLocation
								+ ") not responding after a timeout of " + timeout, failure));
					}
					else {
						markFailed(failure);
					}
					return null;
				}
			}, executor);
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
		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			Future<Acknowledge> stopResultFuture = FutureUtils.retry(
				new Callable<Future<Acknowledge>>() {

					@Override
					public Future<Acknowledge> call() throws Exception {
						return taskManagerGateway.stopTask(attemptId, timeout);
					}
				},
				NUM_STOP_CALL_TRIES,
				executor);

			stopResultFuture.exceptionally(new ApplyFunction<Throwable, Void>() {
				@Override
				public Void apply(Throwable failure) {
					LOG.info("Stopping task was not successful.", failure);
					return null;
				}
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

					try {
						vertex.getExecutionGraph().deregisterExecution(this);
						if (assignedResource != null) {
							assignedResource.releaseSlot();
						}
					}
					finally {
						vertex.executionCanceled();
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
				FlinkFuture.supplyAsync(new Callable<Void>(){
					@Override
					public Void call() throws Exception {
						try {
							consumerVertex.scheduleForExecution(
									consumerVertex.getExecutionGraph().getSlotProvider(),
									consumerVertex.getExecutionGraph().isQueuedSchedulingAllowed());
						} catch (Throwable t) {
							consumerVertex.fail(new IllegalStateException("Could not schedule consumer " +
									"vertex " + consumerVertex, t));
						}

						return null;
					}
				}, executor);

				// double check to resolve race conditions
				if(consumerVertex.getExecutionState() == RUNNING){
					consumerVertex.sendPartitionInfos();
				}
			}
			// ----------------------------------------------------------------
			// Consumer is running => send update message now
			// ----------------------------------------------------------------
			else {
				if (consumerState == RUNNING) {
					final SimpleSlot consumerSlot = consumer.getAssignedResource();

					if (consumerSlot == null) {
						// The consumer has been reset concurrently
						continue;
					}

					final TaskManagerLocation partitionTaskManagerLocation = partition.getProducer()
							.getCurrentAssignedResource().getTaskManagerLocation();
					final ResourceID partitionTaskManager = partitionTaskManagerLocation.getResourceID();
					
					final ResourceID consumerTaskManager = consumerSlot.getTaskManagerID();

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
	public void fail(Throwable t) {
		processFail(t, false);
	}

	/**
	 * Request a stack trace sample from the task of this execution.
	 *
	 * @param sampleId of the stack trace sample
	 * @param numSamples the sample should contain
	 * @param delayBetweenSamples to wait
	 * @param maxStrackTraceDepth of the samples
	 * @param timeout until the request times out
	 * @return Future stack trace sample response
	 */
	public Future<StackTraceSampleResponse> requestStackTraceSample(
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStrackTraceDepth,
			Time timeout) {

		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			return taskManagerGateway.requestStackTraceSample(
				attemptId,
				sampleId,
				numSamples,
				delayBetweenSamples,
				maxStrackTraceDepth,
				timeout);
		} else {
			return FlinkCompletableFuture.completedExceptionally(new Exception("The execution has no slot assigned."));
		}
	}

	/**
	 * Notify the task of this execution about a completed checkpoint.
	 *
	 * @param checkpointId of the completed checkpoint
	 * @param timestamp of the completed checkpoint
	 */
	public void notifyCheckpointComplete(long checkpointId, long timestamp) {
		final SimpleSlot slot = assignedResource;

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
	 */
	public void triggerCheckpoint(long checkpointId, long timestamp) {
		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			taskManagerGateway.triggerCheckpoint(attemptId, getVertex().getJobId(), checkpointId, timestamp);
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

						synchronized (accumulatorLock) {
							this.userAccumulators = userAccumulators;
						}
						this.ioMetrics = metrics;

						assignedResource.releaseSlot();
						vertex.getExecutionGraph().deregisterExecution(this);
					}
					finally {
						vertex.executionFinished();
					}
					return;
				}
			}
			else if (current == CANCELING) {
				// we sent a cancel call, and the task manager finished before it arrived. We
				// will never get a CANCELED call back from the job manager
				cancelingComplete();
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
				if (transitionState(current, CANCELED)) {
					try {
						assignedResource.releaseSlot();
						vertex.getExecutionGraph().deregisterExecution(this);
					}
					finally {
						vertex.executionCanceled();
					}
					return;
				}

				// else fall through the loop
			}
			else {
				// failing in the meantime may happen and is no problem.
				// anything else is a serious problem !!!
				if (current != FAILED) {
					String message = String.format("Asynchronous race: Found state %s after successful cancel call.", state);
					LOG.error(message);
					vertex.getExecutionGraph().fail(new Exception(message));
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
		if(partialInputChannelDeploymentDescriptors != null && !partialInputChannelDeploymentDescriptors.isEmpty()) {

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
				cancelingComplete();
				return false;
			}

			if (transitionState(current, FAILED, t)) {
				// success (in a manner of speaking)
				this.failureCause = t;

				try {
					if (assignedResource != null) {
						assignedResource.releaseSlot();
					}
					vertex.getExecutionGraph().deregisterExecution(this);
				}
				finally {
					vertex.executionFailed(t);
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
						LOG.error("Error triggering cancel call while marking task as failed.", tt);
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
					LOG.debug(String.format("Concurrent canceling/failing of %s while deployment was in progress.", getVertexWithAttempt()));
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
	 * The sending is tried up to NUM_CANCEL_CALL_TRIES times.
	 */
	private void sendCancelRpcCall() {
		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			Future<Acknowledge> cancelResultFuture = FutureUtils.retry(
				new Callable<Future<Acknowledge>>() {
					@Override
					public Future<Acknowledge> call() throws Exception {
						return taskManagerGateway.cancelTask(attemptId, timeout);
					}
				},
				NUM_CANCEL_CALL_TRIES,
				executor);

			cancelResultFuture.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
				@Override
				public Void apply(Throwable failure) {
					fail(new Exception("Task could not be canceled.", failure));
					return null;
				}
			}, executor);
		}
	}

	private void sendFailIntermediateResultPartitionsRpcCall() {
		final SimpleSlot slot = assignedResource;

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

		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
			final TaskManagerLocation taskManagerLocation = slot.getTaskManagerLocation();

			Future<Acknowledge> updatePartitionsResultFuture = taskManagerGateway.updatePartitions(attemptId, partitionInfos, timeout);

			updatePartitionsResultFuture.exceptionallyAsync(new ApplyFunction<Throwable, Void>() {
				@Override
				public Void apply(Throwable failure) {
					fail(new IllegalStateException("Update task on TaskManager " + taskManagerLocation +
						" failed due to:", failure));
					return null;
				}
			}, executor);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState) {
		return transitionState(currentState, targetState, null);
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState, Throwable error) {
		// sanity check
		if (currentState.isTerminal()) {
			throw new IllegalStateException("Cannot leave terminal state " + currentState + " to transition to " + targetState + ".");
		}

		if (STATE_UPDATER.compareAndSet(this, currentState, targetState)) {
			markTimestamp(targetState);

			LOG.info(getVertex().getTaskNameWithSubtaskIndex() + " ("  + getAttemptId() + ") switched from "
				+ currentState + " to " + targetState);

			// make sure that the state transition completes normally.
			// potential errors (in listeners may not affect the main logic)
			try {
				vertex.notifyStateTransition(attemptId, targetState, error);
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
		return vertex.getSimpleName() + " - execution #" + attemptNumber;
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
		return StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);
	}

	@Override
	public int getParallelSubtaskIndex() {
		return getVertex().getParallelSubtaskIndex();
	}
		
	@Override
	public IOMetrics getIOMetrics() {
		return ioMetrics;
	}

	// ------------------------------------------------------------------------
	//  Standard utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("Attempt #%d (%s) @ %s - [%s]", attemptNumber, vertex.getSimpleName(),
				(assignedResource == null ? "(unassigned)" : assignedResource.toString()), state);
	}

	@Override
	public ArchivedExecution archive() {
		return new ArchivedExecution(this);
	}
}
