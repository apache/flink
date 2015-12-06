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

import akka.dispatch.OnComplete;
import akka.dispatch.OnFailure;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartialInputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAllocationFuture;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAllocationFutureAction;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.runtime.messages.TaskMessages.TaskOperationResult;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;

import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static akka.dispatch.Futures.future;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELING;
import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.DEPLOYING;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.runtime.execution.ExecutionState.SCHEDULED;
import static org.apache.flink.runtime.messages.TaskMessages.CancelTask;
import static org.apache.flink.runtime.messages.TaskMessages.FailIntermediateResultPartitions;
import static org.apache.flink.runtime.messages.TaskMessages.SubmitTask;
import static org.apache.flink.runtime.messages.TaskMessages.UpdatePartitionInfo;
import static org.apache.flink.runtime.messages.TaskMessages.UpdateTaskSinglePartitionInfo;
import static org.apache.flink.runtime.messages.TaskMessages.createUpdateTaskMultiplePartitionInfos;

/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times (for recovery,
 * or other re-computation), this class tracks the state of a single execution of that vertex and the resources.
 * 
 * NOTE ABOUT THE DESIGN RATIONAL:
 * 
 * In several points of the code, we need to deal with possible concurrent state changes and actions.
 * For example, while the call to deploy a task (send it to the TaskManager) happens, the task gets cancelled.
 * 
 * We could lock the entire portion of the code (decision to deploy, deploy, set state to running) such that
 * it is guaranteed that any "cancel command" will only pick up after deployment is done and that the "cancel
 * command" call will never overtake the deploying call.
 * 
 * This blocks the threads big time, because the remote calls may take long. Depending of their locking behavior, it
 * may even result in distributed deadlocks (unless carefully avoided). We therefore use atomic state updates and
 * occasional double-checking to ensure that the state after a completed call is as expected, and trigger correcting
 * actions if it is not. Many actions are also idempotent (like canceling).
 */
public class Execution implements Serializable {

	private static final long serialVersionUID = 42L;

	private static final AtomicReferenceFieldUpdater<Execution, ExecutionState> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(Execution.class, ExecutionState.class, "state");
	
	private static final Logger LOG = ExecutionGraph.LOG;
	
	private static final int NUM_CANCEL_CALL_TRIES = 3;

	// --------------------------------------------------------------------------------------------

	private final ExecutionVertex vertex;

	private final ExecutionAttemptID attemptId;

	private final long[] stateTimestamps;

	private final int attemptNumber;

	private final FiniteDuration timeout;

	private ConcurrentLinkedQueue<PartialInputChannelDeploymentDescriptor> partialInputChannelDeploymentDescriptors;

	private volatile ExecutionState state = CREATED;
	
	private volatile SimpleSlot assignedResource;     // once assigned, never changes until the execution is archived
	
	private volatile Throwable failureCause;          // once assigned, never changes
	
	private volatile InstanceConnectionInfo assignedResourceLocation; // for the archived execution
	
	private SerializedValue<StateHandle<?>> operatorState;
	
	private long recoveryTimestamp;

	/** The execution context which is used to execute futures. */
	@SuppressWarnings("NonSerializableFieldInSerializableClass")
	private ExecutionContext executionContext;

	/* Lock for updating the accumulators atomically. */
	private final SerializableObject accumulatorLock = new SerializableObject();

	/* Continuously updated map of user-defined accumulators */
	private volatile Map<String, Accumulator<?, ?>> userAccumulators;

	/* Continuously updated map of internal accumulators */
	private volatile Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> flinkAccumulators;

	// --------------------------------------------------------------------------------------------
	
	public Execution(
			ExecutionContext executionContext,
			ExecutionVertex vertex,
			int attemptNumber,
			long startTimestamp,
			FiniteDuration timeout) {
		this.executionContext = checkNotNull(executionContext);

		this.vertex = checkNotNull(vertex);
		this.attemptId = new ExecutionAttemptID();
		
		this.attemptNumber = attemptNumber;

		this.stateTimestamps = new long[ExecutionState.values().length];
		markTimestamp(ExecutionState.CREATED, startTimestamp);

		this.timeout = timeout;

		this.partialInputChannelDeploymentDescriptors = new ConcurrentLinkedQueue<PartialInputChannelDeploymentDescriptor>();
	}
	
	// --------------------------------------------------------------------------------------------
	//   Properties
	// --------------------------------------------------------------------------------------------

	public ExecutionVertex getVertex() {
		return vertex;
	}

	public ExecutionAttemptID getAttemptId() {
		return attemptId;
	}

	public int getAttemptNumber() {
		return attemptNumber;
	}

	public ExecutionState getState() {
		return state;
	}

	public SimpleSlot getAssignedResource() {
		return assignedResource;
	}

	public InstanceConnectionInfo getAssignedResourceLocation() {
		return assignedResourceLocation;
	}
	
	public Throwable getFailureCause() {
		return failureCause;
	}
	
	public long[] getStateTimestamps() {
		return stateTimestamps;
	}
	
	public long getStateTimestamp(ExecutionState state) {
		return this.stateTimestamps[state.ordinal()];
	}
	
	public boolean isFinished() {
		return state == FINISHED || state == FAILED || state == CANCELED;
	}
	
	/**
	 * This method cleans fields that are irrelevant for the archived execution attempt.
	 */
	public void prepareForArchiving() {
		if (assignedResource != null && assignedResource.isAlive()) {
			throw new IllegalStateException("Cannot archive Execution while the assigned resource is still running.");
		}
		assignedResource = null;

		executionContext = null;

		partialInputChannelDeploymentDescriptors.clear();
		partialInputChannelDeploymentDescriptors = null;
	}
	
	public void setInitialState(SerializedValue<StateHandle<?>> initialState, long recoveryTimestamp) {
		if (state != ExecutionState.CREATED) {
			throw new IllegalArgumentException("Can only assign operator state when execution attempt is in CREATED");
		}
		this.operatorState = initialState;
		this.recoveryTimestamp = recoveryTimestamp;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------
	
	/**
	 * NOTE: This method only throws exceptions if it is in an illegal state to be scheduled, or if the tasks needs
	 *       to be scheduled immediately and no resource is available. If the task is accepted by the schedule, any
	 *       error sets the vertex state to failed and triggers the recovery logic.
	 * 
	 * @param scheduler The scheduler to use to schedule this execution attempt.
	 * @param queued Flag to indicate whether the scheduler may queue this task if it cannot
	 *               immediately deploy it.
	 * 
	 * @throws IllegalStateException Thrown, if the vertex is not in CREATED state, which is the only state that permits scheduling.
	 * @throws NoResourceAvailableException Thrown is no queued scheduling is allowed and no resources are currently available.
	 */
	public boolean scheduleForExecution(Scheduler scheduler, boolean queued) throws NoResourceAvailableException {
		if (scheduler == null) {
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
			if (queued) {
				SlotAllocationFuture future = scheduler.scheduleQueued(toSchedule);

				future.setFutureAction(new SlotAllocationFutureAction() {
					@Override
					public void slotAllocated(SimpleSlot slot) {
						try {
							deployToSlot(slot);
						}
						catch (Throwable t) {
							try {
								slot.releaseSlot();
							} finally {
								markFailed(t);
							}
						}
					}
				});
			}
			else {
				SimpleSlot slot = scheduler.scheduleImmediately(toSchedule);
				try {
					deployToSlot(slot);
				}
				catch (Throwable t) {
					try {
						slot.releaseSlot();
					} finally {
						markFailed(t);
					}
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
			this.assignedResourceLocation = slot.getInstance().getInstanceConnectionInfo();

			// race double check, did we fail/cancel and do we need to release the slot?
			if (this.state != DEPLOYING) {
				slot.releaseSlot();
				return;
			}
			
			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Deploying %s (attempt #%d) to %s", vertex.getSimpleName(),
						attemptNumber, slot.getInstance().getInstanceConnectionInfo().getHostname()));
			}

			final TaskDeploymentDescriptor deployment = vertex.createDeploymentDescriptor(attemptId, slot, operatorState, recoveryTimestamp, attemptNumber);
			
			// register this execution at the execution graph, to receive call backs
			vertex.getExecutionGraph().registerExecution(this);

			final Instance instance = slot.getInstance();
			final ActorGateway gateway = instance.getActorGateway();

			final Future<Object> deployAction = gateway.ask(new SubmitTask(deployment), timeout);

			deployAction.onComplete(new OnComplete<Object>(){

				@Override
				public void onComplete(Throwable failure, Object success) throws Throwable {
					if (failure != null) {
						if (failure instanceof TimeoutException) {
							String taskname = deployment.getTaskInfo().getTaskNameWithSubtasks() + " (" + attemptId + ')';
							
							markFailed(new Exception(
									"Cannot deploy task " + taskname + " - TaskManager (" + instance
											+ ") not responding after a timeout of " + timeout, failure));
						}
						else {
							markFailed(failure);
						}
					}
					else {
						if (!(success.equals(Messages.getAcknowledge()))) {
							markFailed(new Exception("Failed to deploy the task to slot " + slot +
									": Response was not of type Acknowledge"));
						}
					}
				}
			}, executionContext);
		}
		catch (Throwable t) {
			markFailed(t);
			ExceptionUtils.rethrow(t);
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
				// from here, we can directly switch to cancelled, because the no task has been deployed
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
				future(new Callable<Boolean>(){
					@Override
					public Boolean call() throws Exception {
						try {
							consumerVertex.scheduleForExecution(
									consumerVertex.getExecutionGraph().getScheduler(),
									consumerVertex.getExecutionGraph().isQueuedSchedulingAllowed());
						} catch (Throwable t) {
							fail(new IllegalStateException("Could not schedule consumer " +
									"vertex " + consumerVertex, t));
						}

						return true;
					}
				}, executionContext);

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

					final Instance consumerInstance = consumerSlot.getInstance();

					final ResultPartitionID partitionId = new ResultPartitionID(
							partition.getPartitionId(), attemptId);

					final Instance partitionInstance = partition.getProducer()
							.getCurrentAssignedResource().getInstance();

					final ResultPartitionLocation partitionLocation;

					if (consumerInstance.equals(partitionInstance)) {
						// Consuming task is deployed to the same instance as the partition => local
						partitionLocation = ResultPartitionLocation.createLocal();
					}
					else {
						// Different instances => remote
						final ConnectionID connectionId = new ConnectionID(
								partitionInstance.getInstanceConnectionInfo(),
								partition.getIntermediateResult().getConnectionIndex());

						partitionLocation = ResultPartitionLocation.createRemote(connectionId);
					}

					final InputChannelDeploymentDescriptor descriptor = new InputChannelDeploymentDescriptor(
							partitionId, partitionLocation);

					final UpdatePartitionInfo updateTaskMessage = new UpdateTaskSinglePartitionInfo(
						consumer.getAttemptId(),
						partition.getIntermediateResult().getId(),
						descriptor);

					sendUpdatePartitionInfoRpcCall(consumerSlot, updateTaskMessage);
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

	void markFinished(Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> flinkAccumulators, Map<String, Accumulator<?, ?>> userAccumulators) {

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
							this.flinkAccumulators = flinkAccumulators;
							this.userAccumulators = userAccumulators;
						}

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

			List<IntermediateDataSetID> resultIDs = new ArrayList<IntermediateDataSetID>();
			List<InputChannelDeploymentDescriptor> inputChannelDeploymentDescriptors = new ArrayList<InputChannelDeploymentDescriptor>();

			while ((partialInputChannelDeploymentDescriptor = partialInputChannelDeploymentDescriptors.poll()) != null) {
				resultIDs.add(partialInputChannelDeploymentDescriptor.getResultId());
				inputChannelDeploymentDescriptors.add(partialInputChannelDeploymentDescriptor.createInputChannelDeploymentDescriptor(this));
			}

			UpdatePartitionInfo updateTaskMessage = createUpdateTaskMultiplePartitionInfos(
				attemptId,
				resultIDs,
				inputChannelDeploymentDescriptors);

			sendUpdatePartitionInfoRpcCall(assignedResource, updateTaskMessage);
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

			if (current == CANCELED) {
				// we are already aborting or are already aborted
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Ignoring transition of vertex %s to %s while being %s", 
							getVertexWithAttempt(), FAILED, CANCELED));
				}
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
		final SimpleSlot slot = this.assignedResource;

		if (slot != null) {

			final ActorGateway gateway = slot.getInstance().getActorGateway();

			Future<Object> cancelResult = gateway.retry(
				new CancelTask(attemptId),
				NUM_CANCEL_CALL_TRIES,
				timeout,
				executionContext);

			cancelResult.onComplete(new OnComplete<Object>() {

				@Override
				public void onComplete(Throwable failure, Object success) throws Throwable {
					if (failure != null) {
						fail(new Exception("Task could not be canceled.", failure));
					} else {
						TaskOperationResult result = (TaskOperationResult) success;
						if (!result.success()) {
							LOG.debug("Cancel task call did not find task. Probably akka message call" +
									" race.");
						}
					}
				}
			}, executionContext);
		}
	}

	private void sendFailIntermediateResultPartitionsRpcCall() {
		final SimpleSlot slot = this.assignedResource;

		if (slot != null) {
			final Instance instance = slot.getInstance();

			if (instance.isAlive()) {
				final ActorGateway gateway = instance.getActorGateway();

				// TODO For some tests this could be a problem when querying too early if all resources were released
				gateway.tell(new FailIntermediateResultPartitions(attemptId));
			}
		}
	}

	/**
	 * Sends an UpdatePartitionInfo message to the instance of the consumerSlot.
	 *
	 * @param consumerSlot Slot to whose instance the message will be sent
	 * @param updatePartitionInfo UpdatePartitionInfo message
	 */
	private void sendUpdatePartitionInfoRpcCall(
			final SimpleSlot consumerSlot,
			final UpdatePartitionInfo updatePartitionInfo) {

		if (consumerSlot != null) {
			final Instance instance = consumerSlot.getInstance();
			final ActorGateway gateway = instance.getActorGateway();

			Future<Object> futureUpdate = gateway.ask(updatePartitionInfo, timeout);

			futureUpdate.onFailure(new OnFailure() {
				@Override
				public void onFailure(Throwable failure) throws Throwable {
					fail(new IllegalStateException("Update task on instance " + instance +
							" failed due to:", failure));
				}
			}, executionContext);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState) {
		return transitionState(currentState, targetState, null);
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState, Throwable error) {
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
	 * @param flinkAccumulators the flink internal accumulators
	 * @param userAccumulators the user accumulators
	 */
	public void setAccumulators(Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> flinkAccumulators,
								Map<String, Accumulator<?, ?>> userAccumulators) {
		synchronized (accumulatorLock) {
			if (!state.isTerminal()) {
				this.flinkAccumulators = flinkAccumulators;
				this.userAccumulators = userAccumulators;
			}
		}
	}
	
	public Map<String, Accumulator<?, ?>> getUserAccumulators() {
		return userAccumulators;
	}

	public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
		return StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);
	}

	public Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> getFlinkAccumulators() {
		return flinkAccumulators;
	}

	// ------------------------------------------------------------------------
	//  Standard utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("Attempt #%d (%s) @ %s - [%s]", attemptNumber, vertex.getSimpleName(),
				(assignedResource == null ? "(unassigned)" : assignedResource.toString()), state);
	}
}
