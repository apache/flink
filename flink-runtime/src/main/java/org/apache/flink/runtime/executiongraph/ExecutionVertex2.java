/**
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.commons.logging.Log;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState2;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAllocationFuture;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAllocationFutureAction;
import org.apache.flink.runtime.taskmanager.TaskOperationResult;
import org.apache.flink.util.StringUtils;

import static org.apache.flink.runtime.execution.ExecutionState2.*;

/**
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
public class ExecutionVertex2 {
	
	private static final AtomicReferenceFieldUpdater<ExecutionVertex2, ExecutionState2> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(ExecutionVertex2.class, ExecutionState2.class, "state");
	
	private static final AtomicReferenceFieldUpdater<ExecutionVertex2, AllocatedSlot> ASSIGNED_SLOT_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(ExecutionVertex2.class, AllocatedSlot.class, "assignedSlot");

	private static final Log LOG = ExecutionGraph.LOG;
	
	private static final int NUM_CANCEL_CALL_TRIES = 3;
	
	// --------------------------------------------------------------------------------------------
	
	private final ExecutionJobVertex jobVertex;
	
	private final IntermediateResultPartition[] resultPartitions;
	
	private final ExecutionEdge2[][] inputEdges;
	
	private final int subTaskIndex;
	
	
	private volatile ExecutionState2 state = CREATED;
	
	private volatile AllocatedSlot assignedSlot;
	
	private volatile Throwable failureCause;
	
	
	public ExecutionVertex2(ExecutionJobVertex jobVertex, int subTaskIndex, IntermediateResult[] producedDataSets) {
		this.jobVertex = jobVertex;
		this.subTaskIndex = subTaskIndex;
		
		this.resultPartitions = new IntermediateResultPartition[producedDataSets.length];
		for (int i = 0; i < producedDataSets.length; i++) {
			IntermediateResultPartition irp = new IntermediateResultPartition(producedDataSets[i], this, subTaskIndex);
			this.resultPartitions[i] = irp;
			producedDataSets[i].setPartition(subTaskIndex, irp);
		}
		
		this.inputEdges = new ExecutionEdge2[jobVertex.getJobVertex().getInputs().size()][];
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	public JobID getJobId() {
		return this.jobVertex.getJobId();
	}
	
	public JobVertexID getJobvertexId() {
		return this.jobVertex.getJobVertexId();
	}
	
	public String getTaskName() {
		return this.jobVertex.getJobVertex().getName();
	}
	
	public int getTotalNumberOfParallelSubtasks() {
		return this.jobVertex.getParallelism();
	}
	
	public int getParallelSubtaskIndex() {
		return this.subTaskIndex;
	}
	
	public int getNumberOfInputs() {
		return this.inputEdges.length;
	}
	
	public ExecutionEdge2[] getInputEdges(int input) {
		if (input < 0 || input >= this.inputEdges.length) {
			throw new IllegalArgumentException(String.format("Input %d is out of range [0..%d)", input, this.inputEdges.length));
		}
		return inputEdges[input];
	}
	
	public ExecutionState2 getExecutionState() {
		return state;
	}
	
	public Throwable getFailureCause() {
		return failureCause;
	}
	
	public AllocatedSlot getAssignedResource() {
		return assignedSlot;
	}
	
	private ExecutionGraph getExecutionGraph() {
		return this.jobVertex.getGraph();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Graph building
	// --------------------------------------------------------------------------------------------
	
	public void connectSource(int inputNumber, IntermediateResult source, JobEdge edge, int consumerNumber) {
		
		final DistributionPattern pattern = edge.getDistributionPattern();
		final IntermediateResultPartition[] sourcePartitions = source.getPartitions();
		
		ExecutionEdge2[] edges = null;
		
		switch (pattern) {
			case POINTWISE:
				edges = connectPointwise(sourcePartitions, inputNumber);
				break;
				
			case BIPARTITE: 
				edges = connectAllToAll(sourcePartitions, inputNumber);
				break;
				
			default:
				throw new RuntimeException("Unrecognized distribution pattern.");
		
		}
		
		this.inputEdges[inputNumber] = edges;
		
		// add the cousumers to the source
		for (ExecutionEdge2 ee : edges) {
			ee.getSource().addConsumer(ee, consumerNumber);
		}
	}
	
	private ExecutionEdge2[] connectAllToAll(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		ExecutionEdge2[] edges = new ExecutionEdge2[sourcePartitions.length];
		
		for (int i = 0; i < sourcePartitions.length; i++) {
			IntermediateResultPartition irp = sourcePartitions[i];
			edges[i] = new ExecutionEdge2(irp, this, inputNumber);
		}
		
		return edges;
	}
	
	private ExecutionEdge2[] connectPointwise(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		final int numSources = sourcePartitions.length;
		final int parallelism = getTotalNumberOfParallelSubtasks();
		
		// simple case same number of sources as targets
		if (numSources == parallelism) {
			return new ExecutionEdge2[] { new ExecutionEdge2(sourcePartitions[subTaskIndex], this, inputNumber) };
		}
		else if (numSources < parallelism) {
			
			int sourcePartition;
			
			// check if the pattern is regular or irregular
			// we use int arithmetics for regular, and floating point with rounding for irregular
			if (parallelism % numSources == 0) {
				// same number of targets per source
				int factor = parallelism / numSources;
				sourcePartition = subTaskIndex / factor;
			}
			else {
				// different number of targets per source
				float factor = ((float) parallelism) / numSources;
				sourcePartition = (int) (subTaskIndex / factor);
			}
			
			return new ExecutionEdge2[] { new ExecutionEdge2(sourcePartitions[sourcePartition], this, inputNumber) };
		}
		else {
			if (numSources % parallelism == 0) {
				// same number of targets per source
				int factor = numSources / parallelism;
				int startIndex = subTaskIndex * factor;
				
				ExecutionEdge2[] edges = new ExecutionEdge2[factor];
				for (int i = 0; i < factor; i++) {
					edges[i] = new ExecutionEdge2(sourcePartitions[startIndex + i], this, inputNumber);
				}
				return edges;
			}
			else {
				float factor = ((float) numSources) / parallelism;
				
				int start = (int) (subTaskIndex * factor);
				int end = (subTaskIndex == getTotalNumberOfParallelSubtasks() - 1) ?
						sourcePartitions.length : 
						(int) ((subTaskIndex + 1) * factor);
				
				ExecutionEdge2[] edges = new ExecutionEdge2[end - start];
				for (int i = 0; i < edges.length; i++) {
					edges[i] = new ExecutionEdge2(sourcePartitions[start + i], this, inputNumber);
				}
				
				return edges;
			}
		}
	}

	
	// --------------------------------------------------------------------------------------------
	//  Scheduling
	// --------------------------------------------------------------------------------------------
	
	/**
	 * NOTE: This method only throws exceptions if it is in an illegal state to be scheduled, or if the tasks needs
	 *       to be scheduled immediately and no resource is available. If the task is accepted by the schedule, any
	 *       error sets the vertex state to failed and triggers the recovery logic.
	 * 
	 * @param scheduler
	 * 
	 * @throws IllegalStateException Thrown, if the vertex is not in CREATED state, which is the only state that permits scheduling.
	 * @throws NoResourceAvailableException Thrown is no queued scheduling is allowed and no resources are currently available.
	 */
	public void scheduleForExecution(DefaultScheduler scheduler) throws NoResourceAvailableException {
		if (scheduler == null) {
			throw new NullPointerException();
		}
		
		if (STATE_UPDATER.compareAndSet(this, CREATED, SCHEDULED)) {
			
			getExecutionGraph().notifyExecutionChange(getJobvertexId(), subTaskIndex, SCHEDULED, null);
			
			ScheduledUnit toSchedule = new ScheduledUnit(this, jobVertex.getSlotSharingGroup());
		
			// IMPORTANT: To prevent leaks of cluster resources, we need to make sure that slots are returned
			//     in all cases where the deployment failed. we use many try {} finally {} clauses to assure that
			
			boolean queued = jobVertex.getGraph().isQueuedSchedulingAllowed();
			if (queued) {
				SlotAllocationFuture future = scheduler.scheduleQueued(toSchedule);
				
				future.setFutureAction(new SlotAllocationFutureAction() {
					@Override
					public void slotAllocated(AllocatedSlot slot) {
						try {
							deployToSlot(slot);
						}
						catch (Throwable t) {
							try {
								slot.releaseSlot();
							} finally {
								fail(t);
							}
						}
					}
				});
			}
			else {
				AllocatedSlot slot = scheduler.scheduleImmediately(toSchedule);
				try {
					deployToSlot(slot);
				}
				catch (Throwable t) {
					try {
						slot.releaseSlot();
					} finally {
						fail(t);
					}
				}
			}
		}
		else if (this.state == CANCELED) {
			// this can occur very rarely through heavy races. if the task was canceled, we do not
			// schedule it
			return;
		}
		else {
			throw new IllegalStateException("The vertex must be in CREATED state to be scheduled.");
		}
	}
	

	public void deployToSlot(final AllocatedSlot slot) throws JobException {
		// sanity checks
		if (slot == null) {
			throw new NullPointerException();
		}
		if (!slot.isAlive()) {
			throw new IllegalArgumentException("Cannot deploy to a slot that is not alive.");
		}
		
		// make sure exactly one deployment call happens from the correct state
		// note: the transition from CREATED to DEPLOYING is for testing purposes only
		ExecutionState2 previous = this.state;
		if (previous == SCHEDULED || previous == CREATED) {
			if (!STATE_UPDATER.compareAndSet(this, previous, DEPLOYING)) {
				// race condition, someone else beat us to the deploying call.
				// this should actually not happen and indicates a race somewhere else
				throw new IllegalStateException("Cannot deploy task: Concurrent deployment call race.");
			}
			
			getExecutionGraph().notifyExecutionChange(getJobvertexId(), subTaskIndex, DEPLOYING, null);
		}
		else {
			// vertex may have been cancelled, or it was already scheduled
			throw new IllegalStateException("The vertex must be in CREATED or SCHEDULED state to be deployed. Found state " + previous);
		}
		
		// good, we are allowed to deploy
		if (!slot.setExecutedVertex(this)) {
			throw new JobException("Could not assign the ExecutionVertex to the slot " + slot);
		}
		setAssignedSlot(slot);
		
		
		final TaskDeploymentDescriptor deployment = createDeploymentDescriptor();
		
		// we execute the actual deploy call in a concurrent action to prevent this call from blocking for long
		Runnable deployaction = new Runnable() {

			@Override
			public void run() {
				try {
					Instance instance = slot.getInstance();
					instance.checkLibraryAvailability(getJobId());
					
					TaskOperationResult result = instance.getTaskManagerProxy().submitTask(deployment);
					if (result.isSuccess()) {
						switchToRunning();
					}
					else {
						// deployment failed :(
						fail(new Exception("Failed to deploy the tast to slot " + slot + ": " + result.getDescription()));
					}
				}
				catch (Throwable t) {
					// some error occurred. fail the task
					fail(t);
				}
			}
		};
		
		execute(deployaction);
	}
	
	private void switchToRunning() {
		
		// transition state
		if (STATE_UPDATER.compareAndSet(ExecutionVertex2.this, DEPLOYING, RUNNING)) {
			
			getExecutionGraph().notifyExecutionChange(getJobvertexId(), subTaskIndex, RUNNING, null);
			
			this.jobVertex.vertexSwitchedToRunning(subTaskIndex);
		}
		else {
			// something happened while the call was in progress.
			// typically, that means canceling while deployment was in progress
			
			ExecutionState2 currentState = ExecutionVertex2.this.state;
			
			if (currentState == CANCELING) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Concurrent canceling of task %s while deployment was in progress.", ExecutionVertex2.this.toString()));
				}
				
				sendCancelRpcCall();
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Concurrent unexpected state transition of task %s while deployment was in progress.", ExecutionVertex2.this.toString()));
				}
				
				// undo the deployment
				sendCancelRpcCall();
				
				// record the failure
				fail(new Exception("Asynchronous state error. Execution Vertex switched to " + currentState + " while deployment was in progress."));
			}
		}
	}
	
	public void cancel() {
		// depending on the previous state, we go directly to cancelled (no cancel call necessary)
		// -- or to canceling (cancel call needs to be sent to the task manager)
		
		// because of several possibly previous states, we need to again loop until we make a
		// successful atomic state transition
		while (true) {
			
			ExecutionState2 current = this.state;
			
			if (current == CANCELING || current == CANCELED) {
				// already taken care of, no need to cancel again
				return;
			}
				
			// these two are the common cases where we need to send a cancel call
			else if (current == RUNNING || current == DEPLOYING) {
				// try to transition to canceling, if successful, send the cancel call
				if (STATE_UPDATER.compareAndSet(this, current, CANCELING)) {
					
					getExecutionGraph().notifyExecutionChange(getJobvertexId(), subTaskIndex, CANCELING, null);
					
					sendCancelRpcCall();
					return;
				}
				// else: fall through the loop
			}
			
			else if (current == FINISHED || current == FAILED) {
				// nothing to do any more. finished failed before it could be cancelled.
				// in any case, the task is removed from the TaskManager already
				return;
			}
			else if (current == CREATED || current == SCHEDULED) {
				// from here, we can directly switch to cancelled, because the no task has been deployed
				if (STATE_UPDATER.compareAndSet(this, current, CANCELED)) {
					
					getExecutionGraph().notifyExecutionChange(getJobvertexId(), subTaskIndex, CANCELED, null);
					
					return;
				}
				// else: fall through the loop
			}
			else {
				throw new IllegalStateException(current.name());
			}
		}
	}
	
	public void fail(Throwable t) {
		
		// damn, we failed. This means only that we keep our books and notify our parent JobExecutionVertex
		// the actual computation on the task manager is cleaned up by the taskmanager that noticed the failure
		
		// we may need to loop multiple times (in the presence of concurrent calls) in order to
		// atomically switch to failed 
		while (true) {
			ExecutionState2 current = this.state;
			
			if (current == FAILED) {
				// concurrently set to failed. It is enough to remember once that we failed (its sad enough)
				return;
			}
			
			if (current == CANCELED) {
				// we already aborting
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Ignoring transition of vertex %s to %s while being %s",
							getSimpleName(), FAILED, current));
				}
				return;
			}
			
			// we should be in DEPLOYING or RUNNING when a regular failure happens
			if (current != DEPLOYING && current != RUNNING && current != CANCELING) {
				// this should not happen. still, what else to do but to comply and go to the FAILED state
				// at least we should complain loudly to the log
				LOG.error(String.format("Vertex %s unexpectedly went from state %s to %s with error: %s",
						getSimpleName(), CREATED, FAILED, t.getMessage()), t);
			}
			
			if (STATE_UPDATER.compareAndSet(this, current, FAILED)) {
				// success (in a manner of speaking)
				this.failureCause = t;
				
				getExecutionGraph().notifyExecutionChange(getJobvertexId(), subTaskIndex, FAILED, StringUtils.stringifyException(t));
				
				// release the slot (concurrency safe)
				setAssignedSlot(null);
				
				this.jobVertex.vertexFailed(subTaskIndex);
				
				// leave the loop
				return;
			}
		}
	}
	
	private void sendCancelRpcCall() {
		// first of all, copy a reference to the stack. any concurrent change to the
		// field does not affect us now
		final AllocatedSlot slot = this.assignedSlot;
		if (slot == null) {
			throw new IllegalStateException("Cannot cancel when task was not running or deployed.");
		}
		
		Runnable cancelAction = new Runnable() {
			
			@Override
			public void run() {
				Throwable exception = null;
				
				for (int triesLeft = NUM_CANCEL_CALL_TRIES; triesLeft > 0; --triesLeft) {
					
					try {
						// send the call. it may be that the task is not really there (asynchronous / overtaking messages)
						// in which case it is fine (the deployer catches it)
						TaskOperationResult result = slot.getInstance().getTaskManagerProxy().cancelTask(getJobvertexId(), subTaskIndex);
						
						if (result.isSuccess()) {
							
							// make sure that we release the slot
							try {
								// found and canceled
								if (STATE_UPDATER.compareAndSet(ExecutionVertex2.this, CANCELING, CANCELED)) {
									// we completed the call. 
									// release the slot resource and let the parent know we have cancelled
									ExecutionVertex2.this.jobVertex.vertexCancelled(ExecutionVertex2.this.subTaskIndex);
								}
								else {
									ExecutionState2 foundState = ExecutionVertex2.this.state;
									// failing in the meantime may happen and is no problem
									if (foundState != FAILED) {
										// corner case? log at least
										LOG.error(String.format("Asynchronous race: Found state %s after successful cancel call.", foundState));
									}
									
								}
							} finally {
								slot.releaseSlot();
							}
						}
						else {
							// the task was not found, which may be when the task concurrently finishes or fails, or
							// when the cancel call overtakes the deployment call
							if (LOG.isDebugEnabled()) {
								LOG.debug("Cancel task call did not find task. Probably cause: Acceptable asynchronous race.");
							}
						}
						
						// in any case, we need not call multiple times, so we quit
						return;
					}
					catch (Throwable t) {
						if (exception == null) {
							exception = t;
						}
						LOG.error("Canceling vertex " + getSimpleName() + " failed (" + triesLeft + " tries left): " + t.getMessage() , t);
					}
				}
				
				// dang, utterly unsuccessful - the target node must be down, in which case the tasks are lost anyways
				fail(new Exception("Task could not be canceled.", exception));
			}
		};
		
		execute(cancelAction);
	}
	
	public Iterable<Instance> getPreferredLocations() {
		return null;
	}
	
	private void setAssignedSlot(AllocatedSlot slot) {
		
		while (true) {
			AllocatedSlot previous = this.assignedSlot;
			if (ASSIGNED_SLOT_UPDATER.compareAndSet(this, previous, slot)) {
				// successfully swapped
				// release the predecessor, if it was not null. this call is idempotent, so it does not matter if it is
				// called more than once
				try {
					if (previous != null) {
						previous.releaseSlot();
					}
				} catch (Throwable t) {
					LOG.debug("Error releasing slot " + slot, t);
				}
				return;
			}
		}
	}
	
	
	private TaskDeploymentDescriptor createDeploymentDescriptor() {
		//  create the input gate deployment descriptors
		List<GateDeploymentDescriptor> inputGates = new ArrayList<GateDeploymentDescriptor>(inputEdges.length);
		for (ExecutionEdge2[] channels : inputEdges) {
			inputGates.add(GateDeploymentDescriptor.fromEdges(channels));
		}
		
		// create the output gate deployment descriptors
		List<GateDeploymentDescriptor> outputGates = new ArrayList<GateDeploymentDescriptor>(resultPartitions.length);
		for (IntermediateResultPartition partition : resultPartitions) {
			for (List<ExecutionEdge2> channels : partition.getConsumers()) {
				outputGates.add(GateDeploymentDescriptor.fromEdges(channels));
			}
		}
		
		String[] jarFiles = getExecutionGraph().getUserCodeJarFiles();
		
		return new TaskDeploymentDescriptor(getJobId(), getJobvertexId(), getTaskName(), 
				subTaskIndex, getTotalNumberOfParallelSubtasks(), 
				getExecutionGraph().getJobConfiguration(), jobVertex.getJobVertex().getConfiguration(),
				jobVertex.getJobVertex().getInvokableClassName(), outputGates, inputGates, jarFiles);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	public void execute(Runnable action) {
		this.jobVertex.execute(action);
	}
	
	/**
	 * Creates a simple name representation in the style 'taskname (x/y)', where
	 * 'taskname' is the name as returned by {@link #getTaskName()}, 'x' is the parallel
	 * subtask index as returned by {@link #getParallelSubtaskIndex()}{@code + 1}, and 'y' is the total
	 * number of tasks, as returned by {@link #getTotalNumberOfParallelSubtasks()}.
	 * 
	 * @return A simple name representation.
	 */
	public String getSimpleName() {
		return getTaskName() + " (" + (getParallelSubtaskIndex()+1) + '/' + getTotalNumberOfParallelSubtasks() + ')';
	}
	
	@Override
	public String toString() {
		return getSimpleName() + " [" + state + ']';
	}
}
