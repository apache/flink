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

import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.flink.runtime.blob.BlobKey;
import org.slf4j.Logger;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.deployment.GateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;

/**
 * The ExecutionVertex is a parallel subtask of the execution. It may be executed once, or several times, each of
 * which time it spawns an {@link Execution}.
 */
public class ExecutionVertex {

	@SuppressWarnings("unused")
	private static final Logger LOG = ExecutionGraph.LOG;
	
	private static final int MAX_DISTINCT_LOCATIONS_TO_CONSIDER = 8;
	
	// --------------------------------------------------------------------------------------------
	
	private final ExecutionJobVertex jobVertex;
	
	private final IntermediateResultPartition[] resultPartitions;
	
	private final ExecutionEdge[][] inputEdges;
	
	private final int subTaskIndex;
	
	private final List<Execution> priorExecutions;
	
	private final CoLocationConstraint locationConstraint;
	
	private volatile Execution currentExecution;	// this field must never be null
	
	// --------------------------------------------------------------------------------------------

	public ExecutionVertex(ExecutionJobVertex jobVertex, int subTaskIndex, IntermediateResult[] producedDataSets) {
		this(jobVertex, subTaskIndex, producedDataSets, System.currentTimeMillis());
	}
	
	public ExecutionVertex(ExecutionJobVertex jobVertex, int subTaskIndex, IntermediateResult[] producedDataSets, long createTimestamp) {
		this.jobVertex = jobVertex;
		this.subTaskIndex = subTaskIndex;
		
		this.resultPartitions = new IntermediateResultPartition[producedDataSets.length];
		for (int i = 0; i < producedDataSets.length; i++) {
			IntermediateResultPartition irp = new IntermediateResultPartition(producedDataSets[i], this, subTaskIndex);
			this.resultPartitions[i] = irp;
			producedDataSets[i].setPartition(subTaskIndex, irp);
		}
		
		this.inputEdges = new ExecutionEdge[jobVertex.getJobVertex().getInputs().size()][];
		this.priorExecutions = new CopyOnWriteArrayList<Execution>();
		
		this.currentExecution = new Execution(this, 0, createTimestamp);
		
		// create a co-location scheduling hint, if necessary
		CoLocationGroup clg = jobVertex.getCoLocationGroup();
		if (clg != null) {
			this.locationConstraint = clg.getLocationConstraint(subTaskIndex);
		} else {
			this.locationConstraint = null;
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	public JobID getJobId() {
		return this.jobVertex.getJobId();
	}
	
	public ExecutionJobVertex getJobVertex() {
		return jobVertex;
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
	
	public ExecutionEdge[] getInputEdges(int input) {
		if (input < 0 || input >= this.inputEdges.length) {
			throw new IllegalArgumentException(String.format("Input %d is out of range [0..%d)", input, this.inputEdges.length));
		}
		return inputEdges[input];
	}
	
	public CoLocationConstraint getLocationConstraint() {
		return locationConstraint;
	}
	
	public Execution getCurrentExecutionAttempt() {
		return currentExecution;
	}
	
	public ExecutionState getExecutionState() {
		return currentExecution.getState();
	}
	
	public long getStateTimestamp(ExecutionState state) {
		return currentExecution.getStateTimestamp(state);
	}
	
	public Throwable getFailureCause() {
		return currentExecution.getFailureCause();
	}
	
	public AllocatedSlot getCurrentAssignedResource() {
		return currentExecution.getAssignedResource();
	}
	
	public ExecutionGraph getExecutionGraph() {
		return this.jobVertex.getGraph();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Graph building
	// --------------------------------------------------------------------------------------------
	
	public void connectSource(int inputNumber, IntermediateResult source, JobEdge edge, int consumerNumber) {
		
		final DistributionPattern pattern = edge.getDistributionPattern();
		final IntermediateResultPartition[] sourcePartitions = source.getPartitions();
		
		ExecutionEdge[] edges = null;
		
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
		
		ExecutionGraph graph = getExecutionGraph();
		
		// add the consumers to the source
		// for now (until the receiver initiated handshake is in place), we need to register the 
		// edges as the execution graph
		for (ExecutionEdge ee : edges) {
			ee.getSource().addConsumer(ee, consumerNumber);
			graph.registerExecutionEdge(ee);
		}
	}
	
	private ExecutionEdge[] connectAllToAll(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		ExecutionEdge[] edges = new ExecutionEdge[sourcePartitions.length];
		
		for (int i = 0; i < sourcePartitions.length; i++) {
			IntermediateResultPartition irp = sourcePartitions[i];
			edges[i] = new ExecutionEdge(irp, this, inputNumber);
		}
		
		return edges;
	}
	
	private ExecutionEdge[] connectPointwise(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		final int numSources = sourcePartitions.length;
		final int parallelism = getTotalNumberOfParallelSubtasks();
		
		// simple case same number of sources as targets
		if (numSources == parallelism) {
			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[subTaskIndex], this, inputNumber) };
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
			
			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[sourcePartition], this, inputNumber) };
		}
		else {
			if (numSources % parallelism == 0) {
				// same number of targets per source
				int factor = numSources / parallelism;
				int startIndex = subTaskIndex * factor;
				
				ExecutionEdge[] edges = new ExecutionEdge[factor];
				for (int i = 0; i < factor; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[startIndex + i], this, inputNumber);
				}
				return edges;
			}
			else {
				float factor = ((float) numSources) / parallelism;
				
				int start = (int) (subTaskIndex * factor);
				int end = (subTaskIndex == getTotalNumberOfParallelSubtasks() - 1) ?
						sourcePartitions.length : 
						(int) ((subTaskIndex + 1) * factor);
				
				ExecutionEdge[] edges = new ExecutionEdge[end - start];
				for (int i = 0; i < edges.length; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[start + i], this, inputNumber);
				}
				
				return edges;
			}
		}
	}

	/**
	 * Gets the location preferences of this task, determined by the locations of the predecessors from which
	 * it receives input data.
	 * If there are more than MAX_DISTINCT_LOCATIONS_TO_CONSIDER different locations of source data, this
	 * method returns {@code null} to indicate no location preference.
	 * 
	 * @return The preferred locations for this vertex execution, or null, if there is no preference.
	 */
	public Iterable<Instance> getPreferredLocations() {
		HashSet<Instance> locations = new HashSet<Instance>();
		
		for (int i = 0; i < inputEdges.length; i++) {
			ExecutionEdge[] sources = inputEdges[i];
			if (sources != null) {
				for (int k = 0; k < sources.length; k++) {
					AllocatedSlot sourceSlot = sources[k].getSource().getProducer().getCurrentAssignedResource();
					if (sourceSlot != null) {
						locations.add(sourceSlot.getInstance());
						if (locations.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
							return null;
						}
					}
				}
			}
		}
		return locations;
	}
	
	// --------------------------------------------------------------------------------------------
	//   Actions
	// --------------------------------------------------------------------------------------------
	
	public void resetForNewExecution() {
		synchronized (priorExecutions) {
			Execution execution = currentExecution;
			ExecutionState state = execution.getState();
			
			if (state == FINISHED || state == CANCELED || state == FAILED) {
				priorExecutions.add(execution);
				currentExecution = new Execution(this, execution.getAttemptNumber()+1, System.currentTimeMillis());
			}
			else {
				throw new IllegalStateException("Cannot reset a vertex that is in state " + state);
			}
		}
	}
	
	public void scheduleForExecution(Scheduler scheduler, boolean queued) throws NoResourceAvailableException {
		this.currentExecution.scheduleForExecution(scheduler, queued);
	}
	
	public void deployToSlot(AllocatedSlot slot) throws JobException {
		this.currentExecution.deployToSlot(slot);
	}
	
	public void cancel() {
		this.currentExecution.cancel();
	}
	
	public void fail(Throwable t) {
		this.currentExecution.fail(t);
	}
	
	// --------------------------------------------------------------------------------------------
	//   Notifications from the Execution Attempt
	// --------------------------------------------------------------------------------------------
	
	void executionFinished() {
		jobVertex.vertexFinished(subTaskIndex);
	}
	
	void executionCanceled() {
		jobVertex.vertexCancelled(subTaskIndex);
	}
	
	void executionFailed(Throwable t) {
		jobVertex.vertexFailed(subTaskIndex, t);
	}
	
	// --------------------------------------------------------------------------------------------
	//   Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Simply forward this notification. This is for logs and event archivers.
	 * 
	 * @param executionId
	 * @param newState
	 * @param error
	 */
	void notifyStateTransition(ExecutionAttemptID executionId, ExecutionState newState, Throwable error) {
		getExecutionGraph().notifyExecutionChange(getJobvertexId(), subTaskIndex, executionId, newState, error);
	}
	
	TaskDeploymentDescriptor createDeploymentDescriptor(ExecutionAttemptID executionId, AllocatedSlot slot) {
		//  create the input gate deployment descriptors
		List<GateDeploymentDescriptor> inputGates = new ArrayList<GateDeploymentDescriptor>(inputEdges.length);
		for (ExecutionEdge[] channels : inputEdges) {
			inputGates.add(GateDeploymentDescriptor.fromEdges(channels));
		}
		
		// create the output gate deployment descriptors
		List<GateDeploymentDescriptor> outputGates = new ArrayList<GateDeploymentDescriptor>(resultPartitions.length);
		for (IntermediateResultPartition partition : resultPartitions) {
			for (List<ExecutionEdge> channels : partition.getConsumers()) {
				outputGates.add(GateDeploymentDescriptor.fromEdges(channels));
			}
		}
		
		List<BlobKey> jarFiles = getExecutionGraph().getRequiredJarFiles();
		
		return new TaskDeploymentDescriptor(getJobId(), getJobvertexId(), executionId, getTaskName(), 
				subTaskIndex, getTotalNumberOfParallelSubtasks(), 
				getExecutionGraph().getJobConfiguration(), jobVertex.getJobVertex().getConfiguration(),
				jobVertex.getJobVertex().getInvokableClassName(), outputGates, inputGates, jarFiles, slot.getSlotNumber());
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
		return getSimpleName();
	}
}
