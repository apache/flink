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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.slf4j.Logger;


public class ExecutionJobVertex implements Serializable {
	static final long serialVersionUID = 42L;
	
	/** Use the same log for all ExecutionGraph classes */
	private static final Logger LOG = ExecutionGraph.LOG;
	
	private transient final Object stateMonitor = new Object();
	
	private final ExecutionGraph graph;
	
	private final AbstractJobVertex jobVertex;
	
	private final ExecutionVertex[] taskVertices;

	private transient final IntermediateResult[] producedDataSets;
	
	private transient final List<IntermediateResult> inputs;
	
	private final int parallelism;
	
	private final boolean[] finishedSubtasks;
			
	private volatile int numSubtasksInFinalState;
	
	private final SlotSharingGroup slotSharingGroup;
	
	private final CoLocationGroup coLocationGroup;
	
	private final InputSplit[] inputSplits;
	
	private transient InputSplitAssigner splitAssigner;
	
	
	public ExecutionJobVertex(ExecutionGraph graph, AbstractJobVertex jobVertex, int defaultParallelism) throws JobException {
		this(graph, jobVertex, defaultParallelism, System.currentTimeMillis());
	}
	
	public ExecutionJobVertex(ExecutionGraph graph, AbstractJobVertex jobVertex, int defaultParallelism, long createTimestamp)
			throws JobException
	{
		if (graph == null || jobVertex == null) {
			throw new NullPointerException();
		}
		
		this.graph = graph;
		this.jobVertex = jobVertex;
		
		int vertexParallelism = jobVertex.getParallelism();
		int numTaskVertices = vertexParallelism > 0 ? vertexParallelism : defaultParallelism;
		
		this.parallelism = numTaskVertices;
		this.taskVertices = new ExecutionVertex[numTaskVertices];
		
		this.inputs = new ArrayList<IntermediateResult>(jobVertex.getInputs().size());
		
		// take the sharing group
		this.slotSharingGroup = jobVertex.getSlotSharingGroup();
		this.coLocationGroup = jobVertex.getCoLocationGroup();
		
		// setup the coLocation group
		if (coLocationGroup != null && slotSharingGroup == null) {
			throw new JobException("Vertex uses a co-location constraint without using slot sharing");
		}
		
		// create the intermediate results
		this.producedDataSets = new IntermediateResult[jobVertex.getNumberOfProducedIntermediateDataSets()];
		for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
			IntermediateDataSet set = jobVertex.getProducedDataSets().get(i);
			this.producedDataSets[i] = new IntermediateResult(set.getId(), this, numTaskVertices);
		}
		
		// create all task vertices
		for (int i = 0; i < numTaskVertices; i++) {
			ExecutionVertex vertex = new ExecutionVertex(this, i, this.producedDataSets, createTimestamp);
			this.taskVertices[i] = vertex;
		}
		
		// sanity check for the double referencing between intermediate result partitions and execution vertices
		for (IntermediateResult ir : this.producedDataSets) {
			if (ir.getNumberOfAssignedPartitions() != parallelism) {
				throw new RuntimeException("The intermediate result's partitions were not correctly assiged.");
			}
		}
		
		// set up the input splits, if the vertex has any
		try {
			@SuppressWarnings("unchecked")
			InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();
			if (splitSource != null) {
				this.inputSplits = splitSource.createInputSplits(numTaskVertices);
				this.splitAssigner = splitSource.getInputSplitAssigner(this.inputSplits);
			} else {
				this.inputSplits = null;
				this.splitAssigner = null;
			}
		}
		catch (Throwable t) {
			throw new JobException("Creating the input splits caused an error: " + t.getMessage(), t);
		}
		
		this.finishedSubtasks = new boolean[parallelism];
	}

	public ExecutionGraph getGraph() {
		return this.graph;
	}
	
	public AbstractJobVertex getJobVertex() {
		return this.jobVertex;
	}

	public int getParallelism() {
		return this.parallelism;
	}
	
	public JobID getJobId() {
		return this.graph.getJobID();
	}
	
	public JobVertexID getJobVertexId() {
		return this.jobVertex.getID();
	}
	
	public ExecutionVertex[] getTaskVertices() {
		return taskVertices;
	}
	
	public IntermediateResult[] getProducedDataSets() {
		return producedDataSets;
	}
	
	public InputSplitAssigner getSplitAssigner() {
		return splitAssigner;
	}
	
	public SlotSharingGroup getSlotSharingGroup() {
		return slotSharingGroup;
	}
	
	public CoLocationGroup getCoLocationGroup() {
		return coLocationGroup;
	}
	
	public List<IntermediateResult> getInputs() {
		return inputs;
	}
	
	public boolean isInFinalState() {
		return numSubtasksInFinalState == parallelism;
	}
	
	//---------------------------------------------------------------------------------------------
	
	public void connectToPredecessors(Map<IntermediateDataSetID, IntermediateResult> intermediateDataSets) throws JobException {
		
		List<JobEdge> inputs = jobVertex.getInputs();
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Connecting ExecutionJobVertex %s (%s) to %d predecessors.", jobVertex.getID(), jobVertex.getName(), inputs.size()));
		}
		
		for (int num = 0; num < inputs.size(); num++) {
			JobEdge edge = inputs.get(num);
			
			if (LOG.isDebugEnabled()) {
				if (edge.getSource() == null) {
					LOG.debug(String.format("Connecting input %d of vertex %s (%s) to intermediate result referenced via ID %s.", 
							num, jobVertex.getID(), jobVertex.getName(), edge.getSourceId()));
				} else {
					LOG.debug(String.format("Connecting input %d of vertex %s (%s) to intermediate result referenced via predecessor %s (%s).",
							num, jobVertex.getID(), jobVertex.getName(), edge.getSource().getProducer().getID(), edge.getSource().getProducer().getName()));
				}
			}
			
			// fetch the intermediate result via ID. if it does not exist, then it either has not been created, or the order
			// in which this method is called for the job vertices is not a topological order
			IntermediateResult ires = intermediateDataSets.get(edge.getSourceId());
			if (ires == null) {
				throw new JobException("Cannot connect this job graph to the previous graph. No previous intermediate result found for ID "
						+ edge.getSourceId());
			}
			
			this.inputs.add(ires);
			
			int consumerIndex = ires.registerConsumer();
			
			for (int i = 0; i < parallelism; i++) {
				ExecutionVertex ev = taskVertices[i];
				ev.connectSource(num, ires, edge, consumerIndex);
			}
		}
	}
	
	//---------------------------------------------------------------------------------------------
	//  Actions
	//---------------------------------------------------------------------------------------------
	
	public void scheduleAll(Scheduler scheduler, boolean queued) throws NoResourceAvailableException {
		for (ExecutionVertex ev : getTaskVertices()) {
			ev.scheduleForExecution(scheduler, queued);
		}
	}

	public void cancel() {
		for (ExecutionVertex ev : getTaskVertices()) {
			ev.cancel();
		}
	}
	
	public void fail(Throwable t) {
		for (ExecutionVertex ev : getTaskVertices()) {
			ev.fail(t);
		}
	}
	
	public void waitForAllVerticesToReachFinishingState() throws InterruptedException {
		synchronized (stateMonitor) {
			while (numSubtasksInFinalState < parallelism) {
				stateMonitor.wait();
			}
		}
	}
	
	public void resetForNewExecution() {
		if (!(numSubtasksInFinalState == 0 || numSubtasksInFinalState == parallelism)) {
			throw new IllegalStateException("Cannot reset vertex that is not in final state");
		}
		
		synchronized (stateMonitor) {
			// check and reset the sharing groups with scheduler hints
			if (slotSharingGroup != null) {
				slotSharingGroup.clearTaskAssignment();
			}
			if (coLocationGroup != null) {
				coLocationGroup.resetConstraints();
			}
			
			// reset vertices one by one. if one reset fails, the "vertices in final state"
			// fields will be consistent to handle triggered cancel calls
			for (int i = 0; i < parallelism; i++) {
				taskVertices[i].resetForNewExecution();
				if (finishedSubtasks[i]) {
					finishedSubtasks[i] = false;
					numSubtasksInFinalState--;
				}
			}
			
			if (numSubtasksInFinalState != 0) {
				throw new RuntimeException("Bug: resetting the execution job vertex failed.");
			}
			
			// set up the input splits again
			try {
				if (this.inputSplits != null) {
					@SuppressWarnings("unchecked")
					InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();
					this.splitAssigner = splitSource.getInputSplitAssigner(this.inputSplits);
				}
			}
			catch (Throwable t) {
				throw new RuntimeException("Re-creating the input split assigner failed: " + t.getMessage(), t);
			}
		}
	}
	
	//---------------------------------------------------------------------------------------------
	//  Notifications
	//---------------------------------------------------------------------------------------------
	
	void vertexFinished(int subtask) {
		subtaskInFinalState(subtask);
	}
	
	void vertexCancelled(int subtask) {
		subtaskInFinalState(subtask);
	}
	
	void vertexFailed(int subtask, Throwable error) {
		subtaskInFinalState(subtask);
	}
	
	private void subtaskInFinalState(int subtask) {
		synchronized (stateMonitor) {
			if (!finishedSubtasks[subtask]) {
				finishedSubtasks[subtask] = true;
				
				if (numSubtasksInFinalState+1 == parallelism) {
					
					// call finalizeOnMaster hook
					try {
						getJobVertex().finalizeOnMaster(getGraph().getUserClassLoader());
					}
					catch (Throwable t) {
						getGraph().fail(t);
					}

					numSubtasksInFinalState++;
					
					// we are in our final state
					stateMonitor.notifyAll();
					
					// tell the graph
					graph.jobVertexInFinalState(this);
				}else{
					numSubtasksInFinalState++;
				}
			}
		}
	}
}
