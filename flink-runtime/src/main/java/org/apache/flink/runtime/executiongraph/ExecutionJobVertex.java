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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
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
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;


public class ExecutionJobVertex {
	
	/** Use the same log for all ExecutionGraph classes */
	private static final Log LOG = ExecutionGraph.LOG;
	
	
	private final ExecutionGraph graph;
	
	private final AbstractJobVertex jobVertex;
	
	private final ExecutionVertex2[] taskVertices;

	private final IntermediateResult[] producedDataSets;
	
	private final InputSplitAssigner splitAssigner;
	
	private final int parallelism;
	
	
	private final AtomicInteger numRunningTasks = new AtomicInteger(0);
	
	private final AtomicInteger numFinishedTasks = new AtomicInteger(0);
	
	private final AtomicInteger numCancelledOrFailedTasks = new AtomicInteger(0);
	
	
	private SlotSharingGroup slotSharingGroup;
	
	
	public ExecutionJobVertex(ExecutionGraph graph, AbstractJobVertex jobVertex, int defaultParallelism) throws JobException {
		if (graph == null || jobVertex == null) {
			throw new NullPointerException();
		}
		
		this.graph = graph;
		this.jobVertex = jobVertex;
		
		int vertexParallelism = jobVertex.getParallelism();
		int numTaskVertices = vertexParallelism > 0 ? vertexParallelism : defaultParallelism;
		
		this.parallelism = numTaskVertices;
		this.taskVertices = new ExecutionVertex2[numTaskVertices];
		
		// create the intermediate results
		this.producedDataSets = new IntermediateResult[jobVertex.getNumberOfProducedIntermediateDataSets()];
		for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
			IntermediateDataSet set = jobVertex.getProducedDataSets().get(i);
			this.producedDataSets[i] = new IntermediateResult(set.getId(), this, numTaskVertices);
		}
		
		// create all task vertices
		for (int i = 0; i < numTaskVertices; i++) {
			ExecutionVertex2 vertex = new ExecutionVertex2(this, i, this.producedDataSets);
			this.taskVertices[i] = vertex;
		}
		
		// take the sharing group
		this.slotSharingGroup = jobVertex.getSlotSharingGroup();
		
		// set up the input splits, if the vertex has any
		try {
			@SuppressWarnings("unchecked")
			InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();
			if (splitSource != null) {
				InputSplit[] splits = splitSource.createInputSplits(numTaskVertices);
				this.splitAssigner = splitSource.getInputSplitAssigner(splits);
			} else {
				this.splitAssigner = null;
			}
		}
		catch (Throwable t) {
			throw new JobException("Creating the input splits caused an error: " + t.getMessage(), t);
		}
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
	
	public ExecutionVertex2[] getTaskVertices() {
		return taskVertices;
	}
	
	public IntermediateResult[] getProducedDataSets() {
		return producedDataSets;
	}
	
	public InputSplitAssigner getSplitAssigner() {
		return splitAssigner;
	}
	
	public void setSlotSharingGroup(SlotSharingGroup slotSharingGroup) {
		this.slotSharingGroup = slotSharingGroup;
	}
	
	public SlotSharingGroup getSlotSharingGroup() {
		return slotSharingGroup;
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
			
			int consumerIndex = ires.registerConsumer();
			
			for (int i = 0; i < parallelism; i++) {
				ExecutionVertex2 ev = taskVertices[i];
				ev.connectSource(num, ires, edge, consumerIndex);
			}
			
			
		}
	}
	
	//---------------------------------------------------------------------------------------------
	//  State, deployment, and recovery logic 
	//---------------------------------------------------------------------------------------------
	
	void vertexSwitchedToRunning(int subtask) {
		this.numRunningTasks.incrementAndGet();
	}
	
	void vertexFailed(int subtask) {
		
	}
	
	void vertexCancelled(int subtask) {
		
	}
	
	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	public void execute(Runnable action) {
		this.graph.execute(action);
	}
	
}
