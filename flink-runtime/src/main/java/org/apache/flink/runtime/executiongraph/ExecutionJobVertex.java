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

import org.apache.flink.api.common.io.StrictlyLocalAssignment;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.util.SerializableObject;
import org.slf4j.Logger;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionJobVertex implements Serializable {
	
	private static final long serialVersionUID = 42L;
	
	/** Use the same log for all ExecutionGraph classes */
	private static final Logger LOG = ExecutionGraph.LOG;
	
	private final SerializableObject stateMonitor = new SerializableObject();
	
	private final ExecutionGraph graph;
	
	private final AbstractJobVertex jobVertex;
	
	private final ExecutionVertex[] taskVertices;

	private IntermediateResult[] producedDataSets;
	
	private final List<IntermediateResult> inputs;
	
	private final int parallelism;
	
	private final boolean[] finishedSubtasks;
			
	private volatile int numSubtasksInFinalState;
	
	private final SlotSharingGroup slotSharingGroup;
	
	private final CoLocationGroup coLocationGroup;
	
	private final InputSplit[] inputSplits;
	
	private List<LocatableInputSplit>[] inputSplitsPerSubtask;
	
	private InputSplitAssigner splitAssigner;
	
	public ExecutionJobVertex(ExecutionGraph graph, AbstractJobVertex jobVertex,
							int defaultParallelism, FiniteDuration timeout) throws JobException {
		this(graph, jobVertex, defaultParallelism, timeout, System.currentTimeMillis());
	}
	
	public ExecutionJobVertex(ExecutionGraph graph, AbstractJobVertex jobVertex,
							int defaultParallelism, FiniteDuration timeout, long createTimestamp)
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
			final IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);

			this.producedDataSets[i] = new IntermediateResult(
					result.getId(), this, numTaskVertices, result.getResultType());
		}

		// create all task vertices
		for (int i = 0; i < numTaskVertices; i++) {
			ExecutionVertex vertex = new ExecutionVertex(this, i, this.producedDataSets, timeout, createTimestamp);
			this.taskVertices[i] = vertex;
		}
		
		// sanity check for the double referencing between intermediate result partitions and execution vertices
		for (IntermediateResult ir : this.producedDataSets) {
			if (ir.getNumberOfAssignedPartitions() != parallelism) {
				throw new RuntimeException("The intermediate result's partitions were not correctly assigned.");
			}
		}
		
		// set up the input splits, if the vertex has any
		try {
			@SuppressWarnings("unchecked")
			InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();
			
			if (splitSource != null) {
				inputSplits = splitSource.createInputSplits(numTaskVertices);
				
				if (inputSplits != null) {
					if (splitSource instanceof StrictlyLocalAssignment) {
						inputSplitsPerSubtask = computeLocalInputSplitsPerTask(inputSplits);
						splitAssigner = new PredeterminedInputSplitAssigner(inputSplitsPerSubtask);
					} else {
						splitAssigner = splitSource.getInputSplitAssigner(inputSplits);
					}
				}
			}
			else {
				inputSplits = null;
			}
		}
		catch (Throwable t) {
			throw new JobException("Creating the input splits caused an error: " + t.getMessage(), t);
		}
		
		finishedSubtasks = new boolean[parallelism];
	}

	public ExecutionGraph getGraph() {
		return graph;
	}
	
	public AbstractJobVertex getJobVertex() {
		return jobVertex;
	}

	public int getParallelism() {
		return parallelism;
	}
	
	public JobID getJobId() {
		return graph.getJobID();
	}
	
	public JobVertexID getJobVertexId() {
		return jobVertex.getID();
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
		
		ExecutionVertex[] vertices = this.taskVertices;
		
		// check if we need to do pre-assignment of tasks
		if (inputSplitsPerSubtask != null) {
		
			final Map<String, List<Instance>> instances = scheduler.getInstancesByHost();
			final Map<String, Integer> assignments = new HashMap<String, Integer>();
			
			for (int i = 0; i < vertices.length; i++) {
				List<LocatableInputSplit> splitsForHost = inputSplitsPerSubtask[i];
				if (splitsForHost == null || splitsForHost.isEmpty()) {
					continue;
				}
				
				String[] hostNames = splitsForHost.get(0).getHostnames();
				if (hostNames == null || hostNames.length == 0 || hostNames[0] == null) {
					continue;
				}
				
				String host = hostNames[0];
				ExecutionVertex v = vertices[i];
				
				List<Instance> instancesOnHost = instances.get(host);
				
				if (instancesOnHost == null || instancesOnHost.isEmpty()) {
					throw new NoResourceAvailableException("Cannot schedule a strictly local task to host " + host
							+ ". No TaskManager available on that host.");
				}
				
				Integer pos = assignments.get(host);
				if (pos == null) {
					pos = 0;
					assignments.put(host, 0);
				} else {
					assignments.put(host, pos + 1 % instancesOnHost.size());
				}
				
				v.setLocationConstraintHosts(Collections.singletonList(instancesOnHost.get(pos)));
			}
		}
		
		// kick off the tasks
		for (ExecutionVertex ev : vertices) {
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
					
					if (inputSplitsPerSubtask == null) {
						// lazy assignment
						@SuppressWarnings("unchecked")
						InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();
						this.splitAssigner = splitSource.getInputSplitAssigner(this.inputSplits);
					}
					else {
						// eager assignment
						//TODO: this.splitAssigner = new AssignBasedOnPreAssignment();
					}
				}
			}
			catch (Throwable t) {
				throw new RuntimeException("Re-creating the input split assigner failed: " + t.getMessage(), t);
			}

			// Reset intermediate results
			for (IntermediateResult result : producedDataSets) {
				result.resetForNewExecution();
			}
		}
	}
	
	/**
	 * This method cleans fields that are irrelevant for the archived execution attempt.
	 */
	public void prepareForArchiving() {
		
		for (ExecutionVertex vertex : taskVertices) {
			vertex.prepareForArchiving();
		}
		
		// clear intermediate results
		inputs.clear();
		producedDataSets = null;
		
		// reset shared groups
		if (slotSharingGroup != null) {
			slotSharingGroup.clearTaskAssignment();
		}
		if (coLocationGroup != null) {
			coLocationGroup.resetConstraints();
		}
		
		// reset splits and split assigner
		splitAssigner = null;
		if (inputSplits != null) {
			for (int i = 0; i < inputSplits.length; i++) {
				inputSplits[i] = null;
			}
		}
		inputSplitsPerSubtask = null;
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
				} else {
					numSubtasksInFinalState++;
				}
			}
		}
	}

	private List<LocatableInputSplit>[] computeLocalInputSplitsPerTask(InputSplit[] splits) throws JobException {
		
		final int numSubTasks = getParallelism();
		
		// sanity check
		if (numSubTasks > splits.length) {
			throw new JobException("Strictly local assignment requires at least as many splits as subtasks.");
		}
		
		// group the splits by host while preserving order per host
		Map<String, List<LocatableInputSplit>> splitsByHost = new HashMap<String, List<LocatableInputSplit>>();
		
		for (InputSplit split : splits) {
			// check that split has exactly one local host
			if(!(split instanceof LocatableInputSplit)) {
				new JobException("Invalid InputSplit type " + split.getClass().getCanonicalName() + ". " +
						"Strictly local assignment requires LocatableInputSplit");
			}
			LocatableInputSplit lis = (LocatableInputSplit) split;

			if (lis.getHostnames() == null) {
				throw new JobException("LocatableInputSplit has no host information. " +
						"Strictly local assignment requires exactly one hostname for each LocatableInputSplit.");
			}
			else if (lis.getHostnames().length != 1) {
				throw new JobException("Strictly local assignment requires exactly one hostname for each LocatableInputSplit.");
			}
			String hostName = lis.getHostnames()[0];
			
			if (hostName == null) {
				throw new JobException("For strictly local input split assignment, no null host names are allowed.");
			}

			List<LocatableInputSplit> hostSplits = splitsByHost.get(hostName);
			if (hostSplits == null) {
				hostSplits = new ArrayList<LocatableInputSplit>();
				splitsByHost.put(hostName, hostSplits);
			}
			hostSplits.add(lis);
		}
		
		
		int numHosts = splitsByHost.size();
		
		if (numSubTasks < numHosts) {
			throw new JobException("Strictly local split assignment requires at least as " +
					"many parallel subtasks as distinct split hosts. Please increase the parallelism " +
					"of DataSource "+this.getJobVertex().getName()+" to at least "+numHosts+".");
		}

		// get list of hosts in deterministic order
		List<String> hosts = new ArrayList<String>(splitsByHost.keySet());
		Collections.sort(hosts);
		
		@SuppressWarnings("unchecked")
		List<LocatableInputSplit>[] subTaskSplitAssignment = (List<LocatableInputSplit>[]) new List<?>[numSubTasks];
		
		final int subtasksPerHost = numSubTasks / numHosts;
		final int hostsWithOneMore = numSubTasks % numHosts;
		
		int subtaskNum = 0;
		
		// we go over all hosts and distribute the hosts' input splits
		// over the subtasks
		for (int hostNum = 0; hostNum < numHosts; hostNum++) {
			String host = hosts.get(hostNum);
			List<LocatableInputSplit> splitsOnHost = splitsByHost.get(host);
			
			int numSplitsOnHost = splitsOnHost.size();
			
			// the number of subtasks to split this over.
			// NOTE: if the host has few splits, some subtasks will not get anything.
			int subtasks = Math.min(numSplitsOnHost, 
							hostNum < hostsWithOneMore ? subtasksPerHost + 1 : subtasksPerHost);
			
			int splitsPerSubtask = numSplitsOnHost / subtasks;
			int subtasksWithOneMore = numSplitsOnHost % subtasks;
			
			int splitnum = 0;
			
			// go over the subtasks and grab a subrange of the input splits
			for (int i = 0; i < subtasks; i++) {
				int numSplitsForSubtask = (i < subtasksWithOneMore ? splitsPerSubtask + 1 : splitsPerSubtask);
				
				List<LocatableInputSplit> splitList;
				
				if (numSplitsForSubtask == numSplitsOnHost) {
					splitList = splitsOnHost;
				}
				else {
					splitList = new ArrayList<LocatableInputSplit>(numSplitsForSubtask);
					for (int k = 0; k < numSplitsForSubtask; k++) {
						splitList.add(splitsOnHost.get(splitnum++));
					}
				}
				
				subTaskSplitAssignment[subtaskNum++] = splitList;
			}
		}
		
		return subTaskSplitAssignment;
	}
	
	//---------------------------------------------------------------------------------------------
	//  Predetermined InputSplitAssigner
	//---------------------------------------------------------------------------------------------

	public static class PredeterminedInputSplitAssigner implements InputSplitAssigner {

		private List<LocatableInputSplit>[] inputSplitsPerSubtask;

		@SuppressWarnings("unchecked")
		public PredeterminedInputSplitAssigner(List<LocatableInputSplit>[] inputSplitsPerSubtask) {
			// copy input split assignment
			this.inputSplitsPerSubtask = (List<LocatableInputSplit>[]) new List[inputSplitsPerSubtask.length];
			for (int i = 0; i < inputSplitsPerSubtask.length; i++) {
				List<LocatableInputSplit> next = inputSplitsPerSubtask[i];
				
				this.inputSplitsPerSubtask[i] = next == null || next.isEmpty() ?
						Collections.<LocatableInputSplit>emptyList() : 
						new ArrayList<LocatableInputSplit>(inputSplitsPerSubtask[i]);
			}
		}

		@Override
		public InputSplit getNextInputSplit(String host, int taskId) {
			if (inputSplitsPerSubtask[taskId].isEmpty()) {
				return null;
			} else {
				return inputSplitsPerSubtask[taskId].remove(inputSplitsPerSubtask[taskId].size() - 1);
			}
		}
	}
}
