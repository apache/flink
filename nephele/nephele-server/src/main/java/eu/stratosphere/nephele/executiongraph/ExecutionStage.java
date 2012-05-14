/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.executiongraph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.types.Record;

/**
 * An execution stage contains all execution group vertices (and as a result all execution vertices) which
 * must run at the same time. The execution of a job progresses in terms of stages, i.e. the next stage of a
 * job can only start to execute if the execution of its preceding stage is complete.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ExecutionStage {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionStage.class);

	/**
	 * The execution graph that this stage belongs to.
	 */
	private final ExecutionGraph executionGraph;

	/**
	 * List of group vertices which are assigned to this stage.
	 */
	private final CopyOnWriteArrayList<ExecutionGroupVertex> stageMembers = new CopyOnWriteArrayList<ExecutionGroupVertex>();

	/**
	 * Number of the stage.
	 */
	private volatile int stageNum = -1;

	/**
	 * Constructs a new execution stage and assigns the given stage number to it.
	 * 
	 * @param executionGraph
	 *        the executionGraph that this stage belongs to
	 * @param stageNum
	 *        the number of this execution stage
	 */
	public ExecutionStage(final ExecutionGraph executionGraph, final int stageNum) {
		this.executionGraph = executionGraph;
		this.stageNum = stageNum;
	}

	/**
	 * Sets the number of this execution stage.
	 * 
	 * @param stageNum
	 *        the new number of this execution stage
	 */
	public void setStageNumber(final int stageNum) {
		this.stageNum = stageNum;
	}

	/**
	 * Returns the number of this execution stage.
	 * 
	 * @return the number of this execution stage
	 */
	public int getStageNumber() {

		return this.stageNum;
	}

	/**
	 * Adds a new execution group vertex to this stage if it is not already included.
	 * 
	 * @param groupVertex
	 *        the new execution group vertex to include
	 */
	public void addStageMember(final ExecutionGroupVertex groupVertex) {

		if (this.stageMembers.addIfAbsent(groupVertex)) {
			groupVertex.setExecutionStage(this);
		}
	}

	/**
	 * Removes the specified group vertex from the execution stage.
	 * 
	 * @param groupVertex
	 *        the group vertex to remove from the stage
	 */
	public void removeStageMember(ExecutionGroupVertex groupVertex) {

		this.stageMembers.remove(groupVertex);
	}

	/**
	 * Returns the number of group vertices this execution stage includes.
	 * 
	 * @return the number of group vertices this execution stage includes
	 */
	public int getNumberOfStageMembers() {

		return this.stageMembers.size();
	}

	/**
	 * Returns the stage member internally stored at index <code>index</code>.
	 * 
	 * @param index
	 *        the index of the group vertex to return
	 * @return the stage member internally stored at the specified index or <code>null</code> if no group vertex exists
	 *         with such an index
	 */
	public ExecutionGroupVertex getStageMember(final int index) {

		try {
			return this.stageMembers.get(index);
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
	}

	/**
	 * Returns the number of input execution vertices in this stage, i.e. the number
	 * of execution vertices which are connected to vertices in a lower stage
	 * or have no input channels.
	 * 
	 * @return the number of input vertices in this stage
	 */
	public int getNumberOfInputExecutionVertices() {

		int retVal = 0;

		final Iterator<ExecutionGroupVertex> it = this.stageMembers.iterator();
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			if (groupVertex.isInputVertex()) {
				retVal += groupVertex.getCurrentNumberOfGroupMembers();
			}
		}

		return retVal;
	}

	/**
	 * Returns the number of output execution vertices in this stage, i.e. the number
	 * of execution vertices which are connected to vertices in a higher stage
	 * or have no output channels.
	 * 
	 * @return the number of output vertices in this stage
	 */
	public int getNumberOfOutputExecutionVertices() {

		int retVal = 0;

		final Iterator<ExecutionGroupVertex> it = this.stageMembers.iterator();
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			if (groupVertex.isOutputVertex()) {
				retVal += groupVertex.getCurrentNumberOfGroupMembers();
			}
		}

		return retVal;
	}

	/**
	 * Returns the output execution vertex with the given index or <code>null</code> if no such vertex exists.
	 * 
	 * @param index
	 *        the index of the vertex to be selected.
	 * @return the output execution vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public ExecutionVertex getInputExecutionVertex(int index) {

		final Iterator<ExecutionGroupVertex> it = this.stageMembers.iterator();
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			if (groupVertex.isInputVertex()) {
				final int numberOfMembers = groupVertex.getCurrentNumberOfGroupMembers();
				if (index >= numberOfMembers) {
					index -= numberOfMembers;
				} else {
					return groupVertex.getGroupMember(index);
				}
			}
		}

		return null;
	}

	/**
	 * Returns the input execution vertex with the given index or <code>null</code> if no such vertex exists.
	 * 
	 * @param index
	 *        the index of the vertex to be selected.
	 * @return the input execution vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public ExecutionVertex getOutputExecutionVertex(int index) {

		final Iterator<ExecutionGroupVertex> it = this.stageMembers.iterator();
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			if (groupVertex.isOutputVertex()) {
				final int numberOfMembers = groupVertex.getCurrentNumberOfGroupMembers();
				if (index >= numberOfMembers) {
					index -= numberOfMembers;
				} else {
					return groupVertex.getGroupMember(index);
				}
			}
		}

		return null;
	}

	/**
	 * Checks which instance types and how many instances of these types are required to execute this stage
	 * of the job graph. The required instance types and the number of instances are collected in the given map. Note
	 * that this method does not clear the map before collecting the instances.
	 * 
	 * @param instanceRequestMap
	 *        the map containing the instances types and the required number of instances of the respective type
	 * @param executionState
	 *        the execution state the considered vertices must be in
	 */
	public void collectRequiredInstanceTypes(final InstanceRequestMap instanceRequestMap,
			final ExecutionState executionState) {

		final Set<AbstractInstance> collectedInstances = new HashSet<AbstractInstance>();
		final ExecutionGroupVertexIterator groupIt = new ExecutionGroupVertexIterator(this.getExecutionGraph(), true,
			this.stageNum);

		while (groupIt.hasNext()) {

			final ExecutionGroupVertex groupVertex = groupIt.next();
			final Iterator<ExecutionVertex> vertexIt = groupVertex.iterator();
			while (vertexIt.hasNext()) {

				// Get the instance type from the execution vertex if it
				final ExecutionVertex vertex = vertexIt.next();
				if (vertex.getExecutionState() == executionState) {
					final AbstractInstance instance = vertex.getAllocatedResource().getInstance();

					if (collectedInstances.contains(instance)) {
						continue;
					} else {
						collectedInstances.add(instance);
					}

					if (instance instanceof DummyInstance) {

						final InstanceType instanceType = instance.getType();
						int num = instanceRequestMap.getMaximumNumberOfInstances(instanceType);
						++num;
						instanceRequestMap.setMaximumNumberOfInstances(instanceType, num);
						if (groupVertex.isInputVertex()) {
							num = instanceRequestMap.getMinimumNumberOfInstances(instanceType);
							++num;
							instanceRequestMap.setMinimumNumberOfInstances(instanceType, num);
						}
					} else {
						LOG.debug("Execution Vertex " + vertex.getName() + " (" + vertex.getID()
							+ ") is already assigned to non-dummy instance, skipping...");
					}
				}
			}
		}

		final Iterator<Map.Entry<InstanceType, Integer>> it = instanceRequestMap.getMaximumIterator();
		while (it.hasNext()) {

			final Map.Entry<InstanceType, Integer> entry = it.next();
			if (instanceRequestMap.getMinimumNumberOfInstances(entry.getKey()) == 0) {
				instanceRequestMap.setMinimumNumberOfInstances(entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * Returns the execution graph that this stage belongs to.
	 * 
	 * @return the execution graph that this stage belongs to
	 */
	public ExecutionGraph getExecutionGraph() {

		return this.executionGraph;
	}

	/**
	 * Reconstructs the execution pipelines for this execution stage.
	 */
	void reconstructExecutionPipelines() {

		Iterator<ExecutionGroupVertex> it = this.stageMembers.iterator();
		final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();

		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();

			// We only look at input vertices first
			if (!groupVertex.isInputVertex()) {
				continue;
			}

			final Iterator<ExecutionVertex> vertexIt = groupVertex.iterator();
			while (vertexIt.hasNext()) {

				final ExecutionVertex vertex = vertexIt.next();
				reconstructExecutionPipeline(vertex, true, alreadyVisited);
			}
		}

		it = this.stageMembers.iterator();
		alreadyVisited.clear();

		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();

			// We only look at input vertices first
			if (!groupVertex.isOutputVertex()) {
				continue;
			}

			final Iterator<ExecutionVertex> vertexIt = groupVertex.iterator();
			while (vertexIt.hasNext()) {

				final ExecutionVertex vertex = vertexIt.next();
				reconstructExecutionPipeline(vertex, false, alreadyVisited);
			}
		}
	}

	/**
	 * Reconstructs the execution pipeline starting at the given vertex by conducting a depth-first search.
	 * 
	 * @param vertex
	 *        the vertex to start the depth-first search from
	 * @param forward
	 *        <code>true</code> to traverse the graph according to the original direction of the edges or
	 *        <code>false</code> for the opposite direction
	 * @param alreadyVisited
	 *        a set of vertices that have already been visited in the depth-first search
	 */
	private void reconstructExecutionPipeline(final ExecutionVertex vertex, final boolean forward,
			final Set<ExecutionVertex> alreadyVisited) {

		ExecutionPipeline pipeline = vertex.getExecutionPipeline();
		if (pipeline == null) {
			pipeline = new ExecutionPipeline();
			vertex.setExecutionPipeline(pipeline);
		}

		alreadyVisited.add(vertex);

		final RuntimeEnvironment env = vertex.getEnvironment();

		if (forward) {

			final int numberOfOutputGates = env.getNumberOfOutputGates();
			for (int i = 0; i < numberOfOutputGates; ++i) {

				final OutputGate<? extends Record> outputGate = env.getOutputGate(i);
				final ChannelType channelType = outputGate.getChannelType();
				final int numberOfOutputChannels = outputGate.getNumberOfOutputChannels();
				for (int j = 0; j < numberOfOutputChannels; ++j) {

					final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
					final ExecutionVertex connectedVertex = this.executionGraph.getVertexByChannelID(outputChannel
						.getConnectedChannelID());

					boolean recurse = false;

					if (!alreadyVisited.contains(connectedVertex)) {
						recurse = true;
					}

					if (channelType == ChannelType.INMEMORY
						&& !pipeline.equals(connectedVertex.getExecutionPipeline())) {

						connectedVertex.setExecutionPipeline(pipeline);
						recurse = true;
					}

					if (recurse) {
						reconstructExecutionPipeline(connectedVertex, true, alreadyVisited);
					}
				}
			}
		} else {

			final int numberOfInputGates = env.getNumberOfInputGates();
			for (int i = 0; i < numberOfInputGates; ++i) {

				final InputGate<? extends Record> inputGate = env.getInputGate(i);
				final ChannelType channelType = inputGate.getChannelType();
				final int numberOfInputChannels = inputGate.getNumberOfInputChannels();
				for (int j = 0; j < numberOfInputChannels; ++j) {

					final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(j);
					final ExecutionVertex connectedVertex = this.executionGraph.getVertexByChannelID(inputChannel
						.getConnectedChannelID());

					boolean recurse = false;

					if (!alreadyVisited.contains(connectedVertex)) {
						recurse = true;
					}

					if (channelType == ChannelType.INMEMORY
						&& !pipeline.equals(connectedVertex.getExecutionPipeline())) {

						connectedVertex.setExecutionPipeline(pipeline);
						recurse = true;
					}

					if (recurse) {
						reconstructExecutionPipeline(connectedVertex, false, alreadyVisited);
					}
				}
			}
		}
	}
}
