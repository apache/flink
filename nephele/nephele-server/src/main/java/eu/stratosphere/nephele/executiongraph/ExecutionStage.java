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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;

/**
 * An execution stage contains all execution group vertices (and as a result all execution vertices) which
 * must run at the same time. The execution of a job progresses in terms of stages, i.e. the next stage of a
 * job can only start to execute if the execution of its preceding stage is complete.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class ExecutionStage {

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
	private final ArrayList<ExecutionGroupVertex> stageMembers = new ArrayList<ExecutionGroupVertex>();

	/**
	 * Number of the stage.
	 */
	private int stageNum = -1;

	/**
	 * Constructs a new execution stage and assigns the given stage number to it.
	 * 
	 * @param executionGraph
	 *        the executionGraph that this stage belongs to
	 * @param stageNum
	 *        the number of this execution stage
	 */
	public ExecutionStage(ExecutionGraph executionGraph, int stageNum) {
		this.executionGraph = executionGraph;
		this.stageNum = stageNum;
	}

	/**
	 * Sets the number of this execution stage.
	 * 
	 * @param stageNum
	 *        the new number of this execution stage
	 */
	public synchronized void setStageNumber(int stageNum) {
		this.stageNum = stageNum;
	}

	/**
	 * Returns the number of this execution stage.
	 * 
	 * @return the number of this execution stage
	 */
	public synchronized int getStageNumber() {

		return this.stageNum;
	}

	/**
	 * Adds a new execution group vertex to this stage if it is not already included.
	 * 
	 * @param groupVertex
	 *        the new execution group vertex to include
	 */
	public void addStageMember(ExecutionGroupVertex groupVertex) {

		synchronized (this.stageMembers) {

			if (!this.stageMembers.contains(groupVertex)) {
				this.stageMembers.add(groupVertex);
			}
		}

		groupVertex.setExecutionStage(this);
	}

	/**
	 * Removes the specified group vertex from the execution stage.
	 * 
	 * @param groupVertex
	 *        the group vertex to remove from the stage
	 */
	public void removeStageMember(ExecutionGroupVertex groupVertex) {

		synchronized (this.stageMembers) {
			this.stageMembers.remove(groupVertex);
		}
	}

	/**
	 * Returns the number of group vertices this execution stage includes.
	 * 
	 * @return the number of group vertices this execution stage includes
	 */
	public int getNumberOfStageMembers() {

		synchronized (this.stageMembers) {
			return this.stageMembers.size();
		}
	}

	/**
	 * Returns the stage member internally stored at index <code>index</code>.
	 * 
	 * @param index
	 *        the index of the group vertex to return
	 * @return the stage member internally stored at the specified index or <code>null</code> if no group vertex exists
	 *         with such an index
	 */
	public ExecutionGroupVertex getStageMember(int index) {

		synchronized (this.stageMembers) {

			if (index < this.stageMembers.size()) {
				return this.stageMembers.get(index);
			}
		}

		return null;
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

		synchronized (this.stageMembers) {

			final Iterator<ExecutionGroupVertex> it = this.stageMembers.iterator();
			while (it.hasNext()) {

				final ExecutionGroupVertex groupVertex = it.next();
				if (groupVertex.isInputVertex()) {
					retVal += groupVertex.getCurrentNumberOfGroupMembers();
				}
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

		synchronized (this.stageMembers) {

			final Iterator<ExecutionGroupVertex> it = this.stageMembers.iterator();
			while (it.hasNext()) {

				final ExecutionGroupVertex groupVertex = it.next();
				if (groupVertex.isOutputVertex()) {
					retVal += groupVertex.getCurrentNumberOfGroupMembers();
				}
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

		synchronized (this.stageMembers) {

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

		synchronized (this.stageMembers) {

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

		for (int i = 0; i < getNumberOfStageMembers(); i++) {

			final ExecutionGroupVertex groupVertex = getStageMember(i);

			for (int j = 0; j < groupVertex.getCurrentNumberOfGroupMembers(); j++) {
				// Get the instance type from the execution vertex if it
				final ExecutionVertex vertex = groupVertex.getGroupMember(j);
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
}