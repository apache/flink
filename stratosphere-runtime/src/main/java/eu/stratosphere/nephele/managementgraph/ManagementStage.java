/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.managementgraph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class implements a management stage of a {@link ManagementGraph}. The stage is derived from an execution stage
 * which is used in Nephele's internal scheduling structure.
 * <p>
 * This class is not thread-safe.
 * 
 */
public final class ManagementStage extends ManagementAttachment {

	/**
	 * The management graph this management stage belongs to.
	 */
	private final ManagementGraph managementGraph;

	/**
	 * The unique number of this management stage.
	 */
	private int stageNumber = -1;

	/**
	 * The list of management group vertices which are contained in this management stage.
	 */
	private final List<ManagementGroupVertex> groupVertices = new ArrayList<ManagementGroupVertex>();

	/**
	 * Constructs a new management stage.
	 * 
	 * @param managementGraph
	 *        the management graph this management stage belongs to
	 * @param stageNumber
	 *        the number of the stage
	 */
	public ManagementStage(final ManagementGraph managementGraph, final int stageNumber) {
		this.managementGraph = managementGraph;
		this.stageNumber = stageNumber;

		this.managementGraph.addStage(this);
	}

	/**
	 * Returns the management graph this management stage is part of.
	 * 
	 * @return the management graph this management stage is part of
	 */
	public ManagementGraph getGraph() {
		return this.managementGraph;
	}

	/**
	 * Returns this management stage's number.
	 * 
	 * @return this management stage's number
	 */
	public int getStageNumber() {
		return this.stageNumber;
	}

	/**
	 * Returns the number of group vertices included in this management stage.
	 * 
	 * @return the number of group vertices included in this management stage
	 */
	public int getNumberOfGroupVertices() {

		return this.groupVertices.size();
	}

	/**
	 * Returns the management group vertex with the given index.
	 * 
	 * @param index
	 *        the index of the group vertex to be returned
	 * @return the group vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public ManagementGroupVertex getGroupVertex(final int index) {

		if (index < this.groupVertices.size()) {
			return this.groupVertices.get(index);
		}

		return null;
	}

	/**
	 * Adds the given group vertex to this management stage.
	 * 
	 * @param groupVertex
	 *        the group vertex to be added to this management stage
	 */
	void addGroupVertex(final ManagementGroupVertex groupVertex) {

		this.groupVertices.add(groupVertex);

		this.managementGraph.addGroupVertex(groupVertex.getID(), groupVertex);
	}

	/**
	 * Adds all management group vertices contained in this stage to the given list.
	 * 
	 * @param groupVertices
	 *        the list to which the group vertices in this stage shall be added
	 */
	void collectGroupVertices(final List<ManagementGroupVertex> groupVertices) {

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();

		while (it.hasNext()) {
			groupVertices.add(it.next());
		}
	}

	/**
	 * Adds all management vertices contained in this stage's group vertices to the given list.
	 * 
	 * @param vertices
	 *        the list to which the vertices in this stage shall be added
	 */
	void collectVertices(final List<ManagementVertex> vertices) {

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();

		while (it.hasNext()) {
			it.next().collectVertices(vertices);
		}
	}

	/**
	 * Returns the number of input management vertices in this stage, i.e. the number
	 * of management vertices which are connected to vertices in a lower stage
	 * or have no input channels.
	 * 
	 * @return the number of input vertices in this stage
	 */
	public int getNumberOfInputManagementVertices() {

		int retVal = 0;

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			if (groupVertex.isInputVertex()) {
				retVal += groupVertex.getNumberOfGroupMembers();
			}
		}

		return retVal;
	}

	/**
	 * Returns the number of output management vertices in this stage, i.e. the number
	 * of management vertices which are connected to vertices in a higher stage
	 * or have no output channels.
	 * 
	 * @return the number of output vertices in this stage
	 */
	public int getNumberOfOutputManagementVertices() {

		int retVal = 0;

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			if (groupVertex.isOutputVertex()) {
				retVal += groupVertex.getNumberOfGroupMembers();
			}
		}

		return retVal;
	}

	/**
	 * Returns the output management vertex with the given index or <code>null</code> if no such vertex exists.
	 * 
	 * @param index
	 *        the index of the vertex to be selected.
	 * @return the output management vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public ManagementVertex getInputManagementVertex(int index) {

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			if (groupVertex.isInputVertex()) {
				final int numberOfMembers = groupVertex.getNumberOfGroupMembers();
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
	 * Returns the input management vertex with the given index or <code>null</code> if no such vertex exists.
	 * 
	 * @param index
	 *        the index of the vertex to be selected.
	 * @return the input management vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public ManagementVertex getOutputManagementVertex(int index) {

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			if (groupVertex.isOutputVertex()) {
				final int numberOfMembers = groupVertex.getNumberOfGroupMembers();
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
	 * Returns the number of input group vertices in this stage. Input group vertices are those vertices which have
	 * incoming edges from group vertices of a lower stage.
	 * 
	 * @return the number of input group vertices in this stage
	 */
	public int getNumberOfInputGroupVertices() {

		int retVal = 0;

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();
		while (it.hasNext()) {
			if (it.next().isInputVertex()) {
				++retVal;
			}
		}

		return retVal;
	}

	/**
	 * Returns the input group vertex in this stage with the given index. Input group vertices are those vertices which
	 * have incoming edges from group vertices of a lower stage.
	 * 
	 * @param index
	 *        the index of the input group vertex to return
	 * @return the input group vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public ManagementGroupVertex getInputGroupVertex(int index) {

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			if (groupVertex.isInputVertex()) {
				if (index == 0) {
					return groupVertex;
				} else {
					--index;
				}
			}
		}

		return null;
	}

	/**
	 * Returns the number of output group vertices in this stage. Output group vertices are those vertices which have
	 * outgoing edges to group vertices of a higher stage.
	 * 
	 * @return the number of output group vertices in this stage
	 */
	public int getNumberOfOutputGroupVertices() {

		int retVal = 0;

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();
		while (it.hasNext()) {
			if (it.next().isOutputVertex()) {
				++retVal;
			}
		}

		return retVal;
	}

	/**
	 * Returns the output group vertex in this stage with the given index. Output group vertices are those vertices
	 * which have outgoing edges to group vertices of a higher stage.
	 * 
	 * @param index
	 *        the index of the output group vertex to return
	 * @return the output group vertex with the given index or <code>null</code> if no such vertex exists
	 */
	public ManagementGroupVertex getOutputGroupVertex(int index) {

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			if (groupVertex.isOutputVertex()) {
				if (index == 0) {
					return groupVertex;
				} else {
					--index;
				}
			}
		}

		return null;
	}
}
