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

package eu.stratosphere.nephele.managementgraph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ManagementStage {

	private final ManagementGraph managementGraph;

	private int stageNumber = -1;

	private Object attachment = null;

	private final List<ManagementGroupVertex> groupVertices = new ArrayList<ManagementGroupVertex>();

	public ManagementStage(ManagementGraph managementGraph, int stageNumber) {
		this.managementGraph = managementGraph;
		this.stageNumber = stageNumber;

		this.managementGraph.addStage(this);
	}

	public ManagementGraph getGraph() {
		return this.managementGraph;
	}

	public int getStageNumber() {
		return this.stageNumber;
	}

	public int getNumberOfGroupVertices() {

		return this.groupVertices.size();
	}

	public ManagementGroupVertex getGroupVertex(int index) {

		if (index < this.groupVertices.size()) {
			return this.groupVertices.get(index);
		}

		return null;
	}

	void addGroupVertex(ManagementGroupVertex groupVertex) {

		this.groupVertices.add(groupVertex);

		this.managementGraph.addGroupVertex(groupVertex.getID(), groupVertex);
	}

	void collectGroupVertices(List<ManagementGroupVertex> groupVertices) {

		final Iterator<ManagementGroupVertex> it = this.groupVertices.iterator();

		while (it.hasNext()) {
			groupVertices.add(it.next());
		}
	}

	void collectVertices(List<ManagementVertex> vertices) {

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

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}
}
