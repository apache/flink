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
import java.util.List;

public class ManagementGate {

	private final ManagementVertex managementVertex;

	private final String recordType;

	private final boolean isInputGate;

	private final int index;

	private Object attachment = null;

	private final List<ManagementEdge> forwardEdges = new ArrayList<ManagementEdge>();

	private final List<ManagementEdge> backwardEdges = new ArrayList<ManagementEdge>();

	public ManagementGate(ManagementVertex managementVertex, int index, boolean isInputGate, String recordType) {
		this.isInputGate = isInputGate;
		this.managementVertex = managementVertex;
		this.recordType = recordType;
		this.index = index;

		managementVertex.addGate(this);
	}

	public boolean isInputGate() {
		return this.isInputGate;
	}

	void insertForwardEdge(ManagementEdge managementEdge, int index) {

		while (index >= this.forwardEdges.size()) {
			this.forwardEdges.add(null);
		}

		this.forwardEdges.set(index, managementEdge);
	}

	void insertBackwardEdge(ManagementEdge managementEdge, int index) {

		while (index >= this.backwardEdges.size()) {
			this.backwardEdges.add(null);
		}

		this.backwardEdges.set(index, managementEdge);
	}

	public String getRecordType() {
		return this.recordType;
	}

	public ManagementGraph getGraph() {
		return this.managementVertex.getGraph();
	}

	public int getNumberOfForwardEdges() {
		return this.forwardEdges.size();
	}

	public int getNumberOfBackwardEdges() {
		return this.backwardEdges.size();
	}

	public int getIndex() {
		return this.index;
	}

	public ManagementEdge getForwardEdge(int index) {

		if (index < this.forwardEdges.size()) {
			return this.forwardEdges.get(index);
		}

		return null;
	}

	public ManagementEdge getBackwardEdge(int index) {

		if (index < this.backwardEdges.size()) {
			return this.backwardEdges.get(index);
		}

		return null;
	}

	public ManagementVertex getVertex() {
		return this.managementVertex;
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}
}
