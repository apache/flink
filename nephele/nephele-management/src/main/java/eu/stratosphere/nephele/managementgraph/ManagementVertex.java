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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.EnumUtils;

public class ManagementVertex implements IOReadableWritable {

	private final ManagementGroupVertex groupVertex;

	private final ManagementVertexID id;

	private final List<ManagementGate> inputGates = new ArrayList<ManagementGate>();

	private final List<ManagementGate> outputGates = new ArrayList<ManagementGate>();

	private ExecutionState executionState = ExecutionState.CREATED;

	private final String instanceName;

	private final String instanceType;

	private final int indexInGroup;

	private Object attachment;

	public ManagementVertex(ManagementGroupVertex groupVertex, ManagementVertexID id, String instanceName,
			String instanceType, int indexInGroup) {
		this.groupVertex = groupVertex;
		this.id = id;
		this.instanceName = instanceName;
		this.instanceType = instanceType;

		this.indexInGroup = indexInGroup;

		groupVertex.addGroupMember(this);
	}

	void addGate(ManagementGate gate) {

		if (gate.isInputGate()) {
			this.inputGates.add(gate);
		} else {
			this.outputGates.add(gate);
		}
	}

	public String getInstanceName() {
		return this.instanceName;
	}

	public String getInstanceType() {
		return this.instanceType;
	}

	public int getNumberOfInputGates() {
		return this.inputGates.size();
	}

	public int getNumberOfOutputGates() {
		return this.outputGates.size();
	}

	public ManagementGate getInputGate(int index) {

		if (index < this.inputGates.size()) {
			return this.inputGates.get(index);
		}

		return null;
	}

	public ManagementGate getOutputGate(int index) {

		if (index < this.outputGates.size()) {
			return this.outputGates.get(index);
		}

		return null;
	}

	public ManagementGroupVertex getGroupVertex() {
		return this.groupVertex;
	}

	public ManagementGraph getGraph() {
		return this.groupVertex.getGraph();
	}

	public ManagementVertexID getID() {
		return this.id;
	}

	public String getName() {

		return this.groupVertex.getName();
	}

	public int getNumberOfVerticesInGroup() {

		return this.groupVertex.getNumberOfGroupMembers();
	}

	public int getIndexInGroup() {

		return this.indexInGroup;
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}

	public void setExecutionState(ExecutionState executionState) {
		this.executionState = executionState;
	}

	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	@Override
	public void read(DataInput in) throws IOException {

		// Read the execution state
		this.executionState = EnumUtils.readEnum(in, ExecutionState.class);

		// Read number of input gates and record types
		int numberOfInputGates = in.readInt();
		for (int i = 0; i < numberOfInputGates; i++) {

			final String recordType = StringRecord.readString(in);
			new ManagementGate(this, i, true, recordType);
		}

		// Read number of input gates and record types
		int numberOfOutputGates = in.readInt();
		for (int i = 0; i < numberOfOutputGates; i++) {

			final String recordType = StringRecord.readString(in);
			new ManagementGate(this, i, false, recordType);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {

		// Write the execution state
		EnumUtils.writeEnum(out, this.executionState);

		// Write out number of input gates and record types
		out.writeInt(this.inputGates.size());
		Iterator<ManagementGate> it = this.inputGates.iterator();
		while (it.hasNext()) {

			final ManagementGate managementGate = it.next();
			StringRecord.writeString(out, managementGate.getRecordType());
		}

		// Write out number of output gates and record types
		out.writeInt(this.outputGates.size());
		it = this.outputGates.iterator();
		while (it.hasNext()) {

			final ManagementGate managementGate = it.next();
			StringRecord.writeString(out, managementGate.getRecordType());
		}
	}
}
