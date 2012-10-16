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

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;

/**
 * Objects of this class represent either an {@link InputGate} or {@link OutputGate} within an {@link ExecutionGraph},
 * Nephele's internal scheduling representation for jobs.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class ExecutionGate {

	private final GateID gateID;

	private volatile ExecutionVertex vertex;

	private final ExecutionGroupEdge groupEdge;

	private final boolean isInputGate;

	private final CopyOnWriteArrayList<ExecutionEdge> edges = new CopyOnWriteArrayList<ExecutionEdge>();

	ExecutionGate(final GateID gateID, final ExecutionVertex vertex, final ExecutionGroupEdge groupEdge,
			final boolean isInputGate) {

		this.gateID = gateID;
		this.vertex = vertex;
		this.groupEdge = groupEdge;
		this.isInputGate = isInputGate;
	}

	public GateID getGateID() {

		return this.gateID;
	}

	public ExecutionVertex getVertex() {

		return this.vertex;
	}

	public boolean isInputGate() {

		return this.isInputGate;
	}

	public int getNumberOfEdges() {

		return this.edges.size();
	}

	public ExecutionEdge getEdge(final int index) {

		return this.edges.get(index);
	}

	void replaceAllEdges(final Collection<ExecutionEdge> newEdges) {

		this.edges.clear();
		this.edges.addAll(newEdges);
	}

	public ChannelType getChannelType() {

		return this.groupEdge.getChannelType();
	}

	public CompressionLevel getCompressionLevel() {

		return this.groupEdge.getCompressionLevel();
	}

	public boolean allowSpanningRecords() {

		return this.groupEdge.allowSpanningRecords();
	}

	ExecutionGroupEdge getGroupEdge() {

		return this.groupEdge;
	}
}
