/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.jobgraph.EdgeID;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * An edge in the streaming topology. One edge like this does not necessarily
 * gets converted to a connection between two job vertices (due to
 * chaining/optimization).
 */
@Internal
public class StreamEdge implements Serializable {

	private static final long serialVersionUID = 1L;

	private final EdgeID edgeID;

	private final String edgeName;

	private final int sourceId;
	private final int targetId;

	/**
	 * The type number of the input for co-tasks.
	 */
	private final int typeNumber;

	/**
	 * A list of output names that the target vertex listens to (if there is
	 * output selection).
	 */
	private final List<String> selectedNames;

	/**
	 * The side-output tag (if any) of this {@link StreamEdge}.
	 */
	private final OutputTag outputTag;

	/**
	 * The {@link StreamPartitioner} on this {@link StreamEdge}.
	 */
	private StreamPartitioner<?> outputPartitioner;

	/**
	 * The {@link DataExchangeMode} on this {@link StreamEdge}.
	 */
	private DataExchangeMode dataExchangeMode;

	/**
	 * The {@link DamBehavior} of the source operator on this {@link StreamEdge}.
	 */
	private final DamBehavior damBehavior;

	@VisibleForTesting
	public StreamEdge(StreamNode sourceVertex, StreamNode targetVertex, int typeNumber,
			List<String> selectedNames, StreamPartitioner<?> outputPartitioner, OutputTag outputTag) {
		this(sourceVertex, targetVertex, typeNumber, selectedNames, outputPartitioner, outputTag,
			DataExchangeMode.PIPELINED, DamBehavior.PIPELINED);
	}

	public StreamEdge(StreamNode sourceVertex, StreamNode targetVertex, int typeNumber,
			List<String> selectedNames, StreamPartitioner<?> outputPartitioner, OutputTag outputTag,
			DataExchangeMode dataExchangeMode, DamBehavior damBehavior) {
		this.sourceId = sourceVertex.getId();
		this.targetId = targetVertex.getId();
		this.typeNumber = typeNumber;
		this.selectedNames = selectedNames;
		this.outputPartitioner = outputPartitioner;
		this.outputTag = outputTag;
		this.dataExchangeMode = dataExchangeMode;
		this.damBehavior = damBehavior;

		this.edgeID = new EdgeID();

		this.edgeName = sourceVertex + "_" + targetVertex + "_" + typeNumber + "_" + selectedNames
				+ "_" + outputPartitioner;
	}

	public EdgeID getEdgeID() {
		return edgeID;
	}

	public int getSourceId() {
		return sourceId;
	}

	public int getTargetId() {
		return targetId;
	}

	public int getTypeNumber() {
		return typeNumber;
	}

	public List<String> getSelectedNames() {
		return selectedNames;
	}

	public OutputTag getOutputTag() {
		return this.outputTag;
	}

	public StreamPartitioner<?> getPartitioner() {
		return outputPartitioner;
	}

	public DataExchangeMode getDataExchangeMode() {
		return dataExchangeMode;
	}

	public DamBehavior getDamBehavior() {
		return this.damBehavior;
	}

	public void setPartitioner(StreamPartitioner<?> partitioner) {
		this.outputPartitioner = partitioner;
	}

	public void setDataExchangeMode(DataExchangeMode dataExchangeMode) {
		this.dataExchangeMode = dataExchangeMode;
	}

	@Override
	public int hashCode() {
		return Objects.hash(edgeName, edgeID);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StreamEdge that = (StreamEdge) o;

		return edgeName.equals(that.edgeName) && edgeID.equals(that.getEdgeID());
	}

	@Override
	public String toString() {
		return "(" + sourceId + " -> " + targetId + ", typeNumber=" + typeNumber
				+ ", selectedNames=" + selectedNames + ", outputPartitioner=" + outputPartitioner
				+ ", outputTag=" + outputTag + ", dataExchangeMode=" + dataExchangeMode + ", edgeID=" + edgeID + ')';
	}
}
