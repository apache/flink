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
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.List;

/**
 * An edge in the streaming topology. One edge like this does not necessarily
 * gets converted to a connection between two job vertices (due to
 * chaining/optimization).
 */
@Internal
public class StreamEdge implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String edgeId;

	private final StreamNode sourceVertex;
	private final StreamNode targetVertex;

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

	public StreamEdge(StreamNode sourceVertex, StreamNode targetVertex, int typeNumber,
			List<String> selectedNames, StreamPartitioner<?> outputPartitioner, OutputTag outputTag) {
		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
		this.typeNumber = typeNumber;
		this.selectedNames = selectedNames;
		this.outputPartitioner = outputPartitioner;
		this.outputTag = outputTag;

		this.edgeId = sourceVertex + "_" + targetVertex + "_" + typeNumber + "_" + selectedNames
				+ "_" + outputPartitioner;
	}

	public StreamNode getSourceVertex() {
		return sourceVertex;
	}

	public StreamNode getTargetVertex() {
		return targetVertex;
	}

	public int getSourceId() {
		return sourceVertex.getId();
	}

	public int getTargetId() {
		return targetVertex.getId();
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

	public void setPartitioner(StreamPartitioner<?> partitioner) {
		this.outputPartitioner = partitioner;
	}

	@Override
	public int hashCode() {
		return edgeId.hashCode();
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

		return edgeId.equals(that.edgeId);
	}

	@Override
	public String toString() {
		return "(" + sourceVertex + " -> " + targetVertex + ", typeNumber=" + typeNumber
				+ ", selectedNames=" + selectedNames + ", outputPartitioner=" + outputPartitioner
				+ ", outputTag=" + outputTag + ')';
	}
}
