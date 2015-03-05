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

package org.apache.flink.streaming.api;

import java.io.Serializable;
import java.util.List;

import org.apache.flink.streaming.partitioner.StreamPartitioner;

/**
 * An edge in the streaming topology. One edge like this does not necessarily
 * gets converted to a connection between two job vertices (due to chaining/optimization).
 */
public class StreamEdge implements Serializable {

	final private int sourceVertex;
	final private int targetVertex;

	/**
	 * The type number of the input for co-tasks.
	 */
	final private int typeNumber;

	/**
	 * A list of output names that the target vertex listens to (if there is output selection).
	 */
	final private List<String> selectedNames;
	final private StreamPartitioner<?> outputPartitioner;

	public StreamEdge(int sourceVertex, int targetVertex, int typeNumber, List<String> selectedNames, StreamPartitioner<?> outputPartitioner) {
		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
		this.typeNumber = typeNumber;
		this.selectedNames = selectedNames;
		this.outputPartitioner = outputPartitioner;
	}

	public int getSourceVertex() {
		return sourceVertex;
	}

	public int getTargetVertex() {
		return targetVertex;
	}

	public int getTypeNumber() {
		return typeNumber;
	}

	public List<String> getSelectedNames() {
		return selectedNames;
	}

	public StreamPartitioner<?> getPartitioner() {
		return outputPartitioner;
	}

	@Override
	public String toString() {
		return "StreamGraphEdge{" +
				"sourceVertex=" + sourceVertex +
				", targetVertex=" + targetVertex +
				", typeNumber=" + typeNumber +
				", selectedNames=" + selectedNames +
				", outputPartitioner=" + outputPartitioner +
				'}';
	}
}
