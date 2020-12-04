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
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An edge in the streaming topology. One edge like this does not necessarily
 * gets converted to a connection between two job vertices (due to
 * chaining/optimization).
 */
@Internal
public class StreamEdge implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final long ALWAYS_FLUSH_BUFFER_TIMEOUT = 0L;

	private final String edgeId;

	private final int sourceId;
	private final int targetId;

	/**
	 * The type number of the input for co-tasks.
	 */
	private final int typeNumber;
	/**
	 * The side-output tag (if any) of this {@link StreamEdge}.
	 */
	private final OutputTag outputTag;

	/**
	 * The {@link StreamPartitioner} on this {@link StreamEdge}.
	 */
	private StreamPartitioner<?> outputPartitioner;

	/**
	 * The name of the operator in the source vertex.
	 */
	private final String sourceOperatorName;

	/**
	 * The name of the operator in the target vertex.
	 */
	private final String targetOperatorName;

	private final ShuffleMode shuffleMode;

	private long bufferTimeout;

	public StreamEdge(
		StreamNode sourceVertex,
		StreamNode targetVertex,
		int typeNumber,
		StreamPartitioner<?> outputPartitioner,
		OutputTag outputTag) {

		this(
			sourceVertex,
			targetVertex,
			typeNumber,
			ALWAYS_FLUSH_BUFFER_TIMEOUT,
			outputPartitioner,
			outputTag,
			ShuffleMode.UNDEFINED);
	}

	public StreamEdge(
		StreamNode sourceVertex,
		StreamNode targetVertex,
		int typeNumber,
		StreamPartitioner<?> outputPartitioner,
		OutputTag outputTag,
		ShuffleMode shuffleMode) {

		this(
			sourceVertex,
			targetVertex,
			typeNumber,
			sourceVertex.getBufferTimeout(),
			outputPartitioner,
			outputTag,
			shuffleMode);
	}

	public StreamEdge(
		StreamNode sourceVertex,
		StreamNode targetVertex,
		int typeNumber,
		long bufferTimeout,
		StreamPartitioner<?> outputPartitioner,
		OutputTag outputTag,
		ShuffleMode shuffleMode) {

		this.sourceId = sourceVertex.getId();
		this.targetId = targetVertex.getId();
		this.typeNumber = typeNumber;
		this.bufferTimeout = bufferTimeout;
		this.outputPartitioner = outputPartitioner;
		this.outputTag = outputTag;
		this.sourceOperatorName = sourceVertex.getOperatorName();
		this.targetOperatorName = targetVertex.getOperatorName();
		this.shuffleMode = checkNotNull(shuffleMode);
		this.edgeId = sourceVertex + "_" + targetVertex + "_" + typeNumber  + "_" + outputPartitioner;
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

	public OutputTag getOutputTag() {
		return this.outputTag;
	}

	public StreamPartitioner<?> getPartitioner() {
		return outputPartitioner;
	}

	public ShuffleMode getShuffleMode() {
		return shuffleMode;
	}

	public void setPartitioner(StreamPartitioner<?> partitioner) {
		this.outputPartitioner = partitioner;
	}

	public void setBufferTimeout(long bufferTimeout) {
		checkArgument(bufferTimeout >= -1);
		this.bufferTimeout = bufferTimeout;
	}

	public long getBufferTimeout() {
		return bufferTimeout;
	}

	@Override
	public int hashCode() {
		return Objects.hash(edgeId, outputTag);
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
		return Objects.equals(edgeId, that.edgeId) &&
			Objects.equals(outputTag, that.outputTag);
	}

	@Override
	public String toString() {
		return "(" + (sourceOperatorName + "-" + sourceId) + " -> " + (targetOperatorName + "-" + targetId)
			+ ", typeNumber=" + typeNumber + ", outputPartitioner=" + outputPartitioner
			+ ", bufferTimeout=" + bufferTimeout + ", outputTag=" + outputTag + ')';
	}
}
