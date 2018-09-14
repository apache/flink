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

package org.apache.flink.streaming.connectors.kinesis.model;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.amazonaws.services.kinesis.model.Shard;

/**
 * A wrapper class that bundles a {@link StreamShardHandle} with its last processed sequence number.
 */
@Internal
public class KinesisStreamShardState {

	/** A handle object that wraps the actual {@link Shard} instance and stream name. */
	private StreamShardHandle streamShardHandle;

	/** The checkpointed state for each Kinesis stream shard. */
	private StreamShardMetadata streamShardMetadata;
	private SequenceNumber lastProcessedSequenceNum;

	public KinesisStreamShardState(
			StreamShardMetadata streamShardMetadata,
			StreamShardHandle streamShardHandle,
			SequenceNumber lastProcessedSequenceNum) {

		this.streamShardMetadata = Preconditions.checkNotNull(streamShardMetadata);
		this.streamShardHandle = Preconditions.checkNotNull(streamShardHandle);
		this.lastProcessedSequenceNum = Preconditions.checkNotNull(lastProcessedSequenceNum);
	}

	public StreamShardMetadata getStreamShardMetadata() {
		return this.streamShardMetadata;
	}

	public StreamShardHandle getStreamShardHandle() {
		return this.streamShardHandle;
	}

	public SequenceNumber getLastProcessedSequenceNum() {
		return this.lastProcessedSequenceNum;
	}

	public void setLastProcessedSequenceNum(SequenceNumber update) {
		this.lastProcessedSequenceNum = update;
	}

	@Override
	public String toString() {
		return "KinesisStreamShardState{" +
			"streamShardMetadata='" + streamShardMetadata.toString() + "'" +
			", streamShardHandle='" + streamShardHandle.toString() + "'" +
			", lastProcessedSequenceNumber='" + lastProcessedSequenceNum.toString() + "'}";
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof KinesisStreamShardState)) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		KinesisStreamShardState other = (KinesisStreamShardState) obj;

		return streamShardMetadata.equals(other.getStreamShardMetadata()) &&
			streamShardHandle.equals(other.getStreamShardHandle()) &&
			lastProcessedSequenceNum.equals(other.getLastProcessedSequenceNum());
	}

	@Override
	public int hashCode() {
		return 37 * (streamShardMetadata.hashCode() + streamShardHandle.hashCode() + lastProcessedSequenceNum.hashCode());
	}
}
