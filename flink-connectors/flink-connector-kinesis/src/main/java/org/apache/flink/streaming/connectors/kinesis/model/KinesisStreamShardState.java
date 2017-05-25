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

/**
 * A wrapper class that bundles a {@link StreamShardHandle} with its last processed sequence number.
 */
public class KinesisStreamShardState {

	private KinesisStreamShardV2 kinesisStreamShard;
	private StreamShardHandle streamShardHandle;
	private SequenceNumber lastProcessedSequenceNum;

	public KinesisStreamShardState(KinesisStreamShardV2 kinesisStreamShard, StreamShardHandle streamShardHandle, SequenceNumber lastProcessedSequenceNum) {
		this.kinesisStreamShard = kinesisStreamShard;
		this.streamShardHandle = streamShardHandle;
		this.lastProcessedSequenceNum = lastProcessedSequenceNum;
	}

	public KinesisStreamShardV2 getKinesisStreamShard() {
		return this.kinesisStreamShard;
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
			"kinesisStreamShard='" + kinesisStreamShard.toString() + "'" +
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

		return kinesisStreamShard.equals(other.getKinesisStreamShard()) &&
			streamShardHandle.equals(other.getStreamShardHandle()) &&
			lastProcessedSequenceNum.equals(other.getLastProcessedSequenceNum());
	}

	@Override
	public int hashCode() {
		return 37 * (kinesisStreamShard.hashCode() + streamShardHandle.hashCode() + lastProcessedSequenceNum.hashCode());
	}
}
