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
 * A wrapper class that bundles a {@link KinesisStreamShard} with its last processed sequence number.
 */
public class KinesisStreamShardState {

	private KinesisStreamShard kinesisStreamShard;
	private SequenceNumber lastProcessedSequenceNum;

	public KinesisStreamShardState(KinesisStreamShard kinesisStreamShard, SequenceNumber lastProcessedSequenceNum) {
		this.kinesisStreamShard = kinesisStreamShard;
		this.lastProcessedSequenceNum = lastProcessedSequenceNum;
	}

	public KinesisStreamShard getKinesisStreamShard() {
		return this.kinesisStreamShard;
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

		return kinesisStreamShard.equals(other.getKinesisStreamShard()) && lastProcessedSequenceNum.equals(other.getLastProcessedSequenceNum());
	}

	@Override
	public int hashCode() {
		return 37 * (kinesisStreamShard.hashCode() + lastProcessedSequenceNum.hashCode());
	}
}
