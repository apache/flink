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

import com.amazonaws.services.kinesis.model.Shard;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A serializable representation of a AWS Kinesis Stream shard. It is basically a wrapper class around the information
 * provided along with {@link com.amazonaws.services.kinesis.model.Shard}, with some extra utility methods to
 * determine whether or not a shard is closed and whether or not the shard is a result of parent shard splits or merges.
 */
public class KinesisStreamShard implements Serializable {

	private static final long serialVersionUID = -6004217801761077536L;

	private final String streamName;
	private final Shard shard;

	private final int cachedHash;

	/**
	 * Create a new KinesisStreamShard
	 *
	 * @param streamName
	 *           the name of the Kinesis stream that this shard belongs to
	 * @param shard
	 *           the actual AWS Shard instance that will be wrapped within this KinesisStreamShard
	 */
	public KinesisStreamShard(String streamName, Shard shard) {
		this.streamName = checkNotNull(streamName);
		this.shard = checkNotNull(shard);

		this.cachedHash = 37 * (streamName.hashCode() + shard.hashCode());
	}

	public String getStreamName() {
		return streamName;
	}

	public String getShardId() {
		return shard.getShardId();
	}

	public String getStartingSequenceNumber() {
		return shard.getSequenceNumberRange().getStartingSequenceNumber();
	}

	public String getEndingSequenceNumber() {
		return shard.getSequenceNumberRange().getEndingSequenceNumber();
	}

	public String getStartingHashKey() {
		return shard.getHashKeyRange().getStartingHashKey();
	}

	public String getEndingHashKey() {
		return shard.getHashKeyRange().getEndingHashKey();
	}

	public boolean isClosed() {
		return (getEndingSequenceNumber() != null);
	}

	public String getParentShardId() {
		return shard.getParentShardId();
	}

	public String getAdjacentParentShardId() {
		return shard.getAdjacentParentShardId();
	}

	public boolean isSplitShard() {
		return (getParentShardId() != null && getAdjacentParentShardId() == null);
	}

	public boolean isMergedShard() {
		return (getParentShardId() != null && getAdjacentParentShardId() != null);
	}

	public Shard getShard() {
		return shard;
	}

	@Override
	public String toString() {
		return "KinesisStreamShard{" +
			"streamName='" + streamName + "'" +
			", shardId='" + getShardId() + "'" +
			", parentShardId='" + getParentShardId() + "'" +
			", adjacentParentShardId='" + getAdjacentParentShardId() + "'" +
			", startingSequenceNumber='" + getStartingSequenceNumber() + "'" +
			", endingSequenceNumber='" + getEndingSequenceNumber() + "'" +
			", startingHashKey='" + getStartingHashKey() + "'" +
			", endingHashKey='" + getEndingHashKey() + "'}";
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof KinesisStreamShard)) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		KinesisStreamShard other = (KinesisStreamShard) obj;

		return streamName.equals(other.getStreamName()) && shard.equals(other.getShard());
	}

	@Override
	public int hashCode() {
		return cachedHash;
	}
}
