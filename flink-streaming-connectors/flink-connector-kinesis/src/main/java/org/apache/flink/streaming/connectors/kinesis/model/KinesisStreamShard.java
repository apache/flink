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

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A serializable representation of a AWS Kinesis Stream shard. It is basically a wrapper class around the information
 * provided along with {@link com.amazonaws.services.kinesis.model.Shard}, with some extra utility methods to
 * determine whether or not a shard is closed and whether or not the shard is a result of parent shard splits or merges.
 */
public class KinesisStreamShard implements Serializable {

	//private static final long serialVersionUID = 1L;

	private final String regionName;
	private final String streamName;
	private final String shardId;

	private String startingSequenceNumber;
	private String endingSequenceNumber;

	private String parentShardId;
	private String adjacentParentShardId;

	private final int cachedHash;

	/**
	 * Create a new KinesisStreamShard
	 *
	 * @param regionName
	 *           the AWS service of this shard
	 * @param streamName
	 *           the name of the Kinesis stream that this shard belongs to
	 * @param shardId
	 *           unique ID of this shard
	 * @param startingSequenceNumber
	 *           the starting sequence number of this shard
	 * @param endingSequenceNumber
	 *           the ending sequence number of this shard (may be null if the shard isn't closed yet)
	 * @param parentShardId
	 *           the ID of the parent shard (will only exist if this shard is a result of a parent shard split or merge)
	 * @param adjacentParentShardId
	 *           the ID of the shard adjacent to the parent shard (will only exist if this shard is a result of a merge of 2 parent shards)
	 */
	public KinesisStreamShard(String regionName, String streamName, String shardId,
							String startingSequenceNumber, String endingSequenceNumber,
							String parentShardId, String adjacentParentShardId) {
		this.regionName = checkNotNull(regionName);
		this.streamName = checkNotNull(streamName);
		this.shardId = checkNotNull(shardId);
		this.startingSequenceNumber = checkNotNull(startingSequenceNumber);
		this.endingSequenceNumber = endingSequenceNumber; // can be null if the shard is not closed
		this.parentShardId = parentShardId; // can be null if the shard is not a result of a parent shard split or merge
		this.adjacentParentShardId = adjacentParentShardId; // can be null if the shard is not a result of a parent merge

		int hash = 0;
		hash += regionName.hashCode() + streamName.hashCode() + shardId.hashCode() + startingSequenceNumber.hashCode();
		hash += (endingSequenceNumber != null) ? endingSequenceNumber.hashCode() : 0;
		hash += (parentShardId != null) ? parentShardId.hashCode() : 0;
		hash += (adjacentParentShardId != null) ? adjacentParentShardId.hashCode() : 0;
		hash *= 37;
		this.cachedHash = hash;
	}

	public String getRegionName() {
		return regionName;
	}

	public String getStreamName() {
		return streamName;
	}

	public String getShardId() {
		return shardId;
	}

	public String getStartingSequenceNumber() {
		return startingSequenceNumber;
	}

	public String getEndingSequenceNumber() {
		return endingSequenceNumber;
	}

	public boolean isClosed() {
		return (endingSequenceNumber != null);
	}

	public String getParentShardId() {
		return parentShardId;
	}

	public String getAdjacentParentShardId() {
		return adjacentParentShardId;
	}

	public boolean isSplitShard() {
		return (parentShardId != null && adjacentParentShardId == null);
	}

	public boolean isMergedShard() {
		return (parentShardId != null && adjacentParentShardId != null);
	}

	@Override
	public String toString() {
		return "KinesisStreamShard{" +
			"regionName='" + regionName + "'" +
			", streamName='" + streamName + "'" +
			", shardId='" + shardId + "'" +
			", parentShardId='" + parentShardId + "'" +
			", adjacentParentShardId='" + adjacentParentShardId + "'" +
			", startingSequenceNumber='" + startingSequenceNumber + "'" +
			", endingSequenceNumber='" + endingSequenceNumber + "}";
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

		return regionName.equals(other.getRegionName()) &&
			shardId.equals(other.getShardId()) &&
			parentShardId.equals(other.getParentShardId()) &&
			adjacentParentShardId.equals(other.getAdjacentParentShardId()) &&
			streamName.equals(other.getStreamName()) &&
			startingSequenceNumber.equals(other.getStartingSequenceNumber()) &&
			endingSequenceNumber.equals(other.getEndingSequenceNumber());
	}

	@Override
	public int hashCode() {
		return cachedHash;
	}
}
