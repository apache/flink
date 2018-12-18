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

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializable representation of a AWS Kinesis Stream shard. It is basically a wrapper class around the information
 * disintegrated from {@link com.amazonaws.services.kinesis.model.Shard} and its nested classes. The disintegration
 * is required to avoid being locked-in to a specific AWS SDK version in order to maintain the consumer's state
 * backwards compatibility.
 */
@Internal
public class StreamShardMetadata implements Serializable {

	private static final long serialVersionUID = 5134869582298563604L;

	private String streamName;
	private String shardId;
	private String parentShardId;
	private String adjacentParentShardId;
	private String startingHashKey;
	private String endingHashKey;
	private String startingSequenceNumber;
	private String endingSequenceNumber;

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public void setShardId(String shardId) {
		this.shardId = shardId;
	}

	public void setParentShardId(String parentShardId) {
		this.parentShardId = parentShardId;
	}

	public void setAdjacentParentShardId(String adjacentParentShardId) {
		this.adjacentParentShardId = adjacentParentShardId;
	}

	public void setStartingHashKey(String startingHashKey) {
		this.startingHashKey = startingHashKey;
	}

	public void setEndingHashKey(String endingHashKey) {
		this.endingHashKey = endingHashKey;
	}

	public void setStartingSequenceNumber(String startingSequenceNumber) {
		this.startingSequenceNumber = startingSequenceNumber;
	}

	public void setEndingSequenceNumber(String endingSequenceNumber) {
		this.endingSequenceNumber = endingSequenceNumber;
	}

	public String getStreamName() {
		return this.streamName;
	}

	public String getShardId() {
		return this.shardId;
	}

	public String getParentShardId() {
		return this.parentShardId;
	}

	public String getAdjacentParentShardId() {
		return this.adjacentParentShardId;
	}

	public String getStartingHashKey() {
		return this.startingHashKey;
	}

	public String getEndingHashKey() {
		return this.endingHashKey;
	}

	public String getStartingSequenceNumber() {
		return this.startingSequenceNumber;
	}

	public String getEndingSequenceNumber() {
		return this.endingSequenceNumber;
	}

	@Override
	public String toString() {
		return "StreamShardMetadata{" +
			"streamName='" + streamName + "'" +
			", shardId='" + shardId + "'" +
			", parentShardId='" + parentShardId + "'" +
			", adjacentParentShardId='" + adjacentParentShardId + "'" +
			", startingHashKey='" + startingHashKey + "'" +
			", endingHashKey='" + endingHashKey + "'" +
			", startingSequenceNumber='" + startingSequenceNumber + "'" +
			", endingSequenceNumber='" + endingSequenceNumber + "'}";
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof StreamShardMetadata)) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		StreamShardMetadata other = (StreamShardMetadata) obj;

		return streamName.equals(other.getStreamName()) &&
			shardId.equals(other.getShardId()) &&
			Objects.equals(parentShardId, other.getParentShardId()) &&
			Objects.equals(adjacentParentShardId, other.getAdjacentParentShardId()) &&
			Objects.equals(startingHashKey, other.getStartingHashKey()) &&
			Objects.equals(endingHashKey, other.getEndingHashKey()) &&
			Objects.equals(startingSequenceNumber, other.getStartingSequenceNumber()) &&
			Objects.equals(endingSequenceNumber, other.getEndingSequenceNumber());
	}

	@Override
	public int hashCode() {
		int hash = 17;

		if (streamName != null) {
			hash = 37 * hash + streamName.hashCode();
		}
		if (shardId != null) {
			hash = 37 * hash + shardId.hashCode();
		}
		if (parentShardId != null) {
			hash = 37 * hash + parentShardId.hashCode();
		}
		if (adjacentParentShardId != null) {
			hash = 37 * hash + adjacentParentShardId.hashCode();
		}
		if (startingHashKey != null) {
			hash = 37 * hash + startingHashKey.hashCode();
		}
		if (endingHashKey != null) {
			hash = 37 * hash + endingHashKey.hashCode();
		}
		if (startingSequenceNumber != null) {
			hash = 37 * hash + startingSequenceNumber.hashCode();
		}
		if (endingSequenceNumber != null) {
			hash = 37 * hash + endingSequenceNumber.hashCode();
		}

		return hash;
	}

	/** An equivalence wrapper that only checks for the stream name and shard id for equality. */
	public static class EquivalenceWrapper {

		private final StreamShardMetadata shardMetadata;

		public EquivalenceWrapper(StreamShardMetadata shardMetadata) {
			this.shardMetadata = checkNotNull(shardMetadata);
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof EquivalenceWrapper)) {
				return false;
			}

			if (obj == this) {
				return true;
			}

			EquivalenceWrapper other = (EquivalenceWrapper) obj;

			return shardMetadata.getStreamName().equals(other.shardMetadata.getStreamName())
				&& shardMetadata.getShardId().equals(other.shardMetadata.getShardId());
		}

		@Override
		public int hashCode() {
			int hash = 17;

			if (shardMetadata.getStreamName() != null) {
				hash = 37 * hash + shardMetadata.getStreamName().hashCode();
			}
			if (shardMetadata.getShardId() != null) {
				hash = 37 * hash + shardMetadata.getShardId().hashCode();
			}
			return hash;
		}

		public StreamShardMetadata getShardMetadata() {
			return shardMetadata;
		}
	}
}
