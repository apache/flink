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

import com.amazonaws.services.kinesis.model.Shard;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapper class around the information provided along with streamName and
 * {@link com.amazonaws.services.kinesis.model.Shard}, with some extra utility methods to determine whether
 * or not a shard is closed and whether or not the shard is a result of parent shard splits or merges.
 */
@Internal
public class StreamShardHandle {

	private final String streamName;
	private final Shard shard;

	private final int cachedHash;

	/**
	 * Create a new StreamShardHandle.
	 *
	 * @param streamName
	 *           the name of the Kinesis stream that this shard belongs to
	 * @param shard
	 *           the actual AWS Shard instance that will be wrapped within this StreamShardHandle
	 */
	public StreamShardHandle(String streamName, Shard shard) {
		this.streamName = checkNotNull(streamName);
		this.shard = checkNotNull(shard);

		// since our description of Kinesis Streams shards can be fully defined with the stream name and shard id,
		// our hash doesn't need to use hash code of Amazon's description of Shards, which uses other info for calculation
		int hash = 17;
		hash = 37 * hash + streamName.hashCode();
		hash = 37 * hash + shard.getShardId().hashCode();
		this.cachedHash = hash;
	}

	public String getStreamName() {
		return streamName;
	}

	public boolean isClosed() {
		return (shard.getSequenceNumberRange().getEndingSequenceNumber() != null);
	}

	public Shard getShard() {
		return shard;
	}

	@Override
	public String toString() {
		return "StreamShardHandle{" +
			"streamName='" + streamName + "'" +
			", shard='" + shard.toString() + "'}";
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof StreamShardHandle)) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		StreamShardHandle other = (StreamShardHandle) obj;

		return streamName.equals(other.getStreamName()) && shard.equals(other.getShard());
	}

	@Override
	public int hashCode() {
		return cachedHash;
	}

	/**
	 * Utility function to compare two shard ids.
	 *
	 * @param firstShardId first shard id to compare
	 * @param secondShardId second shard id to compare
	 * @return a value less than 0 if the first shard id is smaller than the second shard id,
	 *         or a value larger than 0 the first shard is larger then the second shard id,
	 *         or 0 if they are equal
	 */
	public static int compareShardIds(String firstShardId, String secondShardId) {
		if (!isValidShardId(firstShardId)) {
			throw new IllegalArgumentException("The first shard id has invalid format.");
		}

		if (!isValidShardId(secondShardId)) {
			throw new IllegalArgumentException("The second shard id has invalid format.");
		}

		// digit segment of the shard id starts at index 8
		return Long.compare(Long.parseLong(firstShardId.substring(8)), Long.parseLong(secondShardId.substring(8)));
	}

	/**
	 * Checks if a shard id has valid format.
	 * Kinesis stream shard ids have 12-digit numbers left-padded with 0's,
	 * prefixed with "shardId-", ex. "shardId-000000000015".
	 *
	 * @param shardId the shard id to check
	 * @return whether the shard id is valid
	 */
	public static boolean isValidShardId(String shardId) {
		if (shardId == null) {
			return false;
		}
		return shardId.matches("^shardId-\\d{12}");
	}
}
