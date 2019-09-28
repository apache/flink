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
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;

/**
 * Special flag values for sequence numbers in shards to indicate special positions.
 * The value is initially set by {@link FlinkKinesisConsumer} when {@link KinesisDataFetcher}s are created.
 * The KinesisDataFetchers will use this value to determine how to retrieve the starting shard iterator from AWS Kinesis.
 */
@Internal
public enum SentinelSequenceNumber {

	/** Flag value for shard's sequence numbers to indicate that the
	 * shard should start to be read from the latest incoming records. */
	SENTINEL_LATEST_SEQUENCE_NUM(new SequenceNumber("LATEST_SEQUENCE_NUM")),

	/** Flag value for shard's sequence numbers to indicate that the shard should
	 * start to be read from the earliest records that haven't expired yet. */
	SENTINEL_EARLIEST_SEQUENCE_NUM(new SequenceNumber("EARLIEST_SEQUENCE_NUM")),

	/** Flag value for shard's sequence numbers to indicate that the shard should
	 * start to be read from the specified timestamp. */
	SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM(new SequenceNumber("AT_TIMESTAMP_SEQUENCE_NUM")),

	/** Flag value to indicate that we have already read the last record of this shard
	 * (Note: Kinesis shards that have been closed due to a split or merge will have an ending data record). */
	SENTINEL_SHARD_ENDING_SEQUENCE_NUM(new SequenceNumber("SHARD_ENDING_SEQUENCE_NUM"));

	private SequenceNumber sentinel;

	SentinelSequenceNumber(SequenceNumber sentinel) {
		this.sentinel = sentinel;
	}

	public SequenceNumber get() {
		return sentinel;
	}

	/**
	 * Returns {@code true} if the given {@link SequenceNumber} is a sentinel.
	 */
	public static boolean isSentinelSequenceNumber(SequenceNumber candidateSequenceNumber) {
		for (SentinelSequenceNumber sentinel : values()) {
			if (candidateSequenceNumber.equals(sentinel.get())) {
				return true;
			}
		}
		return false;
	}
}
