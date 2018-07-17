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

package org.apache.flink.streaming.connectors.kinesis.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumer;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;

import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * Optional consumer specific configuration keys and default values for {@link FlinkKinesisConsumer}.
 */
@PublicEvolving
public class ConsumerConfigConstants extends AWSConfigConstants {

	/**
	 * The initial position to start reading shards from. This will affect the {@link ShardIteratorType} used
	 * when the consumer tasks retrieve the first shard iterator for each Kinesis shard.
	 */
	public enum InitialPosition {

		/** Start reading from the earliest possible record in the stream (excluding expired data records). */
		TRIM_HORIZON(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM),

		/** Start reading from the latest incoming record. */
		LATEST(SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM),

		/** Start reading from the record at the specified timestamp. */
		AT_TIMESTAMP(SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM);

		private SentinelSequenceNumber sentinelSequenceNumber;

		InitialPosition(SentinelSequenceNumber sentinelSequenceNumber) {
			this.sentinelSequenceNumber = sentinelSequenceNumber;
		}

		public SentinelSequenceNumber toSentinelSequenceNumber() {
			return this.sentinelSequenceNumber;
		}
	}

	/** The initial position to start reading Kinesis streams from (LATEST is used if not set). */
	public static final String STREAM_INITIAL_POSITION = "flink.stream.initpos";

	/** The initial timestamp to start reading Kinesis stream from (when AT_TIMESTAMP is set for STREAM_INITIAL_POSITION). */
	public static final String STREAM_INITIAL_TIMESTAMP = "flink.stream.initpos.timestamp";

	/** The date format of initial timestamp to start reading Kinesis stream from (when AT_TIMESTAMP is set for STREAM_INITIAL_POSITION). */
	public static final String STREAM_TIMESTAMP_DATE_FORMAT = "flink.stream.initpos.timestamp.format";

	/**
	 * Deprecated key.
	 *
	 * @deprecated Use {@link ConsumerConfigConstants#LIST_SHARDS_BACKOFF_BASE} instead
	 **/
	@Deprecated
	/** The base backoff time between each describeStream attempt. */
	public static final String STREAM_DESCRIBE_BACKOFF_BASE = "flink.stream.describe.backoff.base";

	/**
	 * Deprecated key.
	 *
	 * @deprecated Use {@link ConsumerConfigConstants#LIST_SHARDS_BACKOFF_MAX} instead
	 **/
	@Deprecated
	/** The maximum backoff time between each describeStream attempt. */
	public static final String STREAM_DESCRIBE_BACKOFF_MAX = "flink.stream.describe.backoff.max";

	/**
	 * Deprecated key.
	 *
	 * @deprecated Use {@link ConsumerConfigConstants#LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT} instead
	 **/
	@Deprecated
	/** The power constant for exponential backoff between each describeStream attempt. */
	public static final String STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT = "flink.stream.describe.backoff.expconst";

	/** The base backoff time between each listShards attempt. */
	public static final String LIST_SHARDS_BACKOFF_BASE = "flink.list.shards.backoff.base";

	/** The maximum backoff time between each listShards attempt. */
	public static final String LIST_SHARDS_BACKOFF_MAX = "flink.list.shards.backoff.max";

	/** The power constant for exponential backoff between each listShards attempt. */
	public static final String LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT = "flink.list.shards.backoff.expconst";

	/** The maximum number of records to try to get each time we fetch records from a AWS Kinesis shard. */
	public static final String SHARD_GETRECORDS_MAX = "flink.shard.getrecords.maxrecordcount";

	/** The maximum number of getRecords attempts if we get ProvisionedThroughputExceededException. */
	public static final String SHARD_GETRECORDS_RETRIES = "flink.shard.getrecords.maxretries";

	/** The base backoff time between getRecords attempts if we get a ProvisionedThroughputExceededException. */
	public static final String SHARD_GETRECORDS_BACKOFF_BASE = "flink.shard.getrecords.backoff.base";

	/** The maximum backoff time between getRecords attempts if we get a ProvisionedThroughputExceededException. */
	public static final String SHARD_GETRECORDS_BACKOFF_MAX = "flink.shard.getrecords.backoff.max";

	/** The power constant for exponential backoff between each getRecords attempt. */
	public static final String SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT = "flink.shard.getrecords.backoff.expconst";

	/** The interval between each getRecords request to a AWS Kinesis shard in milliseconds. */
	public static final String SHARD_GETRECORDS_INTERVAL_MILLIS = "flink.shard.getrecords.intervalmillis";

	/** The maximum number of getShardIterator attempts if we get ProvisionedThroughputExceededException. */
	public static final String SHARD_GETITERATOR_RETRIES = "flink.shard.getiterator.maxretries";

	/** The base backoff time between getShardIterator attempts if we get a ProvisionedThroughputExceededException. */
	public static final String SHARD_GETITERATOR_BACKOFF_BASE = "flink.shard.getiterator.backoff.base";

	/** The maximum backoff time between getShardIterator attempts if we get a ProvisionedThroughputExceededException. */
	public static final String SHARD_GETITERATOR_BACKOFF_MAX = "flink.shard.getiterator.backoff.max";

	/** The power constant for exponential backoff between each getShardIterator attempt. */
	public static final String SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT = "flink.shard.getiterator.backoff.expconst";

	/** The interval between each attempt to discover new shards. */
	public static final String SHARD_DISCOVERY_INTERVAL_MILLIS = "flink.shard.discovery.intervalmillis";

	/** The config to turn on adaptive reads from a shard. */
	public static final String SHARD_USE_ADAPTIVE_READS = "flink.shard.adaptivereads";


	// ------------------------------------------------------------------------
	//  Default values for consumer configuration
	// ------------------------------------------------------------------------

	public static final String DEFAULT_STREAM_INITIAL_POSITION = InitialPosition.LATEST.toString();

	public static final String DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

	@Deprecated
	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE = 1000L;

	@Deprecated
	public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX = 5000L;

	@Deprecated
	public static final double DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	public static final long DEFAULT_LIST_SHARDS_BACKOFF_BASE = 1000L;

	public static final long DEFAULT_LIST_SHARDS_BACKOFF_MAX = 5000L;

	public static final double DEFAULT_LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	public static final int DEFAULT_SHARD_GETRECORDS_MAX = 10000;

	public static final int DEFAULT_SHARD_GETRECORDS_RETRIES = 3;

	public static final long DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE = 300L;

	public static final long DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX = 1000L;

	public static final double DEFAULT_SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	public static final long DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS = 200L;

	public static final int DEFAULT_SHARD_GETITERATOR_RETRIES = 3;

	public static final long DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE = 300L;

	public static final long DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX = 1000L;

	public static final double DEFAULT_SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

	public static final long DEFAULT_SHARD_DISCOVERY_INTERVAL_MILLIS = 10000L;

	public static final boolean DEFAULT_SHARD_USE_ADAPTIVE_READS = false;

	/**
	 * To avoid shard iterator expires in {@link ShardConsumer}s, the value for the configured
	 * getRecords interval can not exceed 5 minutes, which is the expire time for retrieved iterators.
	 */
	public static final long MAX_SHARD_GETRECORDS_INTERVAL_MILLIS = 300000L;

}
