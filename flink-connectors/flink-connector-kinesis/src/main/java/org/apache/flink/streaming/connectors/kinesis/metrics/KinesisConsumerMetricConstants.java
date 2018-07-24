/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.metrics;

import org.apache.flink.annotation.Internal;

/**
 * A collection of consumer metric related constant names.
 *
 * <p>The names must not be changed, as that would break backwards compatibility for the consumer metrics.
 */
@Internal
public class KinesisConsumerMetricConstants {

	public static final String KINESIS_CONSUMER_METRICS_GROUP = "KinesisConsumer";

	public static final String STREAM_METRICS_GROUP = "stream";
	public static final String SHARD_METRICS_GROUP = "shardId";

	public static final String MILLIS_BEHIND_LATEST_GAUGE = "millisBehindLatest";
	public static final String SLEEP_TIME_MILLIS = "sleepTimeMillis";
	public static final String MAX_RECORDS_PER_FETCH = "maxNumberOfRecordsPerFetch";
	public static final String NUM_AGGREGATED_RECORDS_PER_FETCH = "numberOfAggregatedRecordsPerFetch";
	public static final String NUM_DEAGGREGATED_RECORDS_PER_FETCH = "numberOfDeaggregatedRecordsPerFetch";
	public static final String AVG_RECORD_SIZE_BYTES = "averageRecordSizeBytes";
	public static final String RUNTIME_LOOP_NANOS = "runLoopTimeNanos";
	public static final String LOOP_FREQUENCY_HZ = "loopFrequencyHz";
	public static final String BYTES_PER_READ = "bytesRequestedPerFetch";

}
