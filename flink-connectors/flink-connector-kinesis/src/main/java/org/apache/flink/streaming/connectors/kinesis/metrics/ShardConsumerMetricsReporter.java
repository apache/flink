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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumer;

/**
 * A container for {@link ShardConsumer}s to report metric values.
 */
@Internal
public class ShardConsumerMetricsReporter {

	private volatile long millisBehindLatest = -1;
	private volatile long averageRecordSizeBytes = 0L;
	private volatile int numberOfAggregatedRecords = 0;
	private volatile int numberOfDeaggregatedRecords = 0;

	public ShardConsumerMetricsReporter(final MetricGroup metricGroup) {
		metricGroup.gauge(KinesisConsumerMetricConstants.MILLIS_BEHIND_LATEST_GAUGE, this::getMillisBehindLatest);
		metricGroup.gauge(KinesisConsumerMetricConstants.NUM_AGGREGATED_RECORDS_PER_FETCH, this::getNumberOfAggregatedRecords);
		metricGroup.gauge(KinesisConsumerMetricConstants.NUM_DEAGGREGATED_RECORDS_PER_FETCH, this::getNumberOfDeaggregatedRecords);
		metricGroup.gauge(KinesisConsumerMetricConstants.AVG_RECORD_SIZE_BYTES, this::getAverageRecordSizeBytes);
	}

	public long getMillisBehindLatest() {
		return millisBehindLatest;
	}

	public void setMillisBehindLatest(long millisBehindLatest) {
		this.millisBehindLatest = millisBehindLatest;
	}

	public long getAverageRecordSizeBytes() {
		return averageRecordSizeBytes;
	}

	public void setAverageRecordSizeBytes(long averageRecordSizeBytes) {
		this.averageRecordSizeBytes = averageRecordSizeBytes;
	}

	public int getNumberOfAggregatedRecords() {
		return numberOfAggregatedRecords;
	}

	public void setNumberOfAggregatedRecords(int numberOfAggregatedRecords) {
		this.numberOfAggregatedRecords = numberOfAggregatedRecords;
	}

	public int getNumberOfDeaggregatedRecords() {
		return numberOfDeaggregatedRecords;
	}

	public void setNumberOfDeaggregatedRecords(int numberOfDeaggregatedRecords) {
		this.numberOfDeaggregatedRecords = numberOfDeaggregatedRecords;
	}

}
