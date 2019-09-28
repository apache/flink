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
import org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumer;

/**
 * A container for {@link ShardConsumer}s to report metric values.
 */
@Internal
public class ShardMetricsReporter {

	private volatile long millisBehindLatest = -1;
	private volatile double loopFrequencyHz = 0.0;
	private volatile double bytesPerRead = 0.0;
	private volatile long runLoopTimeNanos = 0L;
	private volatile long averageRecordSizeBytes = 0L;
	private volatile long sleepTimeMillis = 0L;
	private volatile int numberOfAggregatedRecords = 0;
	private volatile int numberOfDeaggregatedRecords = 0;
	private volatile int maxNumberOfRecordsPerFetch = 0;

	public long getMillisBehindLatest() {
		return millisBehindLatest;
	}

	public void setMillisBehindLatest(long millisBehindLatest) {
		this.millisBehindLatest = millisBehindLatest;
	}

	public double getLoopFrequencyHz() {
		return loopFrequencyHz;
	}

	public void setLoopFrequencyHz(double loopFrequencyHz) {
		this.loopFrequencyHz = loopFrequencyHz;
	}

	public double getBytesPerRead() {
		return bytesPerRead;
	}

	public void setBytesPerRead(double bytesPerRead) {
		this.bytesPerRead = bytesPerRead;
	}

	public long getRunLoopTimeNanos() {
		return runLoopTimeNanos;
	}

	public void setRunLoopTimeNanos(long runLoopTimeNanos) {
		this.runLoopTimeNanos = runLoopTimeNanos;
	}

	public long getAverageRecordSizeBytes() {
		return averageRecordSizeBytes;
	}

	public void setAverageRecordSizeBytes(long averageRecordSizeBytes) {
		this.averageRecordSizeBytes = averageRecordSizeBytes;
	}

	public long getSleepTimeMillis() {
		return sleepTimeMillis;
	}

	public void setSleepTimeMillis(long sleepTimeMillis) {
		this.sleepTimeMillis = sleepTimeMillis;
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

	public int getMaxNumberOfRecordsPerFetch() {
		return maxNumberOfRecordsPerFetch;
	}

	public void setMaxNumberOfRecordsPerFetch(int maxNumberOfRecordsPerFetch) {
		this.maxNumberOfRecordsPerFetch = maxNumberOfRecordsPerFetch;
	}

}
