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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.metrics.MetricNames;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class TaskIOMetricGroup extends ProxyMetricGroup<TaskMetricGroup> {

	private final Counter numBytesIn;
	private final Counter numBytesOut;
	private final SumCounter numRecordsIn;
	private final SumCounter numRecordsOut;
	private final Counter numBuffersOut;

	private final Meter numBytesInRate;
	private final Meter numBytesOutRate;
	private final Meter numRecordsInRate;
	private final Meter numRecordsOutRate;
	private final Meter numBuffersOutRate;
	private final Meter idleTimePerSecond;

	public TaskIOMetricGroup(TaskMetricGroup parent) {
		super(parent);

		this.numBytesIn = counter(MetricNames.IO_NUM_BYTES_IN);
		this.numBytesOut = counter(MetricNames.IO_NUM_BYTES_OUT);
		this.numBytesInRate = meter(MetricNames.IO_NUM_BYTES_IN_RATE, new MeterView(numBytesIn));
		this.numBytesOutRate = meter(MetricNames.IO_NUM_BYTES_OUT_RATE, new MeterView(numBytesOut));

		this.numRecordsIn = counter(MetricNames.IO_NUM_RECORDS_IN, new SumCounter());
		this.numRecordsOut = counter(MetricNames.IO_NUM_RECORDS_OUT, new SumCounter());
		this.numRecordsInRate = meter(MetricNames.IO_NUM_RECORDS_IN_RATE, new MeterView(numRecordsIn));
		this.numRecordsOutRate = meter(MetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOut));

		this.numBuffersOut = counter(MetricNames.IO_NUM_BUFFERS_OUT);
		this.numBuffersOutRate = meter(MetricNames.IO_NUM_BUFFERS_OUT_RATE, new MeterView(numBuffersOut));

		this.idleTimePerSecond = meter(MetricNames.TASK_IDLE_TIME, new MeterView(new SimpleCounter()));
	}

	public IOMetrics createSnapshot() {
		return new IOMetrics(numRecordsInRate, numRecordsOutRate, numBytesInRate, numBytesOutRate);
	}

	// ============================================================================================
	// Getters
	// ============================================================================================

	public Counter getNumBytesInCounter() {
		return numBytesIn;
	}

	public Counter getNumBytesOutCounter() {
		return numBytesOut;
	}

	public Counter getNumRecordsInCounter() {
		return numRecordsIn;
	}

	public Counter getNumRecordsOutCounter() {
		return numRecordsOut;
	}

	public Counter getNumBuffersOutCounter() {
		return numBuffersOut;
	}

	public Meter getIdleTimeMsPerSecond() {
		return idleTimePerSecond;
	}

	// ============================================================================================
	// Metric Reuse
	// ============================================================================================
	public void reuseRecordsInputCounter(Counter numRecordsInCounter) {
		this.numRecordsIn.addCounter(numRecordsInCounter);
	}

	public void reuseRecordsOutputCounter(Counter numRecordsOutCounter) {
		this.numRecordsOut.addCounter(numRecordsOutCounter);
	}

	/**
	 * A {@link SimpleCounter} that can contain other {@link Counter}s. A call to {@link SumCounter#getCount()} returns
	 * the sum of this counters and all contained counters.
	 */
	private static class SumCounter extends SimpleCounter {
		private final List<Counter> internalCounters = new ArrayList<>();

		SumCounter() {
		}

		public void addCounter(Counter toAdd) {
			internalCounters.add(toAdd);
		}

		@Override
		public long getCount() {
			long sum = super.getCount();
			for (Counter counter : internalCounters) {
				sum += counter.getCount();
			}
			return sum;
		}
	}
}
