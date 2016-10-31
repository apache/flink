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
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.SimpleCounter;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent operator metric group.
 */
public class OperatorIOMetricGroup extends ProxyMetricGroup<OperatorMetricGroup> {

	private final Counter numRecordsIn;
	private final Counter numRecordsOut;

	private final Meter numRecordsInRate;
	private final Meter numRecordsOutRate;

	private final LatencyHistogram recordProcLatency;

	public OperatorIOMetricGroup(OperatorMetricGroup parentMetricGroup) {
		super(parentMetricGroup);
		numRecordsIn = parentMetricGroup.counter("numRecordsIn");
		numRecordsOut = parentMetricGroup.counter("numRecordsOut");
		numRecordsInRate = parentMetricGroup.meter("numRecordsInPerSecond", new MeterView(numRecordsIn, 60));
		numRecordsOutRate = parentMetricGroup.meter("numRecordsOutPerSecond", new MeterView(numRecordsOut, 60));

		recordProcLatency = parentMetricGroup.histogram("recordProcLatency", new LatencyHistogram(true));
		if (recordProcLatency.getLatencyAccumulateCounter() != null) {
			parentMetricGroup.meter("recordProcUsage", new MeterView(recordProcLatency.getLatencyAccumulateCounter(), 30));
		}
	}

	public Counter getNumRecordsInCounter() {
		return numRecordsIn;
	}

	public Counter getNumRecordsOutCounter() {
		return numRecordsOut;
	}

	public Meter getNumRecordsInRateMeter() {
		return numRecordsInRate;
	}

	public Meter getNumRecordsOutRate() {
		return numRecordsOutRate;
	}

	public Histogram getRecordProcLatency() {
		return recordProcLatency;
	}

	/**
	 * Causes the containing task to use this operators input record counter.
	 */
	public void reuseInputMetricsForTask() {
		TaskIOMetricGroup taskIO = parentMetricGroup.parent().getIOMetricGroup();
		taskIO.reuseRecordsInputCounter(this.numRecordsIn);
		
	}

	/**
	 * Causes the containing task to use this operators output record counter.
	 */
	public void reuseOutputMetricsForTask() {
		TaskIOMetricGroup taskIO = parentMetricGroup.parent().getIOMetricGroup();
		taskIO.reuseRecordsOutputCounter(this.numRecordsOut);
	}

	/**
	 * Latency measurement, it's a Histogram with given history size or Reservoir.
	 * A latency Accumulate will be involved if accumulate enabled
	 */
	private static class LatencyHistogram implements Histogram {

		// default history size
		private static final int DEFAULT_HISTORY_SIZE = 128;

		// used for conversion of millisecond and nanosecond
		private static final double NANOSECONDS_PER_MILLISECOND = 1000000.0D;

		// a reservoir for history data
		private final Reservoir latencyReservoir;

		// accumulate latency for measurement processing time per second
		private final SimpleCounter latencyAccumulateCounter;

		public LatencyHistogram() {
			this(DEFAULT_HISTORY_SIZE);
		}

		public LatencyHistogram(int historySize) {
			//default disable accumulate
			this(historySize, false);
		}

		public LatencyHistogram(Reservoir latencyReservoir) {
			//default disable accumulate
			this(latencyReservoir, false);
		}

		public LatencyHistogram(boolean enableAccumulate) {
			this(DEFAULT_HISTORY_SIZE, enableAccumulate);
		}

		public LatencyHistogram(int historySize, boolean enableAccumulate) {
			//default with Sliding Window Reservoir
			this(new SlidingWindowReservoir(historySize), enableAccumulate);
		}

		public LatencyHistogram(Reservoir latencyReservoir, boolean enableAccumulate) {
			this.latencyReservoir = latencyReservoir;
			if (enableAccumulate) {
				latencyAccumulateCounter = new SimpleCounter();
			} else {
				latencyAccumulateCounter = null;
			}
		}

		@Override
		public void update(long nanosecond) {
			latencyReservoir.update(nanosecond);
			if (latencyAccumulateCounter != null) {
				latencyAccumulateCounter.inc((long)(nanosecond / NANOSECONDS_PER_MILLISECOND));
			}
		}

		@Override
		public long getCount() {
			return latencyReservoir.size();
		}

		@Override
		public HistogramStatistics getStatistics() {
			return new LatencyHistogramStatistics(latencyReservoir.getSnapshot());
		}

		public Counter getLatencyAccumulateCounter() {
			return latencyAccumulateCounter;
		}


		private static class LatencyHistogramStatistics extends HistogramStatistics {

			private final Snapshot latencySnapshot;

			public LatencyHistogramStatistics(Snapshot latencySnapshot) {
				this.latencySnapshot = latencySnapshot;
			}

			@Override
			public double getQuantile(double quantile) {
				return latencySnapshot.getValue(quantile) / NANOSECONDS_PER_MILLISECOND;
			}

			@Override
			public long[] getValues() {
				long [] nanos = latencySnapshot.getValues();
				long [] millis = new long[nanos.length];

				for (int i = 0; i < nanos.length; ++i) {
					millis[i] = (long)(nanos[i] / NANOSECONDS_PER_MILLISECOND);
				}

				return millis;
			}

			@Override
			public int size() {
				return latencySnapshot.size();
			}

			@Override
			public double getMean() {
				return latencySnapshot.getMean() / NANOSECONDS_PER_MILLISECOND;
			}

			@Override
			public double getStdDev() {
				return latencySnapshot.getStdDev() / NANOSECONDS_PER_MILLISECOND;
			}

			@Override
			public long getMax() {
				return (long)(latencySnapshot.getMax() / NANOSECONDS_PER_MILLISECOND);
			}

			@Override
			public long getMin() {
				return (long)(latencySnapshot.getMin() / NANOSECONDS_PER_MILLISECOND);
			}
		}
	}
}
