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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class TaskIOMetricGroup extends ProxyMetricGroup<TaskMetricGroup> {

	private final Counter numBytesOut;
	private final Counter numBytesInLocal;
	private final Counter numBytesInRemote;
	private final SumCounter numRecordsIn;
	private final SumCounter numRecordsOut;

	private final Meter numBytesInRateLocal;
	private final Meter numBytesInRateRemote;
	private final Meter numBytesOutRate;
	private final Meter numRecordsInRate;
	private final Meter numRecordsOutRate;

	private final LatencyHistogram recordProcessLatency;

	public TaskIOMetricGroup(TaskMetricGroup parent) {
		super(parent);

		this.numBytesOut = counter("numBytesOut");
		this.numBytesInLocal = counter("numBytesInLocal");
		this.numBytesInRemote = counter("numBytesInRemote");
		this.numBytesOutRate = meter("numBytesOutPerSecond", new MeterView(numBytesOut, 60));
		this.numBytesInRateLocal = meter("numBytesInLocalPerSecond", new MeterView(numBytesInLocal, 60));
		this.numBytesInRateRemote = meter("numBytesInRemotePerSecond", new MeterView(numBytesInRemote, 60));
		this.numRecordsIn = counter("numRecordsIn", new SumCounter());
		this.numRecordsOut = counter("numRecordsOut", new SumCounter());
		this.numRecordsInRate = meter("numRecordsInPerSecond", new MeterView(numRecordsIn, 60));
		this.numRecordsOutRate = meter("numRecordsOutPerSecond", new MeterView(numRecordsOut, 60));
		this.recordProcessLatency = histogram("recordProcessLatency", new LatencyHistogram(true));
		if (recordProcessLatency.getLatencyAccumulateCounter() != null) {
			meter("recordProcTimeProportion", new MeterView(recordProcessLatency.getLatencyAccumulateCounter(), 60));
		}
	}

	public IOMetrics createSnapshot() {
		return new IOMetrics(numRecordsInRate, numRecordsOutRate, numBytesInRateLocal, numBytesInRateRemote, numBytesOutRate);
	}

	// ============================================================================================
	// Getters
	// ============================================================================================
	public Counter getNumBytesOutCounter() {
		return numBytesOut;
	}

	public Counter getNumBytesInLocalCounter() {
		return numBytesInLocal;
	}

	public Counter getNumBytesInRemoteCounter() {
		return numBytesInRemote;
	}

	public Counter getNumRecordsInCounter() {
		return numRecordsIn;
	}

	public Counter getNumRecordsOutCounter() {
		return numRecordsOut;
	}

	public Meter getNumBytesInLocalRateMeter() {
		return numBytesInRateLocal;
	}

	public Meter getNumBytesInRemoteRateMeter() {
		return numBytesInRateRemote;
	}

	public Meter getNumBytesOutRateMeter() {
		return numBytesOutRate;
	}

	public Histogram getRecordProcessLatency() {
		return recordProcessLatency;
	}

	// ============================================================================================
	// Buffer metrics
	// ============================================================================================

	/**
	 * Initialize Buffer Metrics for a task
	 */
	public void initializeBufferMetrics(Task task) {
		final MetricGroup buffers = addGroup("buffers");
		buffers.gauge("inputQueueLength", new InputBuffersGauge(task));
		buffers.gauge("outputQueueLength", new OutputBuffersGauge(task));
		buffers.gauge("inPoolUsage", new InputBufferPoolUsageGauge(task));
		buffers.gauge("outPoolUsage", new OutputBufferPoolUsageGauge(task));
	}

	/**
	 * Gauge measuring the number of queued input buffers of a task.
	 */
	private static final class InputBuffersGauge implements Gauge<Integer> {

		private final Task task;

		public InputBuffersGauge(Task task) {
			this.task = task;
		}

		@Override
		public Integer getValue() {
			int totalBuffers = 0;

			for (SingleInputGate inputGate : task.getAllInputGates()) {
				totalBuffers += inputGate.getNumberOfQueuedBuffers();
			}

			return totalBuffers;
		}
	}

	/**
	 * Gauge measuring the number of queued output buffers of a task.
	 */
	private static final class OutputBuffersGauge implements Gauge<Integer> {

		private final Task task;

		public OutputBuffersGauge(Task task) {
			this.task = task;
		}

		@Override
		public Integer getValue() {
			int totalBuffers = 0;

			for (ResultPartition producedPartition : task.getProducedPartitions()) {
				totalBuffers += producedPartition.getNumberOfQueuedBuffers();
			}

			return totalBuffers;
		}
	}

	/**
	 * Gauge measuring the input buffer pool usage gauge of a task.
	 */
	private static final class InputBufferPoolUsageGauge implements Gauge<Float> {

		private final Task task;

		public InputBufferPoolUsageGauge(Task task) {
			this.task = task;
		}

		@Override
		public Float getValue() {
			int usedBuffers = 0;
			int bufferPoolSize = 0;

			for (SingleInputGate inputGate : task.getAllInputGates()) {
				usedBuffers += inputGate.getBufferPool().getNumberOfUsedBuffers();
				bufferPoolSize += inputGate.getBufferPool().getNumBuffers();
			}

			if (bufferPoolSize != 0) {
				return ((float) usedBuffers) / bufferPoolSize;
			} else {
				return 0.0f;
			}
		}
	}

	/**
	 * Gauge measuring the output buffer pool usage gauge of a task.
	 */
	private static final class OutputBufferPoolUsageGauge implements Gauge<Float> {

		private final Task task;

		public OutputBufferPoolUsageGauge(Task task) {
			this.task = task;
		}

		@Override
		public Float getValue() {
			int usedBuffers = 0;
			int bufferPoolSize = 0;

			for (ResultPartition resultPartition : task.getProducedPartitions()) {
				usedBuffers += resultPartition.getBufferPool().getNumberOfUsedBuffers();
				bufferPoolSize += resultPartition.getBufferPool().getNumBuffers();
			}

			if (bufferPoolSize != 0) {
				return ((float) usedBuffers) / bufferPoolSize;
			} else {
				return 0.0f;
			}
		}
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

	// ============================================================================================
	// Latency metrics
	// ============================================================================================

	/**
	 * Histogram measuring the record processing latency of a task.
	 * It's element processing time of a task. But an element emitting time for Source Task.
	 * It could be given a history size or a Reservoir when construct.
	 * A latency accumulate will be activated if accumulate enabled
	 */
	private static class LatencyHistogram implements Histogram {

		private static final int DEFAULT_HISTORY_SIZE = 128;

		// conversion of millisecond and nanosecond
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
