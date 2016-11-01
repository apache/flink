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
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.executiongraph.IOMetrics;

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

	private final MetricGroup buffers;

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

		this.buffers = addGroup("buffers");
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

	public MetricGroup getBuffersGroup() {
		return buffers;
	}

	// ------------------------------------------------------------------------
	//  metrics of Buffers group
	// ------------------------------------------------------------------------

	/**
	 * Input received buffers gauge of a task
	 */
	public static final class InputBuffersGauge implements Gauge<Integer> {

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
	 * Output produced buffers gauge of a task
	 */
	public static final class OutputBuffersGauge implements Gauge<Integer> {

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

	/**
	 * Input buffer pool usage gauge of a task
	 */
	public static final class InputBufferPoolUsageGauge implements Gauge<Float> {

		private final Task task;

		public InputBufferPoolUsageGauge(Task task) {
			this.task = task;
		}

		@Override
		public Float getValue() {
			int availableBuffers = 0;
			int bufferPoolSize = 0;

			for (SingleInputGate inputGate : task.getAllInputGates()) {
				availableBuffers += inputGate.getBufferPool().getNumberOfAvailableMemorySegments();
				bufferPoolSize += inputGate.getBufferPool().getNumBuffers();
			}

			if (bufferPoolSize != 0) {
				return ((float)(bufferPoolSize - availableBuffers)) / bufferPoolSize;
			} else {
				return 0.0f;
			}
		}
	}

	/**
	 * Output buffer pool usage gauge of a task
	 */
	public static final class OutputBufferPoolUsageGauge implements Gauge<Float> {

		private final Task task;

		public OutputBufferPoolUsageGauge(Task task) {
			this.task = task;
		}

		@Override
		public Float getValue() {
			int availableBuffers = 0;
			int bufferPoolSize = 0;

			for (ResultPartition resultPartition : task.getProducedPartitions()) {
				availableBuffers += resultPartition.getBufferPool().getNumberOfAvailableMemorySegments();
				bufferPoolSize += resultPartition.getBufferPool().getNumBuffers();
			}

			if (bufferPoolSize != 0) {
				return ((float)(bufferPoolSize - availableBuffers)) / bufferPoolSize;
			} else {
				return 0.0f;
			}
		}
	}
}
