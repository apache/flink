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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class TaskIOMetricGroup extends ProxyMetricGroup<TaskMetricGroup> {

	private final Counter numBytesOut;
	private final Counter numBytesInLocal;
	private final Counter numBytesInRemote;

	private final MetricGroup buffers;

	public TaskIOMetricGroup(TaskMetricGroup parent) {
		super(parent);

		this.numBytesOut = counter("numBytesOut");
		this.numBytesInLocal = counter("numBytesInLocal");
		this.numBytesInRemote = counter("numBytesInRemote");

		this.buffers = addGroup("Buffers");
	}

	public Counter getNumBytesOutCounter() {
		return numBytesOut;
	}

	public Counter getNumBytesInLocalCounter() {
		return numBytesInLocal;
	}

	public Counter getNumBytesInRemoteCounter() {
		return numBytesInRemote;
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
	public static class InputBuffersGauge implements Gauge<Integer> {

		private final Task task;

		public InputBuffersGauge(Task task) {
			this.task = task;
		}

		@Override
		public Integer getValue() {
			int totalBuffers = 0;

			for(SingleInputGate inputGate: task.getAllInputGates()) {
				totalBuffers += inputGate.getNumberOfQueuedBuffers();
			}

			return totalBuffers;
		}
	}

	/**
	 * Output produced buffers gauge of a task
	 */
	public static class OutputBuffersGauge implements Gauge<Integer> {

		private final Task task;

		public OutputBuffersGauge(Task task) {
			this.task = task;
		}

		@Override
		public Integer getValue() {
			int totalBuffers = 0;

			for(ResultPartition producedPartition: task.getProducedPartitions()) {
				totalBuffers += producedPartition.getNumberOfQueuedBuffers();
			}

			return totalBuffers;
		}
	}

	/**
	 * Input buffer pool usage gauge of a task
	 */
	public static class InputBufferPoolUsageGauge implements Gauge<Float> {

		private final Task task;

		public InputBufferPoolUsageGauge(Task task) {
			this.task = task;
		}

		@Override
		public Float getValue() {
			int availableBuffers = 0;
			int bufferPoolSize = 0;

			for(SingleInputGate inputGate: task.getAllInputGates()) {
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
	public static class OutputBufferPoolUsageGauge implements Gauge<Float> {

		private final Task task;

		public OutputBufferPoolUsageGauge(Task task) {
			this.task = task;
		}

		@Override
		public Float getValue() {
			int availableBuffers = 0;
			int bufferPoolSize = 0;

			for(ResultPartition resultPartition: task.getProducedPartitions()) {
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
