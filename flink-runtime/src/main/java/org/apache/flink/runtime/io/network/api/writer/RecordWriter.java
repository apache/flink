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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.util.Random;

/**
 * A record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flushAll()} after
 * all records have been written with {@link #emit(T)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T> {

	protected final ResultPartitionWriter targetPartition;

	private final ChannelSelector<T> channelSelector;

	private final int[] allChannels;

	private final int numChannels;

	private final Random rng = new XORShiftRandom();

	private final boolean flushAlways;

	private final boolean isBroadcastSelector;

	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>(), false);
	}

	public RecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector<T> channelSelector,
			boolean isBroadcastSelector) {
		this(writer, channelSelector, isBroadcastSelector, false);
	}

	public RecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector<T> channelSelector,
			boolean isBroadcastSelector,
			boolean flushAlways) {
		this.isBroadcastSelector = isBroadcastSelector;
		this.flushAlways = flushAlways;
		this.targetPartition = writer;
		this.channelSelector = channelSelector;
		this.numChannels = writer.getNumberOfSubpartitions();
		this.allChannels = new int[numChannels];
		for (int i = 0; i < numChannels; ++i) {
			allChannels[i] = i;
		}
	}

	public void emit(T record) throws IOException, InterruptedException {
		if (isBroadcastSelector) {
			targetPartition.emitRecord(record, allChannels, isBroadcastSelector, flushAlways);
		} else {
			int targetChannel = channelSelector.selectChannel(record, numChannels);
			targetPartition.emitRecord(record, targetChannel, isBroadcastSelector, flushAlways);
		}
	}

	/**
	 * This is used to broadcast Streaming Watermarks in-band with records. This ignores
	 * the {@link ChannelSelector}.
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		targetPartition.emitRecord(record, allChannels, isBroadcastSelector, flushAlways);
	}

	/**
	 * This is used to send LatencyMarks to a random target channel.
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		int targetChannel = rng.nextInt(numChannels);
		targetPartition.emitRecord(record, targetChannel, isBroadcastSelector, flushAlways);
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		targetPartition.broadcastEvent(event, flushAlways);
	}

	public void flushAll() {
		targetPartition.flushAll();
	}

	public void clearBuffers() {
		targetPartition.clearBuffers();
	}

	/**
	 * Sets the metric group for this RecordWriter.
     */
	public void setMetricGroup(TaskIOMetricGroup metrics, boolean enableTracingMetrics, int tracingMetricsInterval) {
		targetPartition.setMetricGroup(metrics, enableTracingMetrics, tracingMetricsInterval);
	}
}
