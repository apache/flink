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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class wraps {@link InputGate} provided by shuffle service and it is mainly
 * used for increasing general input metrics from {@link TaskIOMetricGroup}.
 */
public class InputGateWithMetrics extends IndexedInputGate {

	private final IndexedInputGate inputGate;

	private final Counter numBytesIn;

	public InputGateWithMetrics(IndexedInputGate inputGate, Counter numBytesIn) {
		this.inputGate = checkNotNull(inputGate);
		this.numBytesIn = checkNotNull(numBytesIn);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return inputGate.getAvailableFuture();
	}

	@Override
	public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
		inputGate.resumeConsumption(channelInfo);
	}

	@Override
	public int getNumberOfInputChannels() {
		return inputGate.getNumberOfInputChannels();
	}

	@Override
	public InputChannel getChannel(int channelIndex) {
		return inputGate.getChannel(channelIndex);
	}

	@Override
	public int getGateIndex() {
		return inputGate.getGateIndex();
	}

	@Override
	public boolean isFinished() {
		return inputGate.isFinished();
	}

	@Override
	public void setup() throws IOException {
		inputGate.setup();
	}

	@Override
	public CompletableFuture<Void> getStateConsumedFuture() {
		return inputGate.getStateConsumedFuture();
	}

	@Override
	public void requestPartitions() throws IOException {
		inputGate.requestPartitions();
	}

	@Override
	public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
		inputGate.setChannelStateWriter(channelStateWriter);
	}

	@Override
	public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
		return inputGate.getNext().map(this::updateMetrics);
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
		return inputGate.pollNext().map(this::updateMetrics);
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		inputGate.sendTaskEvent(event);
	}

	@Override
	public void close() throws Exception {
		inputGate.close();
	}

	@Override
	public CompletableFuture<?> getPriorityEventAvailableFuture() {
		return inputGate.getPriorityEventAvailableFuture();
	}

	@Override
	public void finishReadRecoveredState() throws IOException {
		inputGate.finishReadRecoveredState();
	}

	private BufferOrEvent updateMetrics(BufferOrEvent bufferOrEvent) {
		numBytesIn.inc(bufferOrEvent.getSize());
		return bufferOrEvent;
	}
}
