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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Mock {@link IndexedInputGate}.
 */
public class MockIndexedInputGate extends IndexedInputGate {

	public MockIndexedInputGate() {
	}

	@Override
	public void setup() {
	}

	@Override
	public CompletableFuture<?> readRecoveredState(ExecutorService executor, ChannelStateReader reader) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void requestPartitions() {
	}

	@Override
	public void resumeConsumption(int channelIndex) {
	}

	@Override
	public int getNumberOfInputChannels() {
		return 1;
	}

	@Override
	public InputChannel getChannel(int channelIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFinished() {
		return false;
	}

	@Override
	public Optional<BufferOrEvent> getNext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<BufferOrEvent> pollNext() {
		return getNext();
	}

	@Override
	public void sendTaskEvent(TaskEvent event) {
	}

	@Override
	public void close() {
	}

	@Override
	public void registerBufferReceivedListener(BufferReceivedListener listener) {
	}

	@Override
	public int getGateIndex() {
		return 0;
	}
}
