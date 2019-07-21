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
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
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
public class InputGateWithMetrics extends InputGate {

	private final InputGate inputGate;

	private final Counter numBytesIn;

	public InputGateWithMetrics(InputGate inputGate, Counter numBytesIn) {
		this.inputGate = checkNotNull(inputGate);
		this.numBytesIn = checkNotNull(numBytesIn);
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		return inputGate.isAvailable();
	}

	@Override
	public int getNumberOfInputChannels() {
		return inputGate.getNumberOfInputChannels();
	}

	@Override
	public boolean isFinished() {
		return inputGate.isFinished();
	}

	@Override
	public void setup() throws IOException, InterruptedException {
		inputGate.setup();
	}

	@Override
	public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
		return updateMetrics(inputGate.getNext());
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
		return updateMetrics(inputGate.pollNext());
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		inputGate.sendTaskEvent(event);
	}

	@Override
	public void close() throws Exception {
		inputGate.close();
	}

	private Optional<BufferOrEvent> updateMetrics(Optional<BufferOrEvent> bufferOrEvent) {
		bufferOrEvent.ifPresent(b -> numBytesIn.inc(b.getSize()));
		return bufferOrEvent;
	}
}
