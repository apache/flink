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

import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.util.event.EventHandler;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;

/**
 * A buffer-oriented runtime result writer.
 * <p/>
 * The {@link BufferWriter} is the runtime API for producing results. It
 * supports two kinds of data to be sent: buffers and events.
 * <p/>
 * <strong>Important</strong>: it is necessary to call {@link #finish()} after
 * all data is written to signal that all data has been produced.
 */
public class BufferWriter implements EventListener<TaskEvent> {

	private final IntermediateResultPartition partition;

	private final EventHandler<TaskEvent> taskEventHandler = new EventHandler<TaskEvent>();

	private boolean isFinished;

	public BufferWriter(IntermediateResultPartition partition) {
		this.partition = partition;
	}

	// ------------------------------------------------------------------------
	// Attributes
	// ------------------------------------------------------------------------

	public IntermediateResultPartitionID getPartitionId() {
		return partition.getPartitionId();
	}

	// TODO remove this method (all calls should be mediated through the buffer writer)
	public IntermediateResultPartition getPartition() {
		return partition;
	}

	public BufferProvider getBufferProvider() {
		return partition.getBufferProvider();
	}

	public int getNumberOfOutputChannels() {
		return partition.getNumberOfQueues();
	}

	// ------------------------------------------------------------------------
	// Data processing
	// ------------------------------------------------------------------------

	public void writeBuffer(Buffer buffer, int targetChannel) throws IOException {
		partition.add(new BufferOrEvent(buffer), targetChannel);
	}

	public void writeEvent(AbstractEvent event, int targetChannel) throws IOException {
		partition.add(new BufferOrEvent(event), targetChannel);
	}

	public void writeBufferToAllChannels(Buffer buffer) throws IOException {
		partition.addToAllPartitions(new BufferOrEvent(buffer));
	}

	public void writeEventToAllChannels(AbstractEvent event) throws IOException {
		partition.addToAllPartitions(new BufferOrEvent(event));
	}

	public void writeEndOfSuperstep() throws IOException {
		partition.addToAllPartitions(new BufferOrEvent(EndOfSuperstepEvent.INSTANCE));
	}

	public void finish() throws IOException, InterruptedException {
		partition.finish();
	}

	public boolean isFinished() {
		return partition.isFinished();
	}

	// ------------------------------------------------------------------------
	// Event handling
	// ------------------------------------------------------------------------

	public EventHandler<TaskEvent> getTaskEventHandler() {
		return taskEventHandler;
	}

	public void subscribeToEvent(EventListener<TaskEvent> eventListener, Class<? extends TaskEvent> eventType) {
		taskEventHandler.subscribe(eventListener, eventType);
	}

	@Override
	public void onEvent(TaskEvent event) {
		taskEventHandler.publish(event);
	}
}
