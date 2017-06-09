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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.TaskEventHandler;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;

/**
 * A buffer-oriented runtime result writer.
 * <p>
 * The {@link ResultPartitionWriter} is the runtime API for producing results. It
 * supports two kinds of data to be sent: buffers and events.
 */
public class ResultPartitionWriter implements EventListener<TaskEvent> {

	private final ResultPartition partition;

	private final TaskEventHandler taskEventHandler = new TaskEventHandler();

	public ResultPartitionWriter(ResultPartition partition) {
		this.partition = partition;
	}

	// ------------------------------------------------------------------------
	// Attributes
	// ------------------------------------------------------------------------

	public ResultPartitionID getPartitionId() {
		return partition.getPartitionId();
	}

	public BufferProvider getBufferProvider() {
		return partition.getBufferProvider();
	}

	public int getNumberOfOutputChannels() {
		return partition.getNumberOfSubpartitions();
	}

	public int getNumTargetKeyGroups() {
		return partition.getNumTargetKeyGroups();
	}

	// ------------------------------------------------------------------------
	// Data processing
	// ------------------------------------------------------------------------

	public void writeBuffer(Buffer buffer, int targetChannel) throws IOException {
		partition.add(buffer, targetChannel);
	}

	/**
	 * Writes the given buffer to all available target channels.
	 *
	 * The buffer is taken over and used for each of the channels.
	 * It will be recycled afterwards.
	 *
	 * @param eventBuffer the buffer to write
	 * @throws IOException
	 */
	public void writeBufferToAllChannels(final Buffer eventBuffer) throws IOException {
		try {
			for (int targetChannel = 0; targetChannel < partition.getNumberOfSubpartitions(); targetChannel++) {
				// retain the buffer so that it can be recycled by each channel of targetPartition
				eventBuffer.retain();
				writeBuffer(eventBuffer, targetChannel);
			}
		} finally {
			// we do not need to further retain the eventBuffer
			// (it will be recycled after the last channel stops using it)
			eventBuffer.recycle();
		}
	}

	// ------------------------------------------------------------------------
	// Event handling
	// ------------------------------------------------------------------------

	public void subscribeToEvent(EventListener<TaskEvent> eventListener, Class<? extends TaskEvent> eventType) {
		taskEventHandler.subscribe(eventListener, eventType);
	}

	@Override
	public void onEvent(TaskEvent event) {
		taskEventHandler.publish(event);
	}
}
