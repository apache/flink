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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.buffer.BufferFuture;
import org.apache.flink.runtime.io.network.netty.protocols.partition.PartitionRequestClient;
import org.apache.flink.runtime.io.network.netty.protocols.partition.PartitionRequestListener;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemoteInputChannel extends InputChannel implements PartitionRequestListener {

	private final InputChannelID id;

	private final PartitionRequestClient partitionRequestClient;

	private final Queue<BufferOrEvent> bufferOrEventQueue = new ArrayDeque<BufferOrEvent>();

	private final AtomicBoolean isFinished = new AtomicBoolean(false);

	private boolean isInitialized;

	protected RemoteInputChannel(
			int channelIndex, IntermediateResultPartitionID partitionId, BufferReader reader,
			PartitionRequestClient partitionRequestClient) {

		super(channelIndex, partitionId, reader);

		this.partitionRequestClient = partitionRequestClient;
		this.id = new InputChannelID();
	}

	public InputChannelID getId() {
		return id;
	}

	@Override
	public void initialize() {
		if (!isInitialized) {
			partitionRequestClient.requestIntermediateResultPartition(partitionId, channelIndex, this);

			isInitialized = true;
		}
	}

	public BufferOrEvent getNextBufferOrEvent() throws IOException {
		synchronized (bufferOrEventQueue) {
			BufferOrEvent boe = bufferOrEventQueue.poll();

			if (boe == null) {
				throw new IOException("Queried input channel for data although non is available.");
			}

			return boe;
		}
	}

	@Override
	public void sendTaskEvent(TaskEvent event) {
		partitionRequestClient.sendTaskEvent(partitionId, event, this);
	}

	@Override
	public void finish() {
		isFinished.compareAndSet(false, true);
	}

	@Override
	public boolean isFinished() {
		return isFinished.get();
	}

	@Override
	public void releaseAllResources() {
		if (isFinished.compareAndSet(false, true)) {
			BufferOrEvent boe;
			while ((boe = bufferOrEventQueue.poll()) != null && boe.isBuffer()) {
				boe.getBuffer().recycle();
			}
		}
	}

	// ------------------------------------------------------------------------
	// Partition request listener
	// ------------------------------------------------------------------------

	public void onBufferOrEvent(BufferOrEvent bufferOrEvent) {
		if (!isFinished.get()) {
			synchronized (bufferOrEventQueue) {
				bufferOrEventQueue.add(bufferOrEvent);
				notifyReaderAboutAvailableBufferOrEvent();
			}
		}
		// If already finished and tries to enqueue a buffer, make sure that
		// the buffer is recycled.
		else if (bufferOrEvent.isBuffer()) {
			bufferOrEvent.getBuffer().recycle();
		}
	}

	public void onError(Throwable t) {
		// TODO implement error handling
	}

	@Override
	public BufferFuture requestBuffer() {
		if (!isFinished.get()) {
			return reader.requestBuffer();
		}

		return null;
	}

	@Override
	public BufferFuture requestBuffer(int size) {
		if (!isFinished.get()) {
			return reader.requestBuffer();
		}

		return null;
	}
}
