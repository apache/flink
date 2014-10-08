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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferFuture;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.util.event.EventHandler;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BufferReader implements BufferReaderBase, BufferProvider {

	private final EventHandler<TaskEvent> taskEventHandler = new EventHandler<TaskEvent>();

	private final InputChannel[] inputChannels;

	private final BufferPool bufferPool;

	private final BlockingQueue<InputChannel> inputChannelsWithData = new LinkedBlockingQueue<InputChannel>();

	private final AtomicReference<EventListener<BufferReader>> listener = new AtomicReference<EventListener<BufferReader>>(null);

	private boolean isIterativeReader;

	private int currentNumEndOfSuperstepEvents;

	private int channelIndexOfLastReadBuffer = -1;

	private boolean isInitialized = false;

	public BufferReader(InputChannel[] inputChannels, BufferPool bufferPool) {
		this.inputChannels = checkNotNull(inputChannels);
		this.bufferPool = checkNotNull(bufferPool);
	}

	@Override
	public Buffer getNextBuffer() throws IOException, InterruptedException {
		if (!isInitialized) {
			for (InputChannel channel : inputChannels) {
				channel.initialize();
			}

			isInitialized = true;
		}

		do {
			// Possibly block until data is available at one of the input channels
			final InputChannel currentChannel = inputChannelsWithData.take();
			final BufferOrEvent nextBufferOrEvent = currentChannel.getNextBufferOrEvent();

			if (nextBufferOrEvent.isBuffer()) {
				channelIndexOfLastReadBuffer = currentChannel.getChannelIndex();

				return nextBufferOrEvent.getBuffer();
			}
			else if (nextBufferOrEvent.isEvent()) {
				final AbstractEvent event = nextBufferOrEvent.getEvent();

				// ------------------------------------------------------------
				// Runtime events
				// ------------------------------------------------------------
				// Note: We can not assume that every channel will be finished
				// with an according event. In failure cases or iterations the
				// consumer task never finishes earlier and has to release all
				// resources.
				// ------------------------------------------------------------
				if (event.getClass() == EndOfPartitionEvent.class) {
					currentChannel.finish();

					if (isFinished()) {
						checkState(inputChannelsWithData.isEmpty(), "Reader is closed, but still has input channel notifications.");

						return null;
					}
				}
				else if (event.getClass() == EndOfSuperstepEvent.class) {
					// This condition check can not be combined with the event
					// type check, because we need to fall through the loop, if
					// we didn't receive all end of superstep events yet.
					if (incrementEndOfSuperstepEventAndCheck()) {
						return null;
					}
				}
				// ------------------------------------------------------------
				// Task events (user)
				// ------------------------------------------------------------
				else if (event instanceof TaskEvent) {
					taskEventHandler.publish((TaskEvent) event);
				}
				else {
					throw new IllegalStateException("Received unexpected event " + event + " from input channel " + currentChannel + ".");
				}
			}
			else {
				throw new IllegalStateException("Received illegal BufferOrEvent without buffer nor event.");
			}
		} while (true);
	}

	boolean hasInputChannelWithData() {
		return !inputChannelsWithData.isEmpty();
	}

	public InputChannel getInputChannel(int index) {
		return inputChannels[index];
	}

	@Override
	public Buffer getNextBuffer(Buffer exchangeBuffer) {
		throw new UnsupportedOperationException("Buffer exchange when reading data is not yet supported.");
	}

	@Override
	public int getChannelIndexOfLastBuffer() {
		return channelIndexOfLastReadBuffer;
	}

	public void onAvailableInputChannel(InputChannel inputChannel) {
		inputChannelsWithData.add(inputChannel);

		if (listener.get() != null) {
			listener.get().onEvent(this);
		}
	}

	public int getNumberOfInputChannels() {
		return inputChannels.length;
	}

	public BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public void releaseAllResources() {
		// The buffer pool can actually be destroyed immediately after the
		// reader received all of the data from the input channels.
		bufferPool.destroy();

		for (InputChannel inputChannel : inputChannels) {
			inputChannel.releaseAllResources();
		}
	}

	void subscribeToReader(EventListener<BufferReader> listener) {
		if (!this.listener.compareAndSet(null, listener)) {
			throw new IllegalStateException(listener + " is already registered as a record availability listener");
		}
	}

	@Override
	public BufferFuture requestBuffer() {
		return bufferPool.requestBuffer();
	}

	@Override
	public BufferFuture requestBuffer(int size) {
		return bufferPool.requestBuffer(size);
	}

	@Override
	public boolean isFinished() {
		for (InputChannel inputChannel : inputChannels) {
			if (!inputChannel.isFinished()) {
				return false;
			}
		}

		return true;
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException, InterruptedException {
		// This can be improved by just serializing the event once for all
		// remote input channels.
		for (InputChannel inputChannel : inputChannels) {
			inputChannel.sendTaskEvent(event);
		}
	}

	@Override
	public void subscribeToTaskEvent(EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType) {
		taskEventHandler.subscribe(listener, eventType);
	}

	// ------------------------------------------------------------------------
	// Iteration end of superstep events
	// ------------------------------------------------------------------------

	@Override
	public void setIterativeReader() {
		isIterativeReader = true;
	}

	@Override
	public void startNextSuperstep() {
		checkState(isIterativeReader, "Tried to start next superstep in a non-iterative reader.");
		checkState(currentNumEndOfSuperstepEvents == inputChannels.length, "Tried to start next superstep before reaching end of previous superstep.");

		currentNumEndOfSuperstepEvents = 0;
	}

	@Override
	public boolean hasReachedEndOfSuperstep() {
		return currentNumEndOfSuperstepEvents == inputChannels.length;
	}

	private boolean incrementEndOfSuperstepEventAndCheck() {
		checkState(isIterativeReader, "Received end of superstep event in a non-iterative reader.");

		currentNumEndOfSuperstepEvents++;

		checkState(currentNumEndOfSuperstepEvents <= inputChannels.length, "Received too many (" + currentNumEndOfSuperstepEvents + ") end of superstep events.");

		return currentNumEndOfSuperstepEvents == inputChannels.length;
	}
}
