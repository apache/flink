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

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A buffer-oriented reader, which unions multiple {@link BufferReader}
 * instances.
 */
public class UnionBufferReader implements BufferReaderBase {

	private final BufferReaderBase[] readers;

	private final DataAvailabilityListener dataAvailabilityListener;

	// Set of readers, which are not closed yet
	private final Set<BufferReaderBase> remainingReaders;

	// Logical channel index offset for each reader
	private final Map<BufferReaderBase, Integer> readerToIndexOffsetMap = new HashMap<BufferReaderBase, Integer>();

	private int totalNumInputChannels;

	private BufferReaderBase currentReader;

	private int currentReaderChannelIndexOffset;

	private int channelIndexOfLastReadBuffer = -1;

	private boolean isIterative;

	private boolean hasRequestedPartitions;

	private boolean isTaskEvent;

	public UnionBufferReader(BufferReaderBase... readers) {
		checkNotNull(readers);
		checkArgument(readers.length >= 2, "Union buffer reader must be initialized with at least two individual buffer readers");

		this.readers = readers;
		this.remainingReaders = new HashSet<BufferReaderBase>(readers.length + 1, 1.0F);

		this.dataAvailabilityListener = new DataAvailabilityListener(this);

		int currentChannelIndexOffset = 0;

		for (int i = 0; i < readers.length; i++) {
			BufferReaderBase reader = readers[i];

			reader.subscribeToReader(dataAvailabilityListener);

			remainingReaders.add(reader);
			readerToIndexOffsetMap.put(reader, currentChannelIndexOffset);

			totalNumInputChannels += reader.getNumberOfInputChannels();
			currentChannelIndexOffset += reader.getNumberOfInputChannels();
		}
	}

	@Override
	public void requestPartitionsOnce() throws IOException {
		if (!hasRequestedPartitions) {
			for (BufferReaderBase reader : readers) {
				reader.requestPartitionsOnce();
			}

			hasRequestedPartitions = true;
		}
	}


	@Override
	public Buffer getNextBufferBlocking() throws IOException, InterruptedException {
		requestPartitionsOnce();

		do {
			if (currentReader == null) {
				// Finished when all readers are finished
				if (isFinished()) {
					dataAvailabilityListener.clear();
					return null;
				}
				// Finished with superstep when all readers finished superstep
				else if (isIterative && remainingReaders.isEmpty()) {
					resetRemainingReaders();
					return null;
				}
				else {
					while (true) {
						currentReader = dataAvailabilityListener.getNextReaderBlocking();
						currentReaderChannelIndexOffset = readerToIndexOffsetMap.get(currentReader);

						if (isIterative && !remainingReaders.contains(currentReader)) {
							// If the current reader already received its end
							// of superstep event and notified the union reader
							// about newer data *before* all other readers have
							// done so, we delay this notifications.
							dataAvailabilityListener.addReader(currentReader);
						}
						else {
							break;
						}
					}
				}
			}

			Buffer buffer = currentReader.getNextBufferBlocking();
			channelIndexOfLastReadBuffer = currentReaderChannelIndexOffset + currentReader.getChannelIndexOfLastBuffer();

			isTaskEvent = false;

			if (buffer == null) {
				if (currentReader.isFinished() || currentReader.hasReachedEndOfSuperstep()) {
					remainingReaders.remove(currentReader);
				}

				currentReader = null;

				return null;
			}
			else {
				currentReader = null;
				return buffer;
			}
		} while (true);
	}

	@Override
	public Buffer getNextBuffer(Buffer exchangeBuffer) throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Buffer exchange when reading data is not yet supported.");
	}

	@Override
	public int getChannelIndexOfLastBuffer() {
		return channelIndexOfLastReadBuffer;
	}

	@Override
	public int getNumberOfInputChannels() {
		return totalNumInputChannels;
	}

	@Override
	public boolean isTaskEvent() {
		return isTaskEvent;
	}

	@Override
	public void subscribeToReader(EventListener<BufferReaderBase> listener) {
		dataAvailabilityListener.registerListener(listener);
	}

	@Override
	public boolean isFinished() {
		for (BufferReaderBase reader : readers) {
			if (!reader.isFinished()) {
				return false;
			}
		}

		return true;
	}

	private void resetRemainingReaders() {
		checkState(isIterative, "Tried to reset remaining reader with non-iterative reader.");
		checkState(remainingReaders.isEmpty(), "Tried to reset remaining readers, but there are some remaining readers.");
		for (BufferReaderBase reader : readers) {
			remainingReaders.add(reader);
		}
	}

	// ------------------------------------------------------------------------
	// TaskEvents
	// ------------------------------------------------------------------------

	@Override
	public void subscribeToTaskEvent(EventListener<TaskEvent> eventListener, Class<? extends TaskEvent> eventType) {
		for (BufferReaderBase reader : readers) {
			reader.subscribeToTaskEvent(eventListener, eventType);
		}
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException, InterruptedException {
		for (BufferReaderBase reader : readers) {
			reader.sendTaskEvent(event);
		}
	}

	// ------------------------------------------------------------------------
	// Iteration end of superstep events
	// ------------------------------------------------------------------------

	@Override
	public void setIterativeReader() {
		isIterative = true;

		for (BufferReaderBase reader : readers) {
			reader.setIterativeReader();
		}
	}

	@Override
	public void startNextSuperstep() {
		for (BufferReaderBase reader : readers) {
			reader.startNextSuperstep();
		}
	}

	@Override
	public boolean hasReachedEndOfSuperstep() {
		for (BufferReaderBase reader : readers) {
			if (!reader.hasReachedEndOfSuperstep()) {
				return false;
			}
		}

		return true;
	}

	// ------------------------------------------------------------------------
	// Data availability notifications
	// ------------------------------------------------------------------------

	private static class DataAvailabilityListener implements EventListener<BufferReaderBase> {

		private final UnionBufferReader unionReader;

		private final BlockingQueue<BufferReaderBase> readersWithData = new LinkedBlockingQueue<BufferReaderBase>();

		private volatile EventListener<BufferReaderBase> registeredListener;

		private DataAvailabilityListener(UnionBufferReader unionReader) {
			this.unionReader = unionReader;
		}

		@Override
		public void onEvent(BufferReaderBase reader) {
			readersWithData.add(reader);

			if (registeredListener != null) {
				registeredListener.onEvent(unionReader);
			}
		}

		BufferReaderBase getNextReaderBlocking() throws InterruptedException {
			return readersWithData.take();
		}

		void addReader(BufferReaderBase reader) {
			readersWithData.add(reader);
		}

		void clear() {
			readersWithData.clear();
		}

		void registerListener(EventListener<BufferReaderBase> listener) {
			if (registeredListener == null) {
				registeredListener = listener;
			}
			else {
				throw new IllegalStateException("Already registered listener.");
			}
		}
	}
}
