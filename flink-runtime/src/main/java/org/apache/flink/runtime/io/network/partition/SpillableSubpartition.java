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

package org.apache.flink.runtime.io.network.partition;

import com.google.common.base.Optional;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class SpillableSubpartition extends ResultSubpartition {

	private final IOManager ioManager;

	private final List<Buffer> buffers = new ArrayList<Buffer>();

	private volatile BufferFileWriter writer;

	private volatile boolean finishedProduce;

	public SpillableSubpartition(int index, ResultPartition parent, IOManager ioManager) {
		super(index, parent);
		this.ioManager = ioManager;
	}

	@Override
	public void add(Buffer buffer) throws IOException {
		checkNotNull(buffer);

		synchronized (buffers) {
			if (writer == null) {
				buffers.add(buffer);
				return;
			}
		}

		// else: spilling
		writer.writeBlock(buffer);
	}

	@Override
	public void finish() throws IOException {
		if (finishedProduce) {
			// TODO Or rather throw an exception as this might indicate misuse?
			return;
		}

		add(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE));

		// If we already spilled, make  sure that the writer is finished before we change our state
		// in order to ensure that consumers don't request too early.
		if (writer != null) {
			synchronized (buffers) {
				writer.close();
			}
		}

		finishedProduce = true;
	}

	@Override
	public void release() throws IOException {

	}

	@Override
	public ResultSubpartitionView getReadView(Optional<BufferProvider> bufferProvider) throws IOException {
		if (!finishedProduce) {
			throw new IllegalSubpartitionRequestException("Queue has not been finished yet.");
		}

		if (!bufferProvider.isPresent()) {
			throw new IllegalSubpartitionRequestException("Did not provide a buffer provider request, which is necessary at the moment.");
		}

		synchronized (buffers) {
			if (writer != null && writer.getNumberOfOutstandingRequests() == 0) {
				return new SpilledSubpartitionView(this, ioManager, writer.getChannelID(), bufferProvider.get());
			}
			else {
				return new SpillableSubpartitionView(this, bufferProvider.get());
			}
		}
	}

	@Override
	public boolean isFinished() {
		return finishedProduce;
	}

	@Override
	public int releaseMemory() throws IOException {
		if (writer == null) {
			synchronized (buffers) {
				writer = ioManager.createBufferFileWriter(ioManager.createChannel());

				int numBuffers = buffers.size();

				for (int i = 0; i < numBuffers; i++) {
					writer.writeBlock(buffers.remove(0));
				}

				return numBuffers;
			}
		}

		return 0;
	}

	boolean isInMemory() {
		return writer == null;
	}

// ------------------------------------------------------------------------

	static class SpillableSubpartitionView implements ResultSubpartitionView {

		private final SpillableSubpartition queue;

		private final BufferProvider bufferProvider;

		private final int numBuffers;

		private SpilledSubpartitionView reader;

		private int currentQueuePosition;

		private long currentBytesRead;

		private volatile boolean isConsumed;

		public SpillableSubpartitionView(SpillableSubpartition queue, BufferProvider bufferProvider) {
			this.queue = queue;
			this.bufferProvider = bufferProvider;
			this.numBuffers = queue.buffers.size();
		}

		@Override
		public boolean isConsumed() {
			if (isConsumed) {
				return true;
			}

			synchronized (queue.buffers) {
				if (reader != null) {
					isConsumed = reader.isConsumed();
				}
				else {
					isConsumed = currentQueuePosition >= numBuffers;
				}
			}

			if (isConsumed) {
				queue.notifyConsumed();

				return true;
			}
			else {
				return false;
			}
		}

		@Override
		public Buffer getNextBuffer() throws IOException {
			// 1) In-memory
			synchronized (queue.buffers) {
				if (queue.writer == null) {
					if (currentQueuePosition < numBuffers) {
						Buffer buffer = queue.buffers.get(currentQueuePosition);

						// Ensure that a concurrent spilling does not lead to a buffer leak
						buffer.retain();

						// TODO Fix hard coding of 8 bytes for the header
						currentBytesRead += buffer.getSize() + 8;
						currentQueuePosition++;

						return buffer;
					}

					return null;
				}
			}

			// 2) Spilled
			if (reader != null) {
				return reader.getNextBuffer();
			}

			// 3) Spilling
			// Make sure that all buffers are written before consuming them. We can't block here,
			// because this might be called from an network I/O thread.
			if (queue.writer.getNumberOfOutstandingRequests() > 0) {
				return null;
			}

			if (reader == null) {
				reader = new SpilledSubpartitionView(queue, queue.ioManager, queue.writer.getChannelID(), bufferProvider, currentBytesRead);
			}

			return reader.getNextBuffer();
		}

		@Override
		public void release() throws IOException {

		}

		@Override
		public boolean subscribe(NotificationListener listener) throws IOException {
			if (reader == null) {
				synchronized (queue.buffers) {
					if (queue.writer == null) {
						return false;
					}
				}

				// Spilling
				if (queue.writer.getNumberOfOutstandingRequests() > 0) {
					return queue.writer.subscribe(listener);
				}

				return false;
			}

			return reader.subscribe(listener);
		}
	}
}
