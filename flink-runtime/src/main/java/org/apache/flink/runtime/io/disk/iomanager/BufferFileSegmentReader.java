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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.runtime.io.network.BufferOrEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.runtime.io.network.BufferOrEvent.BufferOrEventType;

public class BufferFileSegmentReader extends ChannelAccess<BufferOrEvent, ReadRequest> implements BufferFileReaderBase {

	protected final Object closeLock = new Object();

	protected AtomicBoolean isClosed = new AtomicBoolean(false);

	// This counter keeps track of pending read/write requests
	protected AtomicInteger pendingRequests = new AtomicInteger(0);

	private final AtomicBoolean isFinished = new AtomicBoolean(false);

	private final LinkedBlockingQueue<BufferOrEvent> bufferOrEventReturnQueue = new LinkedBlockingQueue<BufferOrEvent>();


	protected BufferFileSegmentReader(
			Channel.ID ioChannelId,
			RequestQueue<ReadRequest> ioThreadRequestQueue,
			long initialPosition) throws IOException {

		super(ioChannelId, ioThreadRequestQueue, false);

		checkArgument(initialPosition >= 0 && initialPosition <= fileChannel.size());
		fileChannel.position(initialPosition);
	}

	@Override
	public boolean isClosed() {
		return isClosed.get();
	}

	@Override
	protected void returnBuffer(BufferOrEvent bufferOrEvent) {
		if (!isFinished.get() && bufferOrEvent != null) {
			bufferOrEventReturnQueue.add(bufferOrEvent);
		}

		if (isClosed.get()) {
			synchronized (closeLock) {
				if (pendingRequests.decrementAndGet() == 0) {
					closeLock.notifyAll();
				}
			}
		}
		else {
			pendingRequests.decrementAndGet();
		}
	}

	protected void addRequest(FileSegmentReadRequest request) throws IOException {
		checkErroneous();

		pendingRequests.incrementAndGet();

		if (isClosed.get() || requestQueue.isClosed()) {
			pendingRequests.decrementAndGet();

			throw new IOException("The reader thread has been closed.");
		}

		requestQueue.add(request);
	}

	@Override
	public boolean hasNext() {
		return (!isFinished.get() || !bufferOrEventReturnQueue.isEmpty()) && !isClosed.get();
	}

	@Override
	public BufferOrEvent getNextBufferOrEvent() throws IOException {
		if (!hasNext()) {
			throw new IllegalStateException("Tried to read from finished reader.");
		}

		BufferOrEvent boe = bufferOrEventReturnQueue.poll();
		if (boe != null) {
			return boe;
		}

		while (bufferOrEventReturnQueue.size() < 32 && !isFinished.get() && !isClosed.get()) {
			addRequest(new FileSegmentReadRequest(this, isFinished));
		}

		return bufferOrEventReturnQueue.poll();
	}

	public void close() throws IOException {
		if (isClosed.compareAndSet(false, true)) {
			synchronized (closeLock) {
				try {
					// Wait for all pending buffers to be processed
					while (pendingRequests.get() > 0) {
						try {
							closeLock.wait(1000);
							checkErroneous();
						}
						catch (InterruptedException iex) {
						}
					}
				}
				finally {
					// Close the file
					if (fileChannel.isOpen()) {
						fileChannel.close();
					}
				}
			}
		}
	}

	static class FileSegmentReadRequest implements ReadRequest {

		private final ByteBuffer typeAndLengthBuffer = ByteBuffer.allocateDirect(5);

		private final ChannelAccess<BufferOrEvent, ?> ioChannel;

		private final AtomicBoolean isFinished;

		private BufferOrEvent bufferOrEvent;

		protected FileSegmentReadRequest(ChannelAccess<BufferOrEvent, ?> targetChannel, AtomicBoolean isFinished) {
			this.ioChannel = targetChannel;
			this.isFinished = isFinished;
		}

		@Override
		public void read() throws IOException {
			final FileChannel fileChannel = this.ioChannel.fileChannel;

			if (fileChannel.position() >= fileChannel.size()) {
				isFinished.compareAndSet(false, true);
				return;
			}

			// Read type and length
			fileChannel.read(typeAndLengthBuffer);

			BufferOrEventType type = BufferOrEventType.values()[typeAndLengthBuffer.get(0)];
			int length = typeAndLengthBuffer.getInt(1);

			long fileSegmentPosition = fileChannel.position();
			fileChannel.position(fileSegmentPosition + length);

			FileSegment fileSegment = new FileSegment(fileChannel, fileSegmentPosition, length);

			this.bufferOrEvent = new BufferOrEvent(fileSegment, type);
		}

		@Override
		public void requestDone(IOException e) {
			ioChannel.handleProcessedBuffer(bufferOrEvent, e);
		}
	}
}