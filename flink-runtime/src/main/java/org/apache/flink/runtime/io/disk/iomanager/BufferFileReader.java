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

import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter.WriteRequestType;
import org.apache.flink.runtime.io.network.Envelope;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferFuture;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.BufferOrEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BufferFileReader extends BufferChannelAccess<ReadRequest> {

	private final AtomicBoolean isFinished = new AtomicBoolean(false);

	private final LinkedBlockingQueue<BufferOrEvent> bufferOrEventReturnQueue = new LinkedBlockingQueue<BufferOrEvent>();

	private final BufferProvider bufferProvider;

	private BufferFuture currentBufferRequest;

	protected BufferFileReader(
			Channel.ID ioChannelId,
			RequestQueue<ReadRequest> ioThreadRequestQueue,
			BufferProvider bufferProvider,
			long initialPosition) throws IOException {

		super(ioChannelId, ioThreadRequestQueue, false);

		this.bufferProvider = checkNotNull(bufferProvider);

		checkArgument(initialPosition >= 0 && initialPosition <= fileChannel.size());
		fileChannel.position(initialPosition);
	}

	@Override
	public void close() throws IOException {
		super.close();

		BufferOrEvent boe = bufferOrEventReturnQueue.poll();
		while (boe != null) {
			if (boe.isBuffer()) {
				boe.getBuffer().recycle();
			}

			boe = bufferOrEventReturnQueue.poll();
		}

		if (currentBufferRequest != null) {
			if (currentBufferRequest.getBuffer() != null) {
				currentBufferRequest.getBuffer().recycle();
			}
			else {
				currentBufferRequest.cancel();
			}
		}
	}

	@Override
	protected void returnBuffer(BufferOrEvent bufferOrEvent) {
		if (isFinished.get()) {
			if (bufferOrEvent != null && bufferOrEvent.isBuffer()) {
				bufferOrEvent.getBuffer().recycle();
			}
		}
		else {
			if (bufferOrEvent != null) {
				bufferOrEventReturnQueue.add(bufferOrEvent);
			}
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

	public boolean hasNext() {
		return (!isFinished.get() || !bufferOrEventReturnQueue.isEmpty()) && !isClosed.get();
	}

	public BufferOrEvent getNextBufferOrEvent() throws IOException {
		BufferOrEvent bufferOrEvent = bufferOrEventReturnQueue.poll();
		if (bufferOrEvent != null) {
			return bufferOrEvent;
		}

		if (!isFinished.get()) {
			if (currentBufferRequest != null) {
				if (currentBufferRequest.getBuffer() != null) {
					addRequest(new BufferOrEventReadRequest(this, currentBufferRequest.getBuffer(), isFinished));
					currentBufferRequest = null;
				}
			}

			if (currentBufferRequest == null) {
				currentBufferRequest = bufferProvider.requestBuffer();
				while (currentBufferRequest.getBuffer() != null) {
					addRequest(new BufferOrEventReadRequest(this, currentBufferRequest.getBuffer(), isFinished));
					currentBufferRequest = bufferProvider.requestBuffer();
				}
			}
		}

		return bufferOrEventReturnQueue.poll();
	}

	static class BufferOrEventReadRequest implements ReadRequest {

		// +---+---+---+---+---+
		// + a | b :   :   :   +
		// +---'---'---'---'---+
		//
		// a: write request type (1)
		// b: payload length (4)
		private final ByteBuffer typeAndLengthBuffer = ByteBuffer.allocateDirect(5);

		private final ChannelAccess<BufferOrEvent, ?> ioChannel;

		private final Buffer buffer;

		private final AtomicBoolean isFinished;

		private BufferOrEvent bufferOrEvent;

		protected BufferOrEventReadRequest(ChannelAccess<BufferOrEvent, ?> targetChannel, Buffer buffer, AtomicBoolean isFinished) {
			this.ioChannel = targetChannel;
			this.buffer = buffer;
			this.isFinished = isFinished;
		}

		@Override
		public void read() throws IOException {
			final FileChannel fileChannel = this.ioChannel.fileChannel;

			if (fileChannel.position() >= fileChannel.size()) {
				buffer.recycle();
				bufferOrEvent = null;

				isFinished.compareAndSet(false, true);
				return;
			}

			try {
				// Read type and length
				fileChannel.read(typeAndLengthBuffer);
			}
			catch (IOException e) {
				buffer.recycle();
				throw e;
			}

			WriteRequestType type = WriteRequestType.values()[typeAndLengthBuffer.get(0)];
			int length = typeAndLengthBuffer.getInt(1);

			if (type == WriteRequestType.BUFFER) {
				buffer.setSize(length);
				bufferOrEvent = new BufferOrEvent(buffer);

				fileChannel.read(buffer.getNioBuffer());
			}
			else if (type == WriteRequestType.EVENT) {
				buffer.recycle();

				ByteBuffer serializedEvent = ByteBuffer.allocate(length);
				fileChannel.read(serializedEvent);
				serializedEvent.flip();

				bufferOrEvent = new BufferOrEvent(Envelope.fromSerializedEvent(serializedEvent, getClass().getClassLoader()));
			}
			else {
				throw new IOException("Unexpected file content.");
			}
		}

		@Override
		public void requestDone(IOException e) {
			ioChannel.handleProcessedBuffer(bufferOrEvent, e);
		}
	}
}
