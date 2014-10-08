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

import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.SerializationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.io.network.BufferOrEvent.BufferOrEventType;

public class BufferFileWriter extends BufferChannelAccess<WriteRequest> {

	enum WriteRequestType {
		BUFFER, EVENT
	}

	protected BufferFileWriter(Channel.ID ioChannelId, RequestQueue<WriteRequest> ioThreadRequestQueue) throws IOException {
		super(ioChannelId, ioThreadRequestQueue, true);
	}

	public void writeBufferOrEvent(BufferOrEvent bufferOrEvent) throws IOException {
		if (bufferOrEvent.isBuffer()) {
			writeBuffer(bufferOrEvent.getBuffer());
		}
		else if (bufferOrEvent.isEvent()) {
			writeEvent(bufferOrEvent.getEvent());
		}
		else {
			throw new IOException("Illegal BufferOrEvent");
		}
	}

	public void writeBuffer(Buffer buffer) throws IOException {
		checkErroneous();

		addRequest(new BufferWriteRequest(this, buffer));
	}

	public void writeBuffers(List<Buffer> buffers) throws IOException {
		checkNotNull(buffers);

		for (Buffer buffer : buffers) {
			addRequest(new BufferWriteRequest(this, buffer));
		}
	}

	public void writeEvent(AbstractEvent event) throws IOException {
		checkErroneous();

		ByteBuffer serializedEvent = SerializationUtil.toSerializedEvent(event);
		addRequest(new EventWriteRequest(this, serializedEvent));
	}

	@Override
	protected void returnBuffer(BufferOrEvent bufferOrEvent) {
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

	@Override
	public void deleteFile() throws IOException {
		super.deleteFile();
	}

	// ------------------------------------------------------------------------
	// Write requests for the IO writer thread
	// ------------------------------------------------------------------------

	private static class BufferWriteRequest implements WriteRequest {

		// +---+---+---+---+---+
		// + a | b :   :   :   +
		// +---'---'---'---'---+
		//
		// a: write request type (1)
		// b: payload length (4)
		private final ByteBuffer typeAndLengthBuffer = ByteBuffer.allocateDirect(5);

		private final ChannelAccess<BufferOrEvent, ?> ioChannel;

		private final Buffer buffer;

		private BufferWriteRequest(ChannelAccess<BufferOrEvent, ?> ioChannel, Buffer buffer) {
			this.ioChannel = ioChannel;
			this.buffer = buffer;

			this.typeAndLengthBuffer.put(0, (byte) BufferOrEventType.BUFFER.ordinal());
		}

		@Override
		public void write() throws IOException {
			try {
				// Set the length for this buffer
				typeAndLengthBuffer.putInt(1, buffer.getSize());

				ioChannel.fileChannel.write(typeAndLengthBuffer);
				ioChannel.fileChannel.write(buffer.getNioBuffer());
			}
			finally {
				buffer.recycle();
			}
		}

		@Override
		public void requestDone(IOException e) {
			// Only notify of exception, recycle here
			ioChannel.handleProcessedBuffer(null, e);
		}
	}

	private static class EventWriteRequest implements WriteRequest {

		// +---+---+---+---+---+
		// + a | b :   :   :   +
		// +---'---'---'---'---+
		//
		// a: write request type (1)
		// b: payload length (4)
		private final ByteBuffer typeAndLengthBuffer = ByteBuffer.allocateDirect(5);

		private final ChannelAccess<BufferOrEvent, ?> ioChannel;

		private final ByteBuffer serializedEvent;

		private EventWriteRequest(ChannelAccess<BufferOrEvent, ?> ioChannel, ByteBuffer serializedEvent) {
			this.ioChannel = ioChannel;
			this.serializedEvent = serializedEvent;

			this.typeAndLengthBuffer.put(0, (byte) BufferOrEventType.EVENT.ordinal());
		}

		@Override
		public void write() throws IOException {
			typeAndLengthBuffer.putInt(1, serializedEvent.remaining());

			ioChannel.fileChannel.write(typeAndLengthBuffer);
			ioChannel.fileChannel.write(serializedEvent);
		}

		@Override
		public void requestDone(IOException e) {
			// Only notify of exception
			ioChannel.handleProcessedBuffer(null, e);
		}
	}
}
