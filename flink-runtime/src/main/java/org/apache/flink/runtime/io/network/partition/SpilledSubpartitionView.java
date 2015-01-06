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

import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.RequestDoneCallback;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SpilledSubpartitionView implements ResultSubpartitionView {

	private final Object listenerLock = new Object();

	private final ResultSubpartition partition;

	private final BufferFileReader reader;

	private final BufferProvider bufferProvider;

	private final Queue<Buffer> returnedBuffers = new ConcurrentLinkedQueue<Buffer>();

	private volatile NotificationListener listener;

	private volatile Throwable ioError;

	private volatile boolean readerIsConsumed;

	private volatile boolean isConsumed;

	public SpilledSubpartitionView(ResultSubpartition partition, IOManager ioManager, FileIOChannel.ID channelId, BufferProvider bufferProvider) throws IOException {
		this(partition, ioManager, channelId, bufferProvider, 0);
	}

	public SpilledSubpartitionView(ResultSubpartition partition, IOManager ioManager, FileIOChannel.ID channelId, BufferProvider bufferProvider, long initialPosition) throws IOException {
		this.partition = checkNotNull(partition);
		this.reader = ioManager.createBufferFileReader(channelId, new ReadDoneCallback(this));
		this.bufferProvider = checkNotNull(bufferProvider);

		checkArgument(initialPosition >= 0, "Initial position is < 0.");

		if (initialPosition > 0) {
			reader.seekToPosition(initialPosition);
		}

		sendBufferReadRequests();
	}

	@Override
	public boolean isConsumed() {
		if (isConsumed) {
			return true;
		}

		isConsumed = readerIsConsumed && returnedBuffers.isEmpty();

		if (isConsumed) {
			partition.notifyConsumed();

			return true;
		}

		return false;
	}

	@Override
	public Buffer getNextBuffer() throws IOException {
		checkError();

		Buffer buffer = returnedBuffers.poll();

		if (buffer == null && !isConsumed()) {
			sendBufferReadRequests();

			// Try again...
			buffer = returnedBuffers.poll();
		}

		return buffer;
	}

	@Override
	public void release() throws IOException {

	}

	@Override
	public boolean subscribe(NotificationListener listener) throws IOException {
		checkError();

		synchronized (listenerLock) {
			if (isConsumed() || !returnedBuffers.isEmpty()) {
				return false;
			}

			if (this.listener == null) {
				this.listener = listener;
				return true;
			}
		}

		throw new AlreadySubscribedException();
	}

	private void maybeNotifyListener() {
		synchronized (listenerLock) {
			if (listener != null) {
				final NotificationListener consumer = listener;
				listener = null;
				consumer.onNotification();
			}
		}
	}

	private void checkError() throws IOException {
		if (ioError != null) {
			throw new IOException(ioError.getMessage(), ioError);
		}
	}

	// ------------------------------------------------------------------------

	private void sendBufferReadRequests() throws IOException {
		Buffer buffer;
		while ((buffer = bufferProvider.requestBuffer()) != null) {
			reader.readInto(buffer);
		}
	}

	private static class ReadDoneCallback implements RequestDoneCallback<Buffer> {

		private final SpilledSubpartitionView queue;

		public ReadDoneCallback(SpilledSubpartitionView queue) {
			this.queue = queue;
		}

		@Override
		public void requestSuccessful(Buffer buffer) {
			if (!queue.readerIsConsumed) {
				synchronized (queue.listenerLock) {
					queue.returnedBuffers.add(buffer);

					if (queue.reader.isConsumed()) {
						queue.readerIsConsumed = true;
					}

					queue.maybeNotifyListener();
				}
			}
			else {
				buffer.recycle();
			}
		}

		@Override
		public void requestFailed(Buffer buffer, IOException error) {
			synchronized (queue.listenerLock) {
				if (queue.ioError != null) {
					queue.ioError = error;
				}

				buffer.recycle();

				queue.maybeNotifyListener();
			}
		}
	}
}
