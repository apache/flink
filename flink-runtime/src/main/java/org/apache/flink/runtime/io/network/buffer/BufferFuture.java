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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.runtime.util.event.EventListener;

import static com.google.common.base.Preconditions.checkState;

public class BufferFuture {

	private final Object monitor = new Object();

	private final int bufferSize;

	private Buffer buffer;

	private EventListener<BufferFuture> listener;

	private boolean isCancelled;

	public BufferFuture(int bufferSize) {
		synchronized (monitor) {
			this.bufferSize = bufferSize;
		}
	}

	public BufferFuture(Buffer buffer) {
		synchronized (monitor) {
			this.buffer = buffer;
			this.bufferSize = buffer.getSize();
		}
	}

	public Buffer getBuffer() {
		synchronized (monitor) {
			return buffer;
		}
	}

	public int getBufferSize() {
		synchronized (monitor) {
			return bufferSize;
		}
	}

	public BufferFuture waitForBuffer() throws InterruptedException {
		synchronized (monitor) {
			while (buffer == null && !isCancelled) {
				monitor.wait();
			}

			return this;
		}
	}

	public BufferFuture addListener(EventListener<BufferFuture> listenerToAdd) {
		synchronized (monitor) {
			checkState(!isCancelled, "Buffer future has already been cancelled.");
			checkState(listener == null, "Listener has already been added.");

			// Late registration
			if (buffer != null) {
				listenerToAdd.onEvent(this);
			}
			else {
				listener = listenerToAdd;
			}

			return this;
		}
	}

	public void cancel() {
		synchronized (monitor) {
			checkState(buffer == null, "Too late to cancel. Already handed a buffer in for this future.");

			isCancelled = true;
			monitor.notifyAll();

			if (listener != null) {
				listener.onEvent(this);
			}
		}
	}

	public boolean isDone() {
		synchronized (monitor) {
			return buffer != null || isCancelled;
		}
	}

	public boolean isSuccess() {
		synchronized (monitor) {
			return buffer != null;
		}
	}

	public boolean isCancelled() {
		synchronized (monitor) {
			return isCancelled;
		}
	}

	void handInBuffer(Buffer bufferToHandIn) {
		synchronized (monitor) {
			checkState(buffer == null, "Already handed a buffer in for this future.");

			if (isCancelled) {
				bufferToHandIn.recycle();
				return;
			}

			buffer = bufferToHandIn;
			buffer.setSize(bufferSize);

			if (listener != null) {
				listener.onEvent(this);
			}

			monitor.notifyAll();
		}
	}
}

