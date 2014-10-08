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

import org.apache.flink.runtime.io.network.partition.BufferOrEvent;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

abstract class BufferChannelAccess<T extends IORequest> extends ChannelAccess<BufferOrEvent, T> {

	protected final Object closeLock = new Object();

	protected AtomicBoolean isClosed = new AtomicBoolean(false);

	// This counter keeps track of pending read/write requests
	protected AtomicInteger pendingRequests = new AtomicInteger(0);

	private AtomicBoolean doDelete = new AtomicBoolean(false);

	protected BufferChannelAccess(
			Channel.ID ioChannelId,
			RequestQueue<T> ioThreadRequestQueue,
			boolean isWriteAccess) throws IOException {

		super(ioChannelId, ioThreadRequestQueue, isWriteAccess);
	}

	@Override
	public boolean isClosed() {
		return isClosed.get();
	}

	protected void addRequest(T request) throws IOException {
		checkErroneous();

		pendingRequests.incrementAndGet();

		if (isClosed.get() || requestQueue.isClosed()) {
			pendingRequests.decrementAndGet();

			throw new IOException("The I/O thread has been closed.");
		}

		requestQueue.add(request);
	}

	public int getNumPendingRequests() {
		return pendingRequests.get();
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

					if (doDelete.get()) {
						deleteChannel();
					}
				}
			}
		}
	}

	protected void deleteFile() throws IOException {
		if (isClosed.get() && !fileChannel.isOpen()) {
			// Closed, can safely delete
			deleteChannel();
		}
		else if (isClosed.get() && fileChannel.isOpen()) {
			// Close has been initiated, but not all pending buffers have been
			// returned.
			if (doDelete.compareAndSet(false, true)) {
				if (!fileChannel.isOpen()) {
					deleteChannel();
				}
			}
		}
		else {
			// Not closed, close and delete afterwards
			try {
				close();
			}
			finally {
				deleteChannel();
			}
		}
	}
}