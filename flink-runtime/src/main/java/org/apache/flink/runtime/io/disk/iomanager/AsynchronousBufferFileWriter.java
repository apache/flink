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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.io.IOException;

public class AsynchronousBufferFileWriter extends AsynchronousFileIOChannel<Buffer, WriteRequest> implements BufferFileWriter {

	private static final RecyclingCallback CALLBACK = new RecyclingCallback();

	protected AsynchronousBufferFileWriter(ID channelID, RequestQueue<WriteRequest> requestQueue) throws IOException {
		super(channelID, requestQueue, CALLBACK, true);
	}

	@Override
	public void writeBlock(Buffer buffer) throws IOException {
		addRequest(new BufferWriteRequest(this, buffer));
	}

	@Override
	public int getNumberOfOutstandingRequests() {
		return requestsNotReturned.get();
	}

	@Override
	public boolean registerAllRequestsProcessedListener(NotificationListener listener) throws IOException {
		return super.registerAllRequestsProcessedListener(listener);
	}

	/**
	 * Recycles the buffer after the I/O request.
	 */
	private static class RecyclingCallback implements RequestDoneCallback<Buffer> {

		@Override
		public void requestSuccessful(Buffer buffer) {
			buffer.recycle();
		}

		@Override
		public void requestFailed(Buffer buffer, IOException e) {
			buffer.recycle();
		}
	}
}
