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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsynchronousBufferFileReader extends AsynchronousFileIOChannel<Buffer, ReadRequest> implements BufferFileReader {

	private final AtomicBoolean hasReachedEndOfFile = new AtomicBoolean();
	private final int bufferSize;
	private final boolean withHeader;
	private long currentPosition; // Used only when withHeader == false.

	protected AsynchronousBufferFileReader(ID channelID, RequestQueue<ReadRequest> requestQueue,
			RequestDoneCallback<Buffer> callback, int bufferSize) throws IOException {
		this(channelID, requestQueue, callback, bufferSize, true);
	}

	protected AsynchronousBufferFileReader(ID channelID, RequestQueue<ReadRequest> requestQueue,
			RequestDoneCallback<Buffer> callback, int bufferSize, boolean withHeader) throws IOException {
		super(channelID, requestQueue, callback, false);
		this.bufferSize = bufferSize;
		this.withHeader = withHeader;
	}

	@Override
	public void readInto(Buffer buffer) throws IOException {
		if (withHeader) {
			// If write buffers with their headers, we don't know the real buffer size until the headers have been
			// fetched from the disk, as a result, we cannot update currentPosition.
			addRequest(new BufferReadRequest(this, buffer, hasReachedEndOfFile, bufferSize));
		} else {
			throw new IllegalArgumentException("Method readInto(Buffer buffer) only support withHeader mode.");
		}
	}

	@Override
	public void readInto(Buffer buffer, long length) throws IOException {
		if (!withHeader) {
			addRequest(new StreamReadRequest(
				this, buffer, hasReachedEndOfFile, currentPosition, length, bufferSize));
			currentPosition += length;
		} else {
			throw new IllegalArgumentException("Method readInto(Buffer buffer, long length) only support withoutHeader mode.");
		}
	}

	@Override
	public void seekToPosition(long position) throws IOException {
		currentPosition = position;
		requestQueue.add(new SeekRequest(this, position, bufferSize));
	}

	@Override
	public boolean hasReachedEndOfFile() {
		return hasReachedEndOfFile.get();
	}
}
