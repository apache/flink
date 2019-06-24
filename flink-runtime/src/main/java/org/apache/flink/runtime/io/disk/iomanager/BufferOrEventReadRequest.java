/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of {@link ReadRequest} will used in {@link AsynchronousBufferOrEventFileReader}.
 */
public final class BufferOrEventReadRequest implements ReadRequest {
	private final AsynchronousFileIOChannel<Buffer, ReadRequest> channel;

	private final Buffer buffer;

	private final AtomicBoolean hasReachedEndOfFile;

	public BufferOrEventReadRequest(
		AsynchronousFileIOChannel<Buffer, ReadRequest> targetChannel,
		Buffer buffer,
		AtomicBoolean hasReachedEndOfFile) {
		this.channel = targetChannel;
		this.buffer = buffer;
		this.hasReachedEndOfFile = hasReachedEndOfFile;
	}

	@Override
	public void requestDone(IOException ioex) {
		channel.handleProcessedBuffer(buffer, ioex);
	}

	@Override
	public void read() throws IOException {
		final FileChannel fileChannel = channel.fileChannel;

		if (fileChannel.size() - fileChannel.position() > 0) {
			BufferOrEventFileChannelReader reader = new BufferOrEventFileChannelReader(fileChannel);
			hasReachedEndOfFile.set(reader.readBufferFromFileChannel(buffer));
		}
		else {
			hasReachedEndOfFile.set(true);
		}
	}
}

