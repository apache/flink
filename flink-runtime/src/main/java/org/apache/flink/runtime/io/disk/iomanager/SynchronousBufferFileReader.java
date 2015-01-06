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
import java.nio.ByteBuffer;

/**
 * A synchronous {@link BufferFileReader} implementation.
 *
 * <p> This currently bypasses the I/O manager as it is the only synchronous implementation, which
 * is currently in use.
 *
 * TODO Refactor I/O manager setup and refactor this into it
 */
public class SynchronousBufferFileReader extends SynchronousFileIOChannel implements BufferFileReader {

	private final ByteBuffer header = ByteBuffer.allocateDirect(8);

	private boolean hasReachedEndOfFile;

	public SynchronousBufferFileReader(ID channelID, boolean writeEnabled) throws IOException {
		super(channelID, writeEnabled);
	}

	@Override
	public void readInto(Buffer buffer) throws IOException {
		if (fileChannel.size() - fileChannel.position() > 0) {
			// This is the synchronous counter part to the asynchronous buffer read request

			// Read header
			header.clear();
			fileChannel.read(header);
			header.flip();

			final boolean isBuffer = header.getInt() == 1;
			final int size = header.getInt();

			if (size > buffer.getMemorySegment().size()) {
				throw new IllegalStateException("Buffer is too small for data: " + buffer.getMemorySegment().size() + " bytes available, but " + size + " needed. This is most likely due to an serialized event, which is larger than the buffer size.");
			}

			buffer.setSize(size);

			fileChannel.read(buffer.getNioBuffer());

			if (!isBuffer) {
				buffer.tagAsEvent();
			}

			hasReachedEndOfFile = fileChannel.size() - fileChannel.position() == 0;
		}
		else {
			buffer.recycle();
		}
	}

	@Override
	public void seekToPosition(long position) throws IOException {
		fileChannel.position(position);
	}

	@Override
	public boolean hasReachedEndOfFile() {
		return hasReachedEndOfFile;
	}
}
