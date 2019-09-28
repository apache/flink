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
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Helper class to read {@link Buffer}s from files into objects.
 */
public class BufferFileChannelReader {
	private final ByteBuffer header = ByteBuffer.allocateDirect(8);
	private final FileChannel fileChannel;

	BufferFileChannelReader(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	/**
	 * Reads data from the object's file channel into the given buffer.
	 *
	 * @param buffer the buffer to read into
	 *
	 * @return whether the end of the file has been reached (<tt>true</tt>) or not (<tt>false</tt>)
	 */
	public boolean readBufferFromFileChannel(Buffer buffer) throws IOException {
		checkArgument(fileChannel.size() - fileChannel.position() > 0);

		// Read header
		header.clear();
		fileChannel.read(header);
		header.flip();

		final boolean isBuffer = header.getInt() == 1;
		final int size = header.getInt();

		if (size > buffer.getMaxCapacity()) {
			throw new IllegalStateException("Buffer is too small for data: " + buffer.getMaxCapacity() + " bytes available, but " + size + " needed. This is most likely due to an serialized event, which is larger than the buffer size.");
		}
		checkArgument(buffer.getSize() == 0, "Buffer not empty");

		fileChannel.read(buffer.getNioBuffer(0, size));
		buffer.setSize(size);

		if (!isBuffer) {
			buffer.tagAsEvent();
		}

		return fileChannel.size() - fileChannel.position() == 0;
	}
}
