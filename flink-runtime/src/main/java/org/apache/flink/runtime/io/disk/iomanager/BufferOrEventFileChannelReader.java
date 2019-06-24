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
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Used for read data from fileChannel asynchronous, will be used in {@link AsynchronousBufferOrEventFileReader}.
 */
public class BufferOrEventFileChannelReader {
	/** Header is "channel index" (4 bytes) + length (4 bytes) + buffer/event (1 byte). */
	private static final int HEAD_LENGTH = 9;
	private final FileChannel fileChannel;
	private int size;
	private boolean isBuffer;

	BufferOrEventFileChannelReader(FileChannel fileChannel) {
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
		ByteBuffer header = buffer.getNioBuffer(0, HEAD_LENGTH);
		header.clear();
		IOUtils.readFully(fileChannel, header);
		header.flip();
		readBufferOrEventMeta(header);

		if (size > buffer.getMaxCapacity()) {
			throw new IllegalStateException("Buffer is too small for data: " + buffer.getMaxCapacity() + " bytes available, but " + size + " needed. This is most likely due to an serialized event, which is larger than the buffer size.");
		}
		checkArgument(buffer.getSize() == 0, "Buffer not empty");

		IOUtils.readFully(fileChannel, (buffer.getNioBuffer(HEAD_LENGTH, size)));
		buffer.setSize(HEAD_LENGTH + size);

		if (!isBuffer) {
			buffer.tagAsEvent();
		}

		return fileChannel.size() - fileChannel.position() == 0;
	}

	public static void writeBufferOrEventMeta(ByteBuffer buffer, int channel, int size, byte isBuffer) {
		buffer.putInt(channel);
		buffer.putInt(size);
		buffer.put(isBuffer);
	}

	private void readBufferOrEventMeta(ByteBuffer buffer) {
		// ignore channel.
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		buffer.getInt();
		size = buffer.getInt();
		isBuffer = buffer.get() == 0;
	}
}

