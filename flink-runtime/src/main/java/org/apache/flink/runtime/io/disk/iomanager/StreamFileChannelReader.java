/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.NioBufferedFileInputStream;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Helper class to read {@link Buffer}s from files into objects.
 * Notice that the data in the file is a sequence of stream, and don't have any header to separate each buffer
 * compared to {@link BufferFileChannelReader}.
 */
final public class StreamFileChannelReader {

	public static boolean readBufferFromFileChannel(
		FileChannel fileChannel, Buffer buffer, long offset, long length, File file)
		throws IOException {

		final long currPosition = fileChannel.position();
		// We cannot afford the overhead of seeking before reading each buffer,
		// so make sure the sequence of each ReadRequest.
		if (currPosition != offset) {
			throw new IllegalStateException("Expect fileChannel's position to be " + offset
				+ ", but the current position is " + currPosition + ", fileId: " + file);
		}
		// Make sure this buffer can hold the required data size.
		checkArgument(buffer.getSize() == 0, "Buffer not empty");
		if (length > buffer.getMaxCapacity()) {
			throw new IllegalStateException("Buffer is too small for data: " + buffer.getMaxCapacity()
				+ " bytes available, but " + length
				+ " needed. This is most likely due to an serialized event, which is larger than the buffer size.");
		}
		// Make sure this fileChannel has enough data to be read.
		if ((fileChannel.size() - currPosition) < length) {
			throw new IllegalStateException("Cannot read " + length + "bytes from this file, file size: "
				+ fileChannel.size() + ", current position: " + currPosition + ", fileId: " + file);
		}

		fileChannel.read(buffer.getNioBuffer(0, (int) length));
		buffer.setSize((int) length);
		return fileChannel.size() - fileChannel.position() == 0;
	}

	public static boolean readBufferFromFileBufferedChannel(
		NioBufferedFileInputStream in, Buffer buffer, long offset, long length, File file
	) throws IOException {

		final long currPosition = in.position();
		// We cannot afford the overhead of seeking before reading each buffer,
		// so make sure the sequence of each ReadRequest.
		if (currPosition != offset) {
			throw new IllegalStateException("Expect fileChannel's position to be " + offset
				+ ", but the current position is " + currPosition + ", fileId: " + file);
		}
		// Make sure this buffer can hold the required data size.
		checkArgument(buffer.getSize() == 0, "Buffer not empty");
		if (length > buffer.getMaxCapacity()) {
			throw new IllegalStateException("Buffer is too small for data: " + buffer.getMaxCapacity()
				+ " bytes available, but " + length
				+ " needed. This is most likely due to an serialized event, which is larger than the buffer size.");
		}
		// Make sure this fileChannel has enough data to be read.
		if ((in.size() - currPosition) < length) {
			throw new IllegalStateException("Cannot read " + length + "bytes from this file, file size: "
				+ in.size() + ", current position: " + currPosition + ", fileId: " + file);
		}

		in.continuousRead(buffer.getNioBuffer(0, (int) length));
		buffer.setSize((int) length);
		return in.size() - in.position() == 0;
	}
}
