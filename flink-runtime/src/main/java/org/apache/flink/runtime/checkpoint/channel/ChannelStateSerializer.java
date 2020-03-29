/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.addExact;
import static java.lang.Math.min;

interface ChannelStateSerializer {

	void writeHeader(DataOutputStream dataStream) throws IOException;

	void writeData(DataOutputStream stream, Buffer... flinkBuffers) throws IOException;
}

interface ChannelStateDeserializer {

	void readHeader(InputStream stream) throws IOException;

	int readLength(InputStream stream) throws IOException;

	int readData(InputStream stream, ChannelStateByteBuffer buffer, int bytes) throws IOException;
}

/**
 * Wrapper around various buffers to receive channel state data.
 */
@Internal
@NotThreadSafe
interface ChannelStateByteBuffer {

	boolean isWritable();

	/**
	 * Read up to <code>bytesToRead</code> bytes into this buffer from the given {@link InputStream}.
	 * @return     the total number of bytes read into this buffer.
	 */
	int writeBytes(InputStream input, int bytesToRead) throws IOException;

	static ChannelStateByteBuffer wrap(Buffer buffer) {
		return new ChannelStateByteBuffer() {

			private final ByteBuf byteBuf = buffer.asByteBuf();

			@Override
			public boolean isWritable() {
				return byteBuf.isWritable();
			}

			@Override
			public int writeBytes(InputStream input, int bytesToRead) throws IOException {
				return byteBuf.writeBytes(input, Math.min(bytesToRead, byteBuf.writableBytes()));
			}
		};
	}

	static ChannelStateByteBuffer wrap(BufferBuilder bufferBuilder) {
		final byte[] buf = new byte[1024];
		return new ChannelStateByteBuffer() {
			@Override
			public boolean isWritable() {
				return !bufferBuilder.isFull();
			}

			@Override
			public int writeBytes(InputStream input, int bytesToRead) throws IOException {
				int left = bytesToRead;
				for (int toRead = getToRead(left); toRead > 0; toRead = getToRead(left)) {
					int read = input.read(buf, 0, toRead);
					int copied = bufferBuilder.append(java.nio.ByteBuffer.wrap(buf, 0, read));
					Preconditions.checkState(copied == read);
					left -= read;
				}
				bufferBuilder.commit();
				return bytesToRead - left;
			}

			private int getToRead(int bytesToRead) {
				return min(bytesToRead, min(buf.length, bufferBuilder.getWritableBytes()));
			}
		};
	}

	static ChannelStateByteBuffer wrap(byte[] bytes) {
		return new ChannelStateByteBuffer() {
			private int written = 0;

			@Override
			public boolean isWritable() {
				return written < bytes.length;
			}

			@Override
			public int writeBytes(InputStream input, int bytesToRead) throws IOException {
				final int bytesRead = input.read(bytes, written, bytes.length - written);
				written += bytesRead;
				return bytesRead;
			}
		};
	}
}

class ChannelStateSerializerImpl implements ChannelStateSerializer, ChannelStateDeserializer {
	private static final int SERIALIZATION_VERSION = 0;

	@Override
	public void writeHeader(DataOutputStream dataStream) throws IOException {
		dataStream.writeInt(SERIALIZATION_VERSION);
	}

	@Override
	public void writeData(DataOutputStream stream, Buffer... flinkBuffers) throws IOException {
		stream.writeInt(getSize(flinkBuffers));
		for (Buffer buffer : flinkBuffers) {
			ByteBuf nettyByteBuf = buffer.asByteBuf();
			nettyByteBuf.getBytes(nettyByteBuf.readerIndex(), stream, nettyByteBuf.readableBytes());
		}
	}

	private int getSize(Buffer[] buffers) {
		int len = 0;
		for (Buffer buffer : buffers) {
			len = addExact(len, buffer.readableBytes());
		}
		return len;
	}

	@Override
	public void readHeader(InputStream stream) throws IOException {
		int version = readInt(stream);
		Preconditions.checkArgument(version == SERIALIZATION_VERSION, "unsupported version: " + version);
	}

	@Override
	public int readLength(InputStream stream) throws IOException {
		int len = readInt(stream);
		Preconditions.checkArgument(len >= 0, "negative state size");
		return len;
	}

	@Override
	public int readData(InputStream stream, ChannelStateByteBuffer buffer, int bytes) throws IOException {
		return buffer.writeBytes(stream, bytes);
	}

	private static int readInt(InputStream stream) throws IOException {
		return new DataInputStream(stream).readInt();
	}
}
