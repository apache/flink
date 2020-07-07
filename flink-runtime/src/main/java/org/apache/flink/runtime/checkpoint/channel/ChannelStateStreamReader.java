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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult;
import org.apache.flink.runtime.checkpoint.channel.RefCountingFSDataInputStream.RefCountingFSDataInputStreamFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult.HAS_MORE_DATA;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult.NO_MORE_DATA;

/**
 * Reads the state of a single channel pointed by {@link org.apache.flink.runtime.state.AbstractChannelStateHandle AbstractChannelStateHandle}.
 * Once all data is read, this class can't be used anymore.
 * Uses {@link RefCountingFSDataInputStream} internally.
 */
@NotThreadSafe
class ChannelStateStreamReader implements Closeable {

	private final RefCountingFSDataInputStream stream;
	private final ChannelStateSerializer serializer;
	private final Queue<Long> offsets;
	private int remainingBytes = -1;
	private boolean closed = false;

	ChannelStateStreamReader(AbstractChannelStateHandle<?> handle, RefCountingFSDataInputStreamFactory streamFactory) {
		this(streamFactory.getOrCreate(handle), handle.getOffsets(), streamFactory.getSerializer());
	}

	private ChannelStateStreamReader(RefCountingFSDataInputStream stream, List<Long> offsets, ChannelStateSerializer serializer) {
		this.stream = stream;
		this.stream.incRef();
		this.serializer = serializer;
		this.offsets = new LinkedList<>(offsets);
	}

	ReadResult readInto(Buffer buffer) throws IOException {
		return readInto(wrap(buffer));
	}

	ReadResult readInto(BufferBuilder bufferBuilder) throws IOException {
		return readInto(wrap(bufferBuilder));
	}

	private ReadResult readInto(ChannelStateByteBuffer buffer) throws IOException {
		Preconditions.checkState(!closed, "reader is closed");
		readWhilePossible(buffer);
		if (haveMoreData()) {
			return HAS_MORE_DATA;
		} else {
			closed = true;
			stream.decRef();
			return NO_MORE_DATA;
		}
	}

	private void readWhilePossible(ChannelStateByteBuffer buffer) throws IOException {
		while (haveMoreData() && buffer.isWritable()) {
			if (remainingBytes <= 0) {
				advanceOffset();
			}
			int bytesRead = serializer.readData(stream, buffer, remainingBytes);
			remainingBytes -= bytesRead;
		}
	}

	private boolean haveMoreData() {
		return remainingBytes > 0 || !offsets.isEmpty();
	}

	@SuppressWarnings("ConstantConditions")
	private void advanceOffset() throws IOException {
		stream.seek(offsets.poll());
		remainingBytes = serializer.readLength(stream);
	}

	@Override
	public void close() throws IOException {
		if (!closed) {
			closed = true;
			stream.close();
		}
	}
}
