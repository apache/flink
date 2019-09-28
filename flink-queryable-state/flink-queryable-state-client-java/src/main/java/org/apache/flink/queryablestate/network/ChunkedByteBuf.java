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

package org.apache.flink.queryablestate.network;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedInput;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;

/**
 * A {@link ByteBuf} instance to be consumed in chunks by {@link ChunkedWriteHandler},
 * respecting the high and low watermarks.
 *
 * @see <a href="http://normanmaurer.me/presentations/2014-facebook-eng-netty/slides.html#10.0">Low/High Watermarks</a>
 */
@Internal
public class ChunkedByteBuf implements ChunkedInput<ByteBuf> {

	/** The buffer to chunk. */
	private final ByteBuf buf;

	/** Size of chunks. */
	private final int chunkSize;

	/** Closed flag. */
	private boolean isClosed;

	/** End of input flag. */
	private boolean isEndOfInput;

	public ChunkedByteBuf(ByteBuf buf, int chunkSize) {
		this.buf = Preconditions.checkNotNull(buf, "Buffer");
		Preconditions.checkArgument(chunkSize > 0, "Non-positive chunk size");
		this.chunkSize = chunkSize;
	}

	@Override
	public boolean isEndOfInput() throws Exception {
		return isClosed || isEndOfInput;
	}

	@Override
	public void close() throws Exception {
		if (!isClosed) {
			// If we did not consume the whole buffer yet, we have to release
			// it here. Otherwise, it's the responsibility of the consumer.
			if (!isEndOfInput) {
				buf.release();
			}

			isClosed = true;
		}
	}

	@Override
	public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
		return readChunk();
	}

	@Override
	public ByteBuf readChunk(ByteBufAllocator byteBufAllocator) throws Exception {
		return readChunk();
	}

	private ByteBuf readChunk() {
		if (isClosed) {
			return null;
		} else if (buf.readableBytes() <= chunkSize) {
			isEndOfInput = true;

			// Don't retain as the consumer is responsible to release it
			return buf.slice();
		} else {
			// Return a chunk sized slice of the buffer. The ref count is
			// shared with the original buffer. That's why we need to retain
			// a reference here.
			return buf.readSlice(chunkSize).retain();
		}
	}

	@Override
	public long length() {
		return -1;
	}

	@Override
	public long progress() {
		return buf.readerIndex();
	}

	@Override
	public String toString() {
		return "ChunkedByteBuf{" +
				"buf=" + buf +
				", chunkSize=" + chunkSize +
				", isClosed=" + isClosed +
				", isEndOfInput=" + isEndOfInput +
				'}';
	}
}
