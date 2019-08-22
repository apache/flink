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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Un-synchronized input stream using the given byte buffer.
 */
public class ByteBufferInputStreamWithPos extends InputStream {

	private ByteBuffer buffer;
	private int position;
	private int count;
	private int mark;

	ByteBufferInputStreamWithPos(ByteBuffer buffer, int offset, int length) {
		this.position = offset;
		this.buffer = buffer;
		this.mark = offset;
		this.count = offset + length;
	}

	@Override
	public int read() {
		return (position < count) ? 0xFF & (buffer.get(position++)) : -1;
	}

	@Override
	public int read(@Nonnull byte[] b, int off, int len) {
		Preconditions.checkNotNull(b);

		if (off < 0 || len < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}

		if (position >= count) {
			return -1; // signal EOF
		}

		int available = count - position;

		if (len > available) {
			len = available;
		}

		if (len <= 0) {
			return 0;
		}

		ByteBufferUtils.copyFromBufferToArray(buffer, b, position, off, len);
		position += len;
		return len;
	}

	@Override
	public long skip(long toSkip) {
		long remain = count - position;

		if (toSkip < remain) {
			remain = toSkip < 0 ? 0 : toSkip;
		}

		position += remain;
		return remain;
	}

	@Override
	public boolean markSupported() {
		return true;
	}

	@Override
	public void mark(int readAheadLimit) {
		mark = position;
	}

	@Override
	public void reset() {
		position = mark;
	}

	@Override
	public int available() {
		return count - position;
	}

	@Override
	public void close() {
	}

	void setPosition(long pos) {
		Preconditions.checkArgument(pos >= 0 && pos <= count, "Position out of bounds.");
		this.position = (int) pos;
	}
}
