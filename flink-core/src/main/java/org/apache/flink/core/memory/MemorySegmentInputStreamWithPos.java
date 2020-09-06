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

package org.apache.flink.core.memory;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.InputStream;

/**
 * Un-synchronized input stream using the given memory segment.
 */
public class MemorySegmentInputStreamWithPos extends InputStream {

	private MemorySegment segment;
	private int position;
	private int count;
	private int mark;

	public MemorySegmentInputStreamWithPos(MemorySegment segment, int offset, int length) {
		setSegment(segment, offset, length);
	}

	@Override
	public int read() {
		return (position < count) ? 0xFF & (segment.get(position++)) : -1;
	}

	@Override
	public int read(@Nonnull byte[] b, int off, int len) {
		if (position >= count) {
			return -1; // signal EOF
		}
		if (len <= 0) {
			return 0;
		}

		final int numBytes = Math.min(count - position, len);

		segment.get(position, b, off, numBytes);
		position += numBytes;

		return numBytes;
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

	public int getPosition() {
		return position;
	}

	public void setPosition(int pos) {
		Preconditions.checkArgument(pos >= 0 && pos <= count, "Position out of bounds.");
		this.position = pos;
	}

	public void setSegment(MemorySegment segment, int offset, int length) {
		this.count = Math.min(segment.size(), offset + length);
		setPosition(offset);
		this.segment = segment;
		this.mark = offset;
	}
}
