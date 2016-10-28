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
package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper around a FSDataInputStream to limit the maximum read offset.
 *
 * Based on the implementation from org.apache.commons.io.input.BoundedInputStream
 */
public class BoundedInputStream extends InputStream {
	private final FSDataInputStream delegate;
	private long endOffsetExclusive;
	private long position;
	private long mark;

	public BoundedInputStream(FSDataInputStream delegate, long endOffsetExclusive) throws IOException {
		this.position = delegate.getPos();
		this.mark = -1L;
		this.endOffsetExclusive = endOffsetExclusive;
		this.delegate = delegate;
	}

	public int read() throws IOException {
		if (endOffsetExclusive >= 0L && position >= endOffsetExclusive) {
			return -1;
		} else {
			int result = delegate.read();
			++position;
			return result;
		}
	}

	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) throws IOException {
		if (endOffsetExclusive >= 0L && position >= endOffsetExclusive) {
			return -1;
		} else {
			long maxRead = endOffsetExclusive >= 0L ? Math.min((long) len, endOffsetExclusive - position) : (long) len;
			int bytesRead = delegate.read(b, off, (int) maxRead);
			if (bytesRead == -1) {
				return -1;
			} else {
				position += (long) bytesRead;
				return bytesRead;
			}
		}
	}

	public long skip(long n) throws IOException {
		long toSkip = endOffsetExclusive >= 0L ? Math.min(n, endOffsetExclusive - position) : n;
		long skippedBytes = delegate.skip(toSkip);
		position += skippedBytes;
		return skippedBytes;
	}

	public int available() throws IOException {
		return endOffsetExclusive >= 0L && position >= endOffsetExclusive ? 0 : delegate.available();
	}

	public String toString() {
		return delegate.toString();
	}

	public void close() throws IOException {
		delegate.close();
	}

	public synchronized void reset() throws IOException {
		delegate.reset();
		position = mark;
	}

	public synchronized void mark(int readlimit) {
		delegate.mark(readlimit);
		mark = position;
	}

	public long getEndOffsetExclusive() {
		return endOffsetExclusive;
	}

	public void setEndOffsetExclusive(long endOffsetExclusive) {
		this.endOffsetExclusive = endOffsetExclusive;
	}

	public boolean markSupported() {
		return delegate.markSupported();
	}
}