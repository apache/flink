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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;

/**
 * FSDataInputStream with Buffer, only support single thread.
 * <p>
 * The design of FSDataBufferedInputStream refers to {@link java.io.BufferedInputStream}.
 * Compared to {@link java.io.BufferedInputStream}, the seek and getPos interfaces are mainly added.
 */
public class FSDataBufferedInputStream extends FSDataInputStream {

	private static final int DEFAULT_BUFFER_SIZE = 8192;

	protected byte[] buf;

	// read offset of buf
	private int pos;

	// availed count of buf
	private int count;

	private final FSDataInputStream inputStream;

	private boolean closed;

	public FSDataBufferedInputStream(FSDataInputStream inputStream) {
		this(inputStream, DEFAULT_BUFFER_SIZE);
	}

	public FSDataBufferedInputStream(
		FSDataInputStream inputStream,
		int bufferSize) {
		this.inputStream = inputStream;

		Preconditions.checkState(bufferSize > 0, "bufferSize must > 0");
		this.buf = new byte[bufferSize];

		this.pos = 0;
		this.count = 0;
		this.closed = false;
	}

	@Override
	public void seek(long desired) throws IOException {
		long streamPos = inputStream.getPos();
		long bufStartPos = streamPos - count;
		if (bufStartPos <= desired && desired < streamPos) {
			this.pos = (int) (desired - bufStartPos);
			return;
		}
		inputStream.seek(desired);
		this.pos = 0;
		this.count = 0;
	}

	@Override
	public long getPos() throws IOException {
		int avail = count - pos;
		return inputStream.getPos() - avail;
	}

	@Override
	public int read() throws IOException {
		if (pos >= count) {
			fill();
			if (pos >= count) {
				return -1;
			}
		}
		return buf[pos++] & 0xff;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return 0;
		}

		int n = 0;
		for (; ; ) {
			int nread = read1(b, off + n, len - n);
			if (nread <= 0) {
				return (n == 0) ? nread : n;
			}
			n += nread;
			if (n >= len) {
				return n;
			}
			// if not closed but no bytes available, return
			InputStream input = inputStream;
			if (input != null && input.available() <= 0) {
				return n;
			}
		}
	}

	/**
	 * Read characters into a portion of an array, reading from the underlying
	 * stream at most once if necessary.
	 */
	private int read1(byte[] b, int off, int len) throws IOException {
		int avail = count - pos;
		if (avail <= 0) {
            /* If the requested length is at least as large as the buffer,
               do not bother to copy the bytes into the local buffer.
               In this way buffered streams will cascade harmlessly. */
			if (len >= buf.length) {
				return inputStream.read(b, off, len);
			}
			fill();
			avail = count - pos;
			if (avail <= 0) {
				return -1;
			}
		}
		int cnt = Math.min(avail, len);
		System.arraycopy(buf, pos, b, off, cnt);
		pos += cnt;
		return cnt;
	}

	@Override
	public long skip(long n) throws IOException {
		if (n <= 0) {
			return 0;
		}
		long avail = count - pos;

		if (avail <= 0) {
			// Fill in buffer to save bytes for reset
			fill();
			avail = count - pos;
			if (avail <= 0) {
				return 0;
			}
		}

		long skipped = Math.min(avail, n);
		pos += skipped;
		return skipped;
	}

	@Override
	public int available() throws IOException {
		int avail = count - pos;
		return inputStream.available() + avail;
	}

	@Override
	public void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		if (inputStream != null) {
			inputStream.close();
		}
	}

	@VisibleForTesting
	public int getBufferSize() {
		return buf.length;
	}

	private void fill() throws IOException {
		Preconditions.checkState(pos >= count);
		count = inputStream.read(buf);
		pos = 0;
	}

}
