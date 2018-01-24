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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An un-synchronized stream that connects a prepend byte array buffer to another wrapped {@link InputStream}.
 * The prepend buffer will be fully read before reading the wrapped input stream.
 */
@Internal
public class ByteArrayPrependedInputStream extends InputStream {

	/**
	 * The buffer prepended before the actual input stream.
	 * This will be fully read up to {@link #count} bytes, before
	 * the actual input stream is read.
	 */
	private final byte[] prependBuffer;

	/**
	 * The actual input stream to read, after the prepend buffer.
	 */
	private final InputStream wrappedInputStream;

	/**
	 * Current position in the prepend buffer.
	 * If the buffer is already fully read, the position will be equal to the buffer length.
	 */
	private int position;

	/**
	 * Marked position in the prepend buffer.
	 * A value of -1 indicates that the current marked position is already
	 * pass the prepend buffer, and is within the wrapped input stream.
	 */
	private int mark;

	/**
	 * Number of bytes to read from the prepend buffer, before continuing with the wrapped input stream.
	 */
	private int count;

	public ByteArrayPrependedInputStream(byte[] bufferedBytes, InputStream inputStream) {
		this(bufferedBytes, 0, bufferedBytes.length, inputStream);
	}

	public ByteArrayPrependedInputStream(byte[] prependBuffer, int offset, int length, InputStream inputStream) {
		this.prependBuffer = prependBuffer;
		this.wrappedInputStream = inputStream;
		this.position = offset;
		this.count = Math.min(offset + length, prependBuffer.length);
		this.mark = offset;

		// if marking is supported, we mark the wrapped input stream now
		// so that when we are reset, we do not reset the wrapped input stream
		// to a position before where it is now
		if (markSupported()) {
			inputStream.mark(-1);
		}
	}

	@Override
	public int read() throws IOException {
		return (position < count) ? (0xFF & prependBuffer[position++]) : wrappedInputStream.read();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		checkNotNull(b);

		if (off < 0 || len < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return 0;
		}

		int numAvailableBufferedBytes = count - position;

		if (numAvailableBufferedBytes <= 0) {
			// already pass the prepend buffer; just read from the input stream
			return wrappedInputStream.read(b, off, len);
		} else {
			if (len > numAvailableBufferedBytes) {
				// need to read from the buffer and the input stream
				System.arraycopy(prependBuffer, position, b, off, numAvailableBufferedBytes);
				position += numAvailableBufferedBytes;

				int numBytesFromInputStream = wrappedInputStream.read(
					b, off + numAvailableBufferedBytes, len - numAvailableBufferedBytes);

				return numBytesFromInputStream < 0
					? numAvailableBufferedBytes
					: numAvailableBufferedBytes + numBytesFromInputStream;
			} else {
				// bytes from just the buffer is enough to fulfill this read request
				System.arraycopy(prependBuffer, position, b, off, len);
				position += len;
				return len;
			}
		}
	}

	@Override
	public long skip(long n) throws IOException {
		long numAvailableBufferedBytes = count - position;

		if (numAvailableBufferedBytes <= 0) {
			// no prepend buffer bytes to skip
			return wrappedInputStream.skip(n);
		} else {
			if (n > numAvailableBufferedBytes) {
				// the remaining bytes in the buffer is fully skipped, as well as
				// skipping some bytes in the wrapped input stream
				position += numAvailableBufferedBytes;

				return numAvailableBufferedBytes + wrappedInputStream.skip(n - numAvailableBufferedBytes);
			} else {
				// only bytes in the buffer is skipped
				long skipped = n < 0 ? 0 : n;
				position += skipped;

				return skipped;
			}
		}
	}

	@Override
	public int available() throws IOException {
		int numAvailableBufferedBytes = count - position;

		return numAvailableBufferedBytes < 0
			? wrappedInputStream.available()
			: wrappedInputStream.available() + numAvailableBufferedBytes;
	}

	@Override
	public boolean markSupported() {
		return wrappedInputStream.markSupported();
	}

	@Override
	public void mark(int readlimit) {
		checkState(markSupported());

		if (count - position <= 0) {
			wrappedInputStream.mark(readlimit);
			mark = -1; // this indicates that the marked position is in wrapped the input stream
		} else {
			mark = position;
		}
	}

	@Override
	public void reset() throws IOException {
		checkState(markSupported());

		wrappedInputStream.reset();
		if (mark != -1) {
			position = mark;
		}
	}

	@Override
	public void close() throws IOException {
		wrappedInputStream.close();
	}
}
