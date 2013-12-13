/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.nephele.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A reusable {@link DataOutput} implementation that writes to an in-memory
 * buffer.
 * <p>
 * This saves memory over creating a new DataOutputStream and ByteArrayOutputStream each time data is written.
 * <p>
 * Typical usage is something like the following:
 * 
 * <pre>
 * 
 * DataOutputBuffer buffer = new DataOutputBuffer();
 * while (... loop condition ...) {
 *   buffer.reset();
 *   ... write buffer using DataOutput methods ...
 *   byte[] data = buffer.getData();
 *   int dataLength = buffer.getLength();
 *   ... write data to its ultimate destination ...
 * }
 * </pre>
 */
public class DataOutputBuffer extends DataOutputStream {

	private static class ByteBufferedOutputStream extends OutputStream {

		private ByteBuffer buf;

		public ByteBuffer getData() {
			return this.buf;
		}

		public int getLength() {
			return this.buf.limit();
		}

		public ByteBufferedOutputStream() {
			this(1024);
		}

		public ByteBufferedOutputStream(int size) {
			this.buf = ByteBuffer.allocate(size);
			this.buf.position(0);
			this.buf.limit(0);
		}

		public void reset() {
			this.buf.position(0);
			this.buf.limit(0);
		}

		public void write(DataInput in, int len) throws IOException {

			final int newcount = this.buf.limit() + len;
			if (newcount > this.buf.capacity()) {
				final ByteBuffer newBuf = ByteBuffer.allocate(Math.max(this.buf.capacity() << 1, newcount));
				newBuf.position(0);
				System.arraycopy(this.buf.array(), 0, newBuf.array(), 0, this.buf.limit());
				newBuf.limit(this.buf.limit());
				this.buf = newBuf;
			}

			in.readFully(this.buf.array(), this.buf.limit(), len);
			this.buf.limit(newcount);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {

			final int newcount = this.buf.limit() + len;
			if (newcount > this.buf.capacity()) {
				increaseInternalBuffer(newcount);
			}

			System.arraycopy(b, off, this.buf.array(), this.buf.limit(), len);
			this.buf.limit(newcount);
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(int arg0) throws IOException {

			final int oldLimit = this.buf.limit();
			final int newLimit = oldLimit + 1;

			if (newLimit > this.buf.capacity()) {
				increaseInternalBuffer(newLimit);
			}

			this.buf.limit(newLimit);
			this.buf.put(oldLimit, (byte) arg0);
		}

		private void increaseInternalBuffer(int minimumRequiredSize) {
			final ByteBuffer newBuf = ByteBuffer.allocate(Math.max(this.buf.capacity() << 1, minimumRequiredSize));
			newBuf.position(0);
			System.arraycopy(this.buf.array(), 0, newBuf.array(), 0, this.buf.limit());
			newBuf.limit(this.buf.limit());
			this.buf = newBuf;
		}
	}

	private final ByteBufferedOutputStream byteBufferedOutputStream;

	/** Constructs a new empty buffer. */
	public DataOutputBuffer() {
		this(new ByteBufferedOutputStream());
	}

	public DataOutputBuffer(int size) {
		this(new ByteBufferedOutputStream(size));
	}

	private DataOutputBuffer(ByteBufferedOutputStream byteBufferedOutputStream) {
		super(byteBufferedOutputStream);
		this.byteBufferedOutputStream = byteBufferedOutputStream;
	}

	public ByteBuffer getData() {
		return this.byteBufferedOutputStream.getData();
	}

	public int getLength() {
		return this.byteBufferedOutputStream.getLength();
	}

	/** Resets the buffer to empty. */
	public DataOutputBuffer reset() {
		this.byteBufferedOutputStream.reset();
		return this;
	}

	public void write(DataInput in, int length) throws IOException {
		this.byteBufferedOutputStream.write(in, length);
	}
}
