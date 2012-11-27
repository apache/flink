/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.io.channels;

import java.io.DataInput;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.io.RecordFactory;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.types.Record;

public class DefaultRecordDeserializer<T extends Record> implements RecordDeserializer<T> {

	private static final class FileInputWrapper implements DataInput {

		private ByteBuffer buf;

		private final ReadableByteChannel readableByteChannel;

		private FileInputWrapper(final ReadableByteChannel readableByteChannel) {
			this.readableByteChannel = readableByteChannel;
			this.buf = ByteBuffer.allocate(1024);
			this.buf.limit(0);
		}

		private void ensureAvailable(final int numberOfBytes) throws IOException {

			// Check if buffer is large enough
			if (this.buf.capacity() < numberOfBytes) {
				final ByteBuffer newBuf = ByteBuffer.allocate(numberOfBytes);
				newBuf.put(this.buf);
				newBuf.flip();
				this.buf = newBuf;
			}

			// Check if enough bytes are remaining in buffer
			int remaining = this.buf.remaining();
			while (remaining < numberOfBytes) {

				final int offset = this.buf.position();
				if (offset > 0) {
					for (int i = 0; i < remaining; ++i) {
						this.buf.put(i, this.buf.get(offset + i));
					}
				}
				this.buf.position(remaining);
				this.buf.limit(this.buf.capacity());

				final int read = this.readableByteChannel.read(this.buf);
				if (read < 0) {
					throw new BufferUnderflowException();
				}

				this.buf.flip();
				remaining = this.buf.remaining();
			}
		}

		@Override
		public void readFully(byte[] b) throws IOException {

			ensureAvailable(b.length);
			this.buf.get(b);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {

			ensureAvailable(len);
			this.buf.get(b, off, len);
		}

		@Override
		public int skipBytes(int n) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean readBoolean() throws IOException {

			ensureAvailable(1);
			return (this.buf.get() == 1);
		}

		@Override
		public byte readByte() throws IOException {

			ensureAvailable(1);

			return this.buf.get();
		}

		@Override
		public int readUnsignedByte() throws IOException {

			ensureAvailable(1);

			return (this.buf.get() & 0xFF);
		}

		@Override
		public short readShort() throws IOException {

			ensureAvailable(2);

			return this.buf.getShort();
		}

		@Override
		public int readUnsignedShort() throws IOException {

			ensureAvailable(2);

			return (this.buf.getShort() & 0xFFFF);
		}

		@Override
		public char readChar() throws IOException {

			ensureAvailable(2);

			return this.buf.getChar();
		}

		@Override
		public int readInt() throws IOException {

			ensureAvailable(4);

			return this.buf.getInt();
		}

		@Override
		public long readLong() throws IOException {

			ensureAvailable(8);

			return this.buf.getLong();
		}

		@Override
		public float readFloat() throws IOException {

			ensureAvailable(4);

			return this.buf.getFloat();
		}

		@Override
		public double readDouble() throws IOException {

			ensureAvailable(8);

			return this.buf.getDouble();
		}

		@Override
		public String readLine() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public String readUTF() throws IOException {

			final int utf8Length = readUnsignedShort();
			final char[] data = new char[utf8Length];
			int count = 0;

			if (utf8Length == 0) {
				return "";
			}

			ensureAvailable(utf8Length);

			final ByteBuffer buf = this.buf;

			int char1, char2, char3;
			for (int i = 0; i < utf8Length; ++i) {

				char1 = (int) (buf.get() & 0xFF);
				if ((char1 & 0x80) == 0) {
					data[count++] = (char) char1;
				} else if ((char1 & 0x20) == 0) {
					char2 = (int) (buf.get() & 0xFF);
					data[count++] = (char) (((char1 & 0x1F) << 6) | (char2 & 0x3F));
					++i;
				} else {
					char2 = (int) (buf.get() & 0xFF);
					char3 = (int) (buf.get() & 0xFF);
					data[count++] = (char) (((char1 & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
					i += 2;
				}
			}

			return new String(data, 0, count);
		}

	}

	private static final class MemoryInputWrapper implements DataInput {

		private final ByteBuffer buffer;

		private MemoryInputWrapper(final ByteBuffer buffer) {
			this.buffer = buffer;
		}

		@Override
		public void readFully(byte[] b) throws IOException {
			this.buffer.get(b);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			this.buffer.get(b, off, len);
		}

		@Override
		public int skipBytes(int n) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean readBoolean() throws IOException {
			return (this.buffer.get() == 1);
		}

		@Override
		public byte readByte() throws IOException {
			return this.buffer.get();
		}

		@Override
		public int readUnsignedByte() throws IOException {
			return (this.buffer.get() & 0xFF);
		}

		@Override
		public short readShort() throws IOException {
			return this.buffer.getShort();
		}

		@Override
		public int readUnsignedShort() throws IOException {
			return (this.buffer.getShort() & 0xFFFF);
		}

		@Override
		public char readChar() throws IOException {
			return this.buffer.getChar();
		}

		@Override
		public int readInt() throws IOException {
			return this.buffer.getInt();
		}

		@Override
		public long readLong() throws IOException {
			return this.buffer.getLong();
		}

		@Override
		public float readFloat() throws IOException {
			return this.buffer.getFloat();
		}

		@Override
		public double readDouble() throws IOException {
			return this.buffer.getDouble();
		}

		@Override
		public String readLine() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public String readUTF() throws IOException {

			final ByteBuffer buf = this.buffer;
			final int utf8Length = readUnsignedShort();
			final char[] data = new char[utf8Length];
			int count = 0;

			if (utf8Length == 0) {
				return "";
			}

			int char1, char2, char3;
			for (int i = 0; i < utf8Length; ++i) {

				char1 = (int) (buf.get() & 0xFF);
				if ((char1 & 0x80) == 0) {
					data[count++] = (char) char1;
				} else if ((char1 & 0x20) == 0) {
					char2 = (int) (buf.get() & 0xFF);
					data[count++] = (char) (((char1 & 0x1F) << 6) | (char2 & 0x3F));
					++i;
				} else {
					char2 = (int) (buf.get() & 0xFF);
					char3 = (int) (buf.get() & 0xFF);
					data[count++] = (char) (((char1 & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
					i += 2;
				}
			}

			return new String(data, 0, count);
		}
	}

	private final RecordFactory<T> recordFactory;

	private ReadableByteChannel lastReadableByteChannel = null;

	private DataInput dataInput = null;

	public DefaultRecordDeserializer(final RecordFactory<T> recordFactory) {
		this.recordFactory = recordFactory;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readData(T target, final ReadableByteChannel readableByteChannel) throws IOException {

		if (readableByteChannel != this.lastReadableByteChannel) {
			final Buffer buffer = (Buffer) readableByteChannel;
			if (buffer.isBackedByMemory()) {
				this.dataInput = new MemoryInputWrapper(((MemoryBuffer) buffer).getByteBuffer());
			} else {
				this.dataInput = new FileInputWrapper(buffer);
			}
			this.lastReadableByteChannel = readableByteChannel;
		}

		if (target == null) {
			target = this.recordFactory.createRecord();
		}

		try {
			target.read(this.dataInput);
		} catch (BufferUnderflowException e) {
			return null;
		}

		return target;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		this.dataInput = null;
		this.lastReadableByteChannel = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasUnfinishedData() {

		return false;
	}

}
