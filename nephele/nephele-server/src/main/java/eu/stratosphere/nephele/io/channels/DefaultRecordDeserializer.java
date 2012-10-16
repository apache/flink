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

package eu.stratosphere.nephele.io.channels;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.io.RecordFactory;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.types.Record;

public class DefaultRecordDeserializer<T extends Record> implements RecordDeserializer<T> {

	private static final class DataInputWrapper implements DataInput {

		private final ByteBuffer buffer;

		private DataInputWrapper(final ByteBuffer buffer) {
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
			throw new UnsupportedOperationException();
		}

		@Override
		public short readShort() throws IOException {
			return this.buffer.getShort();
		}

		@Override
		public int readUnsignedShort() throws IOException {
			throw new UnsupportedOperationException();
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
			throw new UnsupportedOperationException();
		}

	}

	private final RecordFactory<T> recordFactory;

	private Buffer lastBuffer = null;

	private DataInputWrapper wrapper = null;

	public DefaultRecordDeserializer(final RecordFactory<T> recordFactory) {
		this.recordFactory = recordFactory;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readData(T target, final ReadableByteChannel readableByteChannel) throws IOException {

		final Buffer buffer = (Buffer) readableByteChannel;

		if (buffer != this.lastBuffer) {
			this.wrapper = new DataInputWrapper(((MemoryBuffer) buffer).getByteBuffer());
			this.lastBuffer = buffer;
		}

		if (target == null) {
			target = this.recordFactory.createRecord();
		}

		target.read(this.wrapper);

		return target;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasUnfinishedData() {

		return false;
	}

}
