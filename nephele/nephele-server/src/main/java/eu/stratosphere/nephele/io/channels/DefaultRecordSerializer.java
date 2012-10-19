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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.WritableByteChannel;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.types.Record;

/**
 * This is the default implementation to transform {@link Record} objects into sequences of bytes. The implementation is
 * optimized for speed. As a result, it does not consider special cases in which individual records may not fit into a
 * single buffer. In these cases, the {@link SpanningRecordSerializer} should be used.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of record to be serialized with this record serializer
 */
final class DefaultRecordSerializer<T extends Record> implements RecordSerializer<T> {

	/**
	 * Auxiliary class to make a {@link ByteBuffer} accessible through a {@link DataOutput} interface.
	 * <p>
	 * This class is not thread-safe.
	 * 
	 * @author warneke
	 */
	private static final class DataOutputWrapper implements DataOutput {

		/**
		 * The wrapped byte buffer.
		 */
		private final ByteBuffer buffer;

		/**
		 * The number of bytes written to the buffer during the serialization of a single record.
		 */
		private int written = 0;

		/**
		 * Constructs a new data output wrapper.
		 * 
		 * @param buffer
		 *        the byte buffer to be wrapped
		 */
		private DataOutputWrapper(final ByteBuffer buffer) {
			this.buffer = buffer;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(final int b) throws IOException {
			this.buffer.put((byte) b);
			++this.written;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(final byte[] b) throws IOException {
			this.buffer.put(b);
			this.written += b.length;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(final byte[] b, final int off, final int len) throws IOException {
			this.buffer.put(b, off, len);
			this.written += len;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeBoolean(final boolean v) throws IOException {
			this.buffer.put((byte) (v ? 1 : 0));
			++this.written;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeByte(final int v) throws IOException {
			this.buffer.put((byte) v);
			++this.written;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeShort(final int v) throws IOException {
			this.buffer.putShort((short) (v & 0xFFFF));
			this.written += 2;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeChar(final int v) throws IOException {
			this.buffer.putChar((char) v);
			this.written += 2;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeInt(final int v) throws IOException {
			this.buffer.putInt(v);
			this.written += 4;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeLong(final long v) throws IOException {
			this.buffer.putLong(v);
			this.written += 8;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeFloat(final float v) throws IOException {
			this.buffer.putFloat(v);
			this.written += 4;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeDouble(final double v) throws IOException {
			this.buffer.putDouble(v);
			this.written += 8;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeBytes(final String s) throws IOException {

			if (s == null) {
				throw new NullPointerException();
			}

			final int len = s.length();
			final ByteBuffer buf = this.buffer;

			if ((len + 2) > buf.remaining()) {
				throw new BufferOverflowException();
			}

			// Write length
			buf.put((byte) ((len >>> 8) & 0xFF));
			buf.put((byte) (len & 0xFF));

			// Write string
			for (int i = 0; i < len; ++i) {
				buf.put((byte) (s.charAt(i) & 0xFF));
			}

			this.written += len + 2;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeChars(final String s) throws IOException {

			if (s == null) {
				throw new NullPointerException();
			}

			final ByteBuffer buf = this.buffer;

			final int len = s.length();
			if ((len * 2 + 2) > buf.remaining()) {
				throw new BufferOverflowException();
			}

			// Write length
			buf.put((byte) ((len >>> 8) & 0xFF));
			buf.put((byte) (len & 0xFF));

			final CharBuffer charBuffer = CharBuffer.wrap(s);
			buf.asCharBuffer().put(charBuffer);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void writeUTF(final String s) throws IOException {

			if (s == null) {
				throw new NullPointerException();
			}

			final int len = s.length();
			final ByteBuffer buf = this.buffer;
			int ch, uft8Length = 0;

			// Compute length of encoded string
			for (int i = 0; i < len; ++i) {
				ch = s.charAt(i);
				if ((ch >= 0x0001) && (ch <= 0x007F)) {
					++uft8Length;
				} else if (ch > 0x07FF) {
					uft8Length += 3;
				} else {
					uft8Length += 2;
				}
			}

			if (uft8Length > 65535) {
				throw new IOException("String too long to encode in UTF-8");
			}

			if ((uft8Length + 2) > buf.remaining()) {
				throw new BufferOverflowException();
			}

			// Write length
			buf.put((byte) ((uft8Length >>> 8) & 0xFF));
			buf.put((byte) (uft8Length & 0xFF));

			// Encode actual string
			for (int i = 0; i < len; i++) {
				ch = s.charAt(i);
				if ((ch >= 0x0001) && (ch <= 0x007F)) {
					buf.put((byte) ch);
				} else if (ch > 0x07FF) {
					buf.put((byte) (0xE0 | ((ch >> 12) & 0x0F)));
					buf.put((byte) (0x80 | ((ch >> 6) & 0x3F)));
					buf.put((byte) (0x80 | (ch & 0x3F)));
				} else {
					buf.put((byte) (0xC0 | ((ch >> 6) & 0x1F)));
					buf.put((byte) (0x80 | (ch & 0x3F)));
				}
			}

			this.written += uft8Length + 2;
		}
	}

	/**
	 * Stores the next record to be serialized.
	 */
	private T nextRecordToSerialize = null;

	/**
	 * Stores the last channel that was written to.
	 */
	private WritableByteChannel lastWritableByteChannel = null;

	/**
	 * The current data output wrapper.
	 */
	private DataOutputWrapper wrapper = null;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void serialize(final T record) throws IOException {

		if (this.nextRecordToSerialize != null) {
			throw new IOException("The previous record has not been fully serialized yet");
		}

		this.nextRecordToSerialize = record;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean dataLeftFromPreviousSerialization() {
		return (this.nextRecordToSerialize != null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean read(final WritableByteChannel writableByteChannel) throws IOException {

		final T record = this.nextRecordToSerialize;
		if (record == null) {
			return true;
		}

		if (writableByteChannel != this.lastWritableByteChannel) {

			final Buffer buffer = (Buffer) writableByteChannel;

			if (!buffer.isBackedByMemory()) {
				throw new IllegalStateException("This record serializer only works with memory backed buffers");
			}

			this.wrapper = new DataOutputWrapper(((MemoryBuffer) buffer).getByteBuffer());
			this.lastWritableByteChannel = writableByteChannel;
		}

		try {
			record.write(this.wrapper);
		} catch (BufferOverflowException e) {
			// Correct the position pointer
			this.wrapper.buffer.position(this.wrapper.buffer.position() - this.wrapper.written);
			this.wrapper.written = 0;

			if (this.wrapper.buffer.position() == 0) {
				throw new IOException("Record " + this.nextRecordToSerialize + " is too large to be serialized");
			}

			return false;
		}

		this.nextRecordToSerialize = null;
		this.wrapper.written = 0;

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		this.nextRecordToSerialize = null;
		this.lastWritableByteChannel = null;
		this.wrapper = null;
	}

}
