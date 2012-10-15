package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.MemoryBuffer;
import eu.stratosphere.nephele.types.Record;

final class DefaultRecordSerializer<T extends Record> implements RecordSerializer<T> {

	private static final class DataOutputWrapper implements DataOutput {

		private final ByteBuffer buffer;

		private int written = 0;

		private DataOutputWrapper(final ByteBuffer buffer) {
			this.buffer = buffer;
		}

		@Override
		public void write(final int b) throws IOException {
			this.buffer.put((byte) b);
			++this.written;
		}

		@Override
		public void write(final byte[] b) throws IOException {
			this.buffer.put(b);
			this.written += b.length;
		}

		@Override
		public void write(final byte[] b, final int off, final int len) throws IOException {
			this.buffer.put(b, off, len);
			this.written += len;
		}

		@Override
		public void writeBoolean(final boolean v) throws IOException {
			this.buffer.put((byte) (v ? 1 : 0));
			++this.written;
		}

		@Override
		public void writeByte(final int v) throws IOException {
			this.buffer.put((byte) v);
			++this.written;
		}

		@Override
		public void writeShort(final int v) throws IOException {
			this.buffer.putShort((short) v);
			this.written += 2;
		}

		@Override
		public void writeChar(final int v) throws IOException {
			this.buffer.putChar((char) v);
			this.written += 2;
		}

		@Override
		public void writeInt(final int v) throws IOException {
			this.buffer.putInt(v);
			this.written += 4;
		}

		@Override
		public void writeLong(final long v) throws IOException {
			this.buffer.putLong(v);
			this.written += 8;
		}

		@Override
		public void writeFloat(final float v) throws IOException {
			this.buffer.putFloat(v);
			this.written += 4;
		}

		@Override
		public void writeDouble(final double v) throws IOException {
			this.buffer.putDouble(v);
			this.written += 8;
		}

		@Override
		public void writeBytes(final String s) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void writeChars(final String s) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void writeUTF(final String s) throws IOException {
			throw new UnsupportedOperationException();
		}

		private int getAndResetWritten() {

			final int retVal = this.written;
			this.written = 0;

			return retVal;
		}
	}

	private T nextRecordToSerialize = null;

	private Buffer lastBuffer = null;

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
	public boolean read(final Buffer buffer) throws IOException {

		final T record = this.nextRecordToSerialize;
		if (record == null) {
			return true;
		}

		if (buffer != this.lastBuffer) {

			if (!buffer.isBackedByMemory()) {
				throw new IllegalStateException("This record serializer only works with memory backed buffers");
			}

			this.wrapper = new DataOutputWrapper(((MemoryBuffer) buffer).getByteBuffer());
			this.lastBuffer = buffer;
		}

		try {
			record.write(this.wrapper);
		} catch (BufferOverflowException e) {
			// Correct the position pointer
			this.wrapper.buffer.position(this.wrapper.buffer.position() - this.wrapper.getAndResetWritten());
			return false;
		}

		this.nextRecordToSerialize = null;
		this.wrapper.getAndResetWritten();

		return true;
	}

	@Override
	public void clear() {
	}

}
