package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.MemoryBuffer;
import eu.stratosphere.nephele.types.Record;

final class DefaultRecordSerializer<T extends Record> implements RecordSerializer<T> {

	private static final class DataOutputWrapper implements DataOutput {

		private final ByteBuffer buffer;

		private DataOutputWrapper(final ByteBuffer buffer) {
			this.buffer = buffer;
		}

		@Override
		public void write(final int b) throws IOException {
			this.buffer.put((byte) b);
		}

		@Override
		public void write(final byte[] b) throws IOException {
			this.buffer.put(b);
		}

		@Override
		public void write(final byte[] b, final int off, final int len) throws IOException {
			this.buffer.put(b, off, len);
		}

		@Override
		public void writeBoolean(final boolean v) throws IOException {
			this.buffer.put((byte) (v ? 1 : 0));
		}

		@Override
		public void writeByte(final int v) throws IOException {
			this.buffer.put((byte) v);
		}

		@Override
		public void writeShort(final int v) throws IOException {
			this.buffer.putShort((short) v);
		}

		@Override
		public void writeChar(final int v) throws IOException {
			this.buffer.putChar((char) v);
		}

		@Override
		public void writeInt(final int v) throws IOException {
			this.buffer.putInt(v);
		}

		@Override
		public void writeLong(final long v) throws IOException {
			this.buffer.putLong(v);
		}

		@Override
		public void writeFloat(final float v) throws IOException {
			this.buffer.putFloat(v);
		}

		@Override
		public void writeDouble(final double v) throws IOException {
			this.buffer.putDouble(v);
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

	}

	private T nextRecordToSerialize = null;

	private WritableByteChannel lastWritableByteChannel = null;

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
	public int read(final WritableByteChannel writableByteChannel) throws IOException {

		final T record = this.nextRecordToSerialize;
		if (record == null) {
			return 0;
		}

		if (writableByteChannel != this.lastWritableByteChannel) {

			final Buffer buffer = (Buffer) writableByteChannel;
			if (buffer.isBackedByMemory()) {
				throw new IllegalStateException("This record serializer only works with memory backed buffers");
			}

			this.wrapper = new DataOutputWrapper(((MemoryBuffer) buffer).getByteBuffer());
			this.lastWritableByteChannel = writableByteChannel;
		}

		record.write(this.wrapper);

		return 0;
	}

	@Override
	public void clear() {
	}

}
