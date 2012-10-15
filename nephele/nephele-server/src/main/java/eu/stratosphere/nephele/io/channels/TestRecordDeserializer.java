package eu.stratosphere.nephele.io.channels;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.types.Record;

public class TestRecordDeserializer<T extends Record> implements RecordDeserializer<T> {

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

	private Buffer lastBuffer = null;

	private DataInputWrapper wrapper = null;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readData(final T target, final ReadableByteChannel readableByteChannel) throws IOException {

		final Buffer buffer = (Buffer) readableByteChannel;

		if (buffer != this.lastBuffer) {
			this.wrapper = new DataInputWrapper(((MemoryBuffer) buffer).getByteBuffer());
			this.lastBuffer = buffer;
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
