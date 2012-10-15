package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.IOException;
import java.nio.ByteBuffer;

import eu.stratosphere.nephele.io.DataOutputBuffer;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.types.Record;

final class BufferSpanningRecordSerializer<T extends Record> implements RecordSerializer<T> {

	private static final int SIZEOFINT = 4;

	private DataOutputBuffer serializationBuffer = new DataOutputBuffer();

	private ByteBuffer lengthBuf = ByteBuffer.allocate(SIZEOFINT);

	private int bytesReadFromBuffer = 0;

	/**
	 * Translates an integer into an array of bytes.
	 * 
	 * @param val
	 *        The integer to be translated
	 * @param arr
	 *        The byte buffer to store the data of the integer
	 */
	private void integerToByteBuffer(final int val, final ByteBuffer byteBuffer) {

		for (int i = 0; i < SIZEOFINT; ++i) {
			final int shift = i << (SIZEOFINT - 1); // i * 8
			byteBuffer.put(SIZEOFINT - 1 - i, (byte) ((val & (0xff << shift)) >>> shift));
		}

		byteBuffer.position(0);
		byteBuffer.limit(SIZEOFINT);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean dataLeftFromPreviousSerialization() {
		return leftInSerializationBuffer() > 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean read(final Buffer buffer) throws IOException {

		// Deal with length buffer first
		if (this.lengthBuf.hasRemaining()) { // There is data from the length buffer to be written
			buffer.write(this.lengthBuf);
		}

		final int bytesReadFromSerializationBuf = buffer.write(this.serializationBuffer.getData());
		// byteBuffer.put(this.serializationBuffer.getData(), this.bytesReadFromBuffer, length);
		this.bytesReadFromBuffer += bytesReadFromSerializationBuf;

		if (leftInSerializationBuffer() == 0) { // Record is entirely written to byteBuffer
			this.serializationBuffer.reset();
			this.bytesReadFromBuffer = 0;
		}

		return buffer.hasRemaining();
	}

	/**
	 * Return the number of bytes that have not been read from the internal serialization
	 * buffer so far.
	 * 
	 * @return the number of bytes that have not been read from the internal serialization buffer so far
	 */
	private int leftInSerializationBuffer() {

		return (this.serializationBuffer.getLength() - this.bytesReadFromBuffer);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void serialize(final T record) throws IOException {

		// Check if there is data left in the buffer
		if (dataLeftFromPreviousSerialization()) {
			throw new IOException("Cannot write new data, " + leftInSerializationBuffer()
				+ " bytes still left from previous call");
		}

		record.write(this.serializationBuffer); // serializationBuffer grows dynamically

		// Now record is completely in serializationBuffer;
		integerToByteBuffer(this.serializationBuffer.getLength(), this.lengthBuf);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		this.bytesReadFromBuffer = 0;
		this.lengthBuf.clear();
		this.serializationBuffer.reset();
	}
}
