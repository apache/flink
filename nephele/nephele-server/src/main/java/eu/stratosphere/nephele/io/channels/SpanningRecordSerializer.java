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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import eu.stratosphere.nephele.io.DataOutputBuffer;
import eu.stratosphere.nephele.types.Record;

final class SpanningRecordSerializer<T extends Record> implements RecordSerializer<T> {

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
	public boolean read(final WritableByteChannel writableByteChannel) throws IOException {

		// Deal with length buffer first
		if (this.lengthBuf.hasRemaining()) { // There is data from the length buffer to be written
			writableByteChannel.write(this.lengthBuf);
		}

		final int bytesReadFromSerializationBuf = writableByteChannel.write(this.serializationBuffer.getData());
		// byteBuffer.put(this.serializationBuffer.getData(), this.bytesReadFromBuffer, length);
		this.bytesReadFromBuffer += bytesReadFromSerializationBuf;

		if (leftInSerializationBuffer() == 0) { // Record is entirely written to byteBuffer
			this.serializationBuffer.reset();
			this.bytesReadFromBuffer = 0;
		}

		return (bytesReadFromSerializationBuf > 0);
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
