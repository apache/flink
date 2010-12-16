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
import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * A class for serializing a record to its binary representation.
 * 
 * @author warneke
 * @param <T>
 *        the type of the record this serialization buffer can be used for
 */
public class SerializationBuffer<T extends IOReadableWritable> {

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
	private void integerToByteBuffer(int val, ByteBuffer byteBuffer) {

		for (int i = 0; i < SIZEOFINT; ++i) {
			int shift = i << (SIZEOFINT - 1); // i * 8
			byteBuffer.put(SIZEOFINT - 1 - i, (byte) ((val & (0xff << shift)) >>> shift));
		}

		byteBuffer.position(0);
		byteBuffer.limit(SIZEOFINT);
	}

	/**
	 * Return <code>true</code> if the internal serialization buffer still contains data.
	 * In this case the method serialize must not be called. If the internal buffer is empty
	 * the method return <code>false</code>
	 * 
	 * @return <code>true</code> if the internal serialization buffer still contains data, <code>false</code> it it is
	 *         empty
	 */
	public boolean dataLeftFromPreviousSerialization() {

		if (leftInSerializationBuffer() > 0)
			return true;

		return false;
	}

	/**
	 * Reads the internal serialization buffer and writes the data to the given {@link WritableByteChannel} byte
	 * channel.
	 * 
	 * @param writableByteChannel
	 *        the byte channel to write the serialized data to
	 * @return the number of bytes written the to given byte channel
	 * @throws IOException
	 *         thrown if an error occurs while writing to serialized data to the channel
	 */
	public int read(WritableByteChannel writableByteChannel) throws IOException {

		int bytesReadFromLengthBuf = 0;

		// Deal with length buffer first
		if (this.lengthBuf.hasRemaining()) { // There is data from the length buffer to be written
			bytesReadFromLengthBuf = writableByteChannel.write(this.lengthBuf);
		}

		final int bytesReadFromSerializationBuf = writableByteChannel.write(this.serializationBuffer.getData());
		// byteBuffer.put(this.serializationBuffer.getData(), this.bytesReadFromBuffer, length);
		this.bytesReadFromBuffer += bytesReadFromSerializationBuf;

		if (leftInSerializationBuffer() == 0) { // Record is entirely written to byteBuffer
			this.serializationBuffer.reset();
			this.bytesReadFromBuffer = 0;
		}

		return (bytesReadFromSerializationBuf + bytesReadFromLengthBuf);
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
	 * Serializes the record and writes it to an internal buffer. The buffer grows dynamically
	 * in case more memory is required to serialization.
	 * 
	 * @param record
	 *        The record to the serialized
	 * @throws IOException
	 *         Thrown if data from a previous serialization process is still in the internal buffer and has not yet been
	 *         transfered to a byte buffer
	 */
	public void serialize(T record) throws IOException {

		// Check if there is data left in the buffer
		if (dataLeftFromPreviousSerialization())
			throw new IOException("Cannot write new data, " + leftInSerializationBuffer()
				+ " bytes still left from previous call");

		record.write(this.serializationBuffer); // serializationBuffer grows dynamically

		// Now record is completely in serializationBuffer;
		integerToByteBuffer(serializationBuffer.getLength(), this.lengthBuf);
	}

	public void clear() {
		this.bytesReadFromBuffer = 0;
		this.lengthBuf.clear();
		this.serializationBuffer.reset();
	}
}
