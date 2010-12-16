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

import eu.stratosphere.nephele.io.DataInputBuffer;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.RecordDeserializer;

/**
 * A class for deserializing a portion of binary data into records of type <code>T</code>. The internal
 * buffer grows dynamically to the size that is required for deserialization.
 * 
 * @author warneke
 * @param <T>
 *        the type of the record this deserialization buffer can be used for
 */
public class DeserializationBuffer<T extends IOReadableWritable> {

	/**
	 * The size of an integer in byte.
	 */
	private static final int SIZEOFINT = 4;

	/**
	 * The data input buffer used for deserialization.
	 */
	private DataInputBuffer deserializationBuffer = new DataInputBuffer();

	/**
	 * The class of the type to be deserialized.
	 */
	private RecordDeserializer<T> deserializer = null;

	/**
	 * Number of bytes already read from the length field.
	 */
	private int bytesReadFromLength = 0;

	/**
	 * Number of bytes already read from the buffer.
	 */
	private int bytesReadFromBuffer = 0;

	/**
	 * Buffer to reconstruct the length field.
	 */
	byte[] lengthBuf = new byte[SIZEOFINT];

	/**
	 * Size of the record to be deserialized in bytes.
	 */
	private int recordLength = 0;

	/**
	 * Temporary buffer.
	 */
	private byte[] tempBuffer = null;

	/**
	 * Constructs a new deserialization buffer with the specified type.
	 * 
	 * @param type
	 *        the type of the record the deserialization buffer can be used for
	 */
	public DeserializationBuffer(RecordDeserializer<T> deserializer) {
		this.deserializer = deserializer;
	}

	/**
	 * Reads the data from the provided {@link ByteBuffer} and attempts to deserialize the record from the data.
	 * If the {@link ByteBuffer} does not contain all the data that is required for deserializing the data is stored
	 * internally and deserialization is continued on the next call of this method.
	 * 
	 * @param buffer
	 *        the byte buffer to read the data from
	 * @return the deserialized record of the byte buffer contained all the data required for deserialization,
	 *         <code>null</code> otherwise
	 * @throws IOException
	 *         thrown if an error occurs while deserializing the record
	 */
	public T readData(ByteBuffer buffer) throws IOException {

		if (bytesReadFromLength < SIZEOFINT) {
			final int length = Math.min(SIZEOFINT - bytesReadFromLength, buffer.remaining());
			buffer.get(lengthBuf, bytesReadFromLength, length);
			bytesReadFromLength += length;

			if (bytesReadFromLength < SIZEOFINT) {
				return null;
			}

			recordLength = byteArrayToInt(lengthBuf);

			if (tempBuffer == null) {
				tempBuffer = new byte[recordLength];
			}

			if (tempBuffer.length < recordLength) {
				// for(int i = buffer.position(); i < buffer.position() + 20; i++) {
				// System.out.print((char)buffer.get(i));
				// }
				// System.out.println("");
				// System.out.println("Increasing temp buffer to " + recordLength);
				tempBuffer = new byte[recordLength];
			}

			bytesReadFromBuffer = 0;
		}

		if (buffer.remaining() == 0)
			return null;

		{
			final int length = Math.min(recordLength - bytesReadFromBuffer, buffer.remaining());
			buffer.get(tempBuffer, bytesReadFromBuffer, length);
			bytesReadFromBuffer += length;

			if (bytesReadFromBuffer < recordLength) {
				return null;
			}

			deserializationBuffer.reset(tempBuffer, recordLength);

			// TODO erik (mod)
			/*
			 * T record = null;
			 * try {
			 * record = type.newInstance();
			 * } catch(Exception e) {
			 * throw new IOException(e);
			 * }
			 * record.read(deserializationBuffer);
			 */

			T record = deserializer.deserialize(deserializationBuffer);

			// If this has really worked out, prepare for the next item
			bytesReadFromLength = 0;

			return record;
		}
	}

	/**
	 * Translates an array of bytes into an integer.
	 * 
	 * @param arr
	 *        the array of bytes used as input.
	 * @return the resulting integer
	 */
	private int byteArrayToInt(byte[] arr) {

		int number = 0;
		for (int i = 0; i < SIZEOFINT; ++i) {
			number |= (arr[SIZEOFINT - 1 - i] & 0xff) << (i << (SIZEOFINT - 1));
		}

		return number;
	}

	// TODO: Does this have to be public?
	public int getLengthOfNextRecord() {

		if (bytesReadFromLength < SIZEOFINT) {
			return -1;
		}

		return this.recordLength;
	}

	// TODO: Does this have to be public?
	public int getBytesFilledInBuffer() {

		return this.bytesReadFromBuffer;
	}

	public void clear() {

		this.bytesReadFromBuffer = 0;
		this.bytesReadFromLength = 0;
	}
}
