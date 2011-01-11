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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

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
	 * Buffer to reconstruct the length field.
	 */
	ByteBuffer lengthBuf = ByteBuffer.allocate(SIZEOFINT);

	/**
	 * Size of the record to be deserialized in bytes.
	 */
	private int recordLength = -1;

	private final boolean propagateEndOfStream;

	/**
	 * Temporary buffer.
	 */
	ByteBuffer tempBuffer = null;

	/**
	 * Constructs a new deserialization buffer with the specified type.
	 * 
	 * @param type
	 *        the type of the record the deserialization buffer can be used for
	 * @param propagateEndOfStream
	 *        <code>true> if end of stream notifications during the
	 * deserialization process shall be propagated to the caller, <code>false</code> otherwise
	 */
	public DeserializationBuffer(RecordDeserializer<T> deserializer, boolean propagateEndOfStream) {
		this.deserializer = deserializer;
		this.propagateEndOfStream = propagateEndOfStream;
	}

	/**
	 * Reads data from the given byte channel and deserializes an object of type <code>T</code> from it.
	 * 
	 * @param readableByteChannel
	 *        the byte channel to read data from
	 * @return an object of type <code>T</code>
	 * @throws IOException
	 *         thrown if an error occurs while reading the data or deserializing the object
	 */
	public T readData(ReadableByteChannel readableByteChannel) throws IOException {

		if (this.recordLength < 0) {
			if (readableByteChannel.read(this.lengthBuf) == -1 && this.propagateEndOfStream) {
				if (this.lengthBuf.position() == 0) {
					throw new EOFException();
				} else {
					throw new IOException("Deserilization error: Expected to read " + this.lengthBuf.remaining()
						+ " more bytes of length information from the stream!");
				}
			}

			if (this.lengthBuf.hasRemaining()) {
				return null;
			}

			this.recordLength = byteArrayToInt(lengthBuf.array());

			if (this.tempBuffer == null) {
				tempBuffer = ByteBuffer.allocate(recordLength);
			}

			if (this.tempBuffer.capacity() < recordLength) {
				tempBuffer = ByteBuffer.allocate(recordLength);
			}

			// Important: limit the number of bytes that can be read into the buffer
			this.tempBuffer.position(0);
			this.tempBuffer.limit(this.recordLength);
		}

		if (readableByteChannel.read(tempBuffer) == -1 && this.propagateEndOfStream) {
			throw new IOException("Deserilization error: Expected to read " + this.tempBuffer.remaining()
				+ " more bytes from stream!");
		}

		if (this.tempBuffer.hasRemaining()) {
			return null;
		}

		deserializationBuffer.reset(tempBuffer.array(), this.recordLength);
		final T record = deserializer.deserialize(deserializationBuffer);

		this.recordLength = -1;
		this.lengthBuf.clear();

		return record;
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

	/**
	 * Clears the internal buffers of the deserializer and resets its state.
	 */
	public void clear() {

		this.recordLength = -1;
		if (this.tempBuffer != null) {
			this.tempBuffer.clear();
		}
		if (this.lengthBuf != null) {
			this.lengthBuf.clear();
		}
	}

	/**
	 * Checks if the deserializer has data from a previous deserialization attempt stored in its internal buffers which
	 * is not yet finished.
	 * 
	 * @return <code>true</code> if the deserializer's internal buffers contain data from a previous deserialization
	 *         attempt, <code>false</code> otherwise
	 */
	public boolean hasUnfinishedData() {

		if (this.recordLength != -1) {
			return true;
		}

		if (this.lengthBuf.position() > 0) {
			return true;
		}

		return false;
	}
}
