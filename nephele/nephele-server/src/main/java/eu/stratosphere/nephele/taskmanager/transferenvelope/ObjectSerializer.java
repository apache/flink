/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

final class ObjectSerializer {

	/**
	 * The initial size of the serialization buffer in bytes.
	 */
	private static final int INITIAL_BUFFER_SIZE = 8192;

	private static final int SIZE_OF_INT = 4;

	private final Kryo kryo = new Kryo();

	private final Output output = new Output(INITIAL_BUFFER_SIZE);

	private final ByteBuffer lengthBuf;

	private int bytesReadFromBuffer = 0;

	ObjectSerializer() {
		this.lengthBuf = ByteBuffer.allocate(SIZE_OF_INT);
		this.lengthBuf.order(ByteOrder.BIG_ENDIAN);
	}

	/**
	 * Return <code>true</code> if the internal serialization buffer still contains data.
	 * In this case the method serialize must not be called. If the internal buffer is empty
	 * the method return <code>false</code>
	 * 
	 * @return <code>true</code> if the internal serialization buffer still contains data, <code>false</code> it it is
	 *         empty
	 */
	boolean dataLeftFromPreviousSerialization() {
		return leftInSerializationBuffer() > 0;
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
	int read(final WritableByteChannel writableByteChannel) throws IOException {

		int bytesReadFromLengthBuf = 0;

		// Deal with length buffer first
		if (this.lengthBuf.hasRemaining()) { // There is data from the length buffer to be written
			bytesReadFromLengthBuf = writableByteChannel.write(this.lengthBuf);
		}

		final ByteBuffer bb = ByteBuffer.wrap(this.output.getBuffer(), this.bytesReadFromBuffer,
			leftInSerializationBuffer());

		final int bytesReadFromSerializationBuf = writableByteChannel.write(bb);
		this.bytesReadFromBuffer += bytesReadFromSerializationBuf;

		if (leftInSerializationBuffer() == 0) { // Record is entirely written to byteBuffer
			this.output.clear();
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

		return (this.output.total() - this.bytesReadFromBuffer);
	}

	/**
	 * Serializes the record and writes it to an internal buffer. The buffer grows dynamically
	 * in case more memory is required to serialization.
	 * 
	 * @param object
	 *        the object to be serialized
	 * @throws IOException
	 *         Thrown if data from a previous serialization process is still in the internal buffer and has not yet been
	 *         transfered to a byte buffer
	 */
	void serialize(final Object object) throws IOException {

		// Check if there is data left in the buffer
		if (dataLeftFromPreviousSerialization()) {
			throw new IOException("Cannot write new data, " + leftInSerializationBuffer()
				+ " bytes still left from previous call");
		}

		this.kryo.writeObject(this.output, object);

		this.lengthBuf.putInt(this.output.total());
		this.lengthBuf.flip();
	}

	void clear() {
		this.kryo.reset();
		this.bytesReadFromBuffer = 0;
		this.lengthBuf.clear();
		this.output.clear();
	}
}
