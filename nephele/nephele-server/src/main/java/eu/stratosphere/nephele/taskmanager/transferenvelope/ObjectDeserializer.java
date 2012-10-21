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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

class ObjectDeserializer {

	/**
	 * The initial size of the deserialization buffer in bytes.
	 */
	private static final int INITIAL_BUFFER_SIZE = 8192;

	/**
	 * The size of an integer in byte.
	 */
	private static final int SIZE_OF_INT = 4;

	/**
	 * Buffer to reconstruct the length field.
	 */
	private final ByteBuffer lengthBuf;

	/**
	 * The deserialization buffer.
	 */
	private ByteBuffer tempBuffer;

	/**
	 * Size of the record to be deserialized in bytes.
	 */
	private int recordLength = -1;

	private final Kryo kryo = new Kryo();

	private final Input input;

	ObjectDeserializer() {

		this.lengthBuf = ByteBuffer.allocate(SIZE_OF_INT);
		this.lengthBuf.order(ByteOrder.BIG_ENDIAN);
		this.tempBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
		this.input = new Input(this.tempBuffer.array());
	}

	<T> T deserialize(final ReadableByteChannel readableByteChannel, final Class<T> type) throws IOException {

		// check whether the length has already been de-serialized
		final int len;
		if (this.recordLength < 0) {
			if (readableByteChannel.read(this.lengthBuf) == -1) {
				if (this.lengthBuf.position() == 0) {
					throw new EOFException();
				} else {
					throw new IOException("Deserialization error: Expected to read " + this.lengthBuf.remaining()
						+ " more bytes of length information from the stream!");
				}
			}

			if (this.lengthBuf.hasRemaining()) {
				return null;
			}

			len = this.lengthBuf.getInt(0);
			this.lengthBuf.clear();

			if (this.tempBuffer.capacity() < len) {
				this.tempBuffer = ByteBuffer.allocate(len);
				this.input.setBuffer(this.tempBuffer.array());
			}

			// Important: limit the number of bytes that can be read into the buffer
			this.tempBuffer.position(0);
			this.tempBuffer.limit(len);
			this.input.setPosition(0);
			this.input.setLimit(len);
		} else {
			len = this.recordLength;
		}

		if (readableByteChannel.read(this.tempBuffer) == -1) {
			throw new IOException("Deserilization error: Expected to read " + this.tempBuffer.remaining()
				+ " more bytes from stream!");
		}

		if (this.tempBuffer.hasRemaining()) {
			this.recordLength = len;
			return null;
		} else {
			this.recordLength = -1;
		}

		return this.kryo.readObject(this.input, type);
	}

	void clear() {
		this.kryo.reset();
		this.recordLength = -1;
		this.tempBuffer.clear();
		this.lengthBuf.clear();
	}

	boolean hasUnfinishedData() {

		if (this.recordLength != -1) {
			return true;
		}

		if (this.lengthBuf.position() > 0) {
			return true;
		}

		return false;
	}
}
