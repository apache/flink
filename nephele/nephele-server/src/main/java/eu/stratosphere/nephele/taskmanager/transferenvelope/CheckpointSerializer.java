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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.FileBuffer;

/**
 * A checkpoint serializer is a special implementation of a transfer envelope serializer. Unlike the
 * {@link DefaultSerializer}, this implementation does not copy the actual buffer data into the byte stream. Instead,
 * since the buffer's data is expected to reside on disk anyway, it just inserts a reference to this data into the byte
 * stream.
 * 
 * @author warneke
 */
public class CheckpointSerializer extends AbstractSerializer {

	private static final int SIZEOFLONG = 8;

	private boolean bufferDataSerializationStarted = false;

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean writeBufferData(final WritableByteChannel writableByteChannel, final Buffer buffer) throws IOException {

		final ByteBuffer tempBuffer = getTempBuffer();

		if (!this.bufferDataSerializationStarted) {

			if (buffer == null) {
				throw new IllegalArgumentException("Argument buffer must not be null");
			}

			if (buffer.isInWriteMode()) {
				throw new IllegalStateException("Buffer to be serialized is still in write mode");
			}

			if (!(buffer instanceof FileBuffer)) {
				throw new IllegalArgumentException("Provided buffer is not a file buffer");
			}

			final FileBuffer fileBuffer = (FileBuffer) buffer;

			tempBuffer.clear();
			longToByteBuffer(fileBuffer.getOffset(), tempBuffer);

			this.bufferDataSerializationStarted = true;
		}

		if (tempBuffer.hasRemaining()) {
			writableByteChannel.write(tempBuffer);
		} else {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() {

		super.reset();

		this.bufferDataSerializationStarted = false;
	}

	private void longToByteBuffer(long longToSerialize, ByteBuffer byteBuffer) throws IOException {

		if (SIZEOFLONG > byteBuffer.capacity()) {
			throw new IOException("Cannot convert long to byte buffer, buffer is too small (" + byteBuffer.limit()
				+ ", required " + SIZEOFLONG + ")");
		}

		byteBuffer.limit(SIZEOFLONG);

		for (int i = 0; i < SIZEOFLONG; ++i) {
			final int shift = i << 3; // i * 8
			byteBuffer.put((SIZEOFLONG - 1) - i, (byte) ((longToSerialize & (0xffL << shift)) >>> shift));
		}
	}
}
