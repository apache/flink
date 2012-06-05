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
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.FileBufferManager;

public final class CheckpointDeserializer extends AbstractDeserializer {

	private final AbstractID ownerID;

	private final FileBufferManager fileBufferManager;

	private final boolean distributed;

	private boolean bufferDataSerializationStarted = false;

	public CheckpointDeserializer(final AbstractID ownerID, final boolean distributed) {
		this.ownerID = ownerID;
		this.fileBufferManager = FileBufferManager.getInstance();
		this.distributed = distributed;
	}

	@Override
	protected boolean readBufferData(final ReadableByteChannel readableByteChannel) throws IOException {

		final ByteBuffer tempBuffer = getTempBuffer();

		if (!this.bufferDataSerializationStarted) {
			tempBuffer.clear();
			this.bufferDataSerializationStarted = true;
		}

		readableByteChannel.read(tempBuffer);
		if (tempBuffer.hasRemaining()) {
			return true;
		}

		final long offset = byteBufferToLong(tempBuffer);

		final Buffer fileBuffer = BufferFactory.createFromCheckpoint(getSizeOfBuffer(), offset, this.ownerID,
			this.fileBufferManager, this.distributed);

		setBuffer(fileBuffer);

		this.bufferDataSerializationStarted = false;
		return false;
	}

	private long byteBufferToLong(final ByteBuffer byteBuffer) throws IOException {

		long l = 0;

		if (Long.SIZE > byteBuffer.limit()) {
			throw new IOException("Cannot convert byte buffer to long, not enough data in byte buffer ("
				+ byteBuffer.limit() + ")");
		}

		for (int i = 0; i < Long.SIZE; ++i) {
			l |= (byteBuffer.get((Long.SIZE - 1) - i) & 0xffL) << (i << 3);
		}

		return l;
	}
}
