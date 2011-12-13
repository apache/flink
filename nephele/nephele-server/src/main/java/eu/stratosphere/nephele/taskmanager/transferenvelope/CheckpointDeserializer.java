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
import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.DeserializationBuffer;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.io.channels.FileID;

public final class CheckpointDeserializer extends AbstractDeserializer {

	private static final int SIZEOFLONG = 8;

	private final AbstractID ownerID;

	private final FileBufferManager fileBufferManager;

	private FileID deserializedFileID = null;

	private DeserializationBuffer<FileID> fileIDDeserializationBuffer = new DeserializationBuffer<FileID>(
			new DefaultRecordDeserializer<FileID>(FileID.class), true);

	public CheckpointDeserializer(final AbstractID ownerID) {
		this.ownerID = ownerID;
		this.fileBufferManager = FileBufferManager.getInstance();
	}

	@Override
	protected boolean readBufferData(final ReadableByteChannel readableByteChannel) throws IOException {

		final ByteBuffer tempBuffer = getTempBuffer();

		if (this.deserializedFileID == null) {
			this.deserializedFileID = this.fileIDDeserializationBuffer.readData(null, readableByteChannel);
			if (this.deserializedFileID != null) {
				tempBuffer.position(0);
				tempBuffer.limit(8);
			} else {
				return true;
			}
		}

		readableByteChannel.read(tempBuffer);
		if (tempBuffer.hasRemaining()) {
			return true;
		}

		final long offset = byteBufferToLong(tempBuffer);

		final Buffer fileBuffer = BufferFactory.createFromCheckpoint(getSizeOfBuffer(), this.deserializedFileID,
			offset, this.ownerID, this.fileBufferManager);

		setBuffer(fileBuffer);

		this.fileBufferManager.registerExternalReadableSpillingFile(this.ownerID, this.deserializedFileID);

		this.deserializedFileID = null;

		return false;
	}

	private long byteBufferToLong(final ByteBuffer byteBuffer) throws IOException {

		long l = 0;

		if (SIZEOFLONG > byteBuffer.limit()) {
			throw new IOException("Cannot convert byte buffer to long, not enough data in byte buffer ("
				+ byteBuffer.limit() + ")");
		}

		for (int i = 0; i < SIZEOFLONG; ++i) {
			l |= (byteBuffer.get((SIZEOFLONG - 1) - i) & 0xffL) << (i << 3);
		}

		return l;
	}
}
