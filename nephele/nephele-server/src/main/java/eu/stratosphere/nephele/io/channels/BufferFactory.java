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

import java.nio.ByteBuffer;
import java.util.Queue;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.InternalBuffer;

public abstract class BufferFactory {

	public static Buffer createFromFile(final int bufferSize, final AbstractID ownerID,
			final FileBufferManager fileBufferManager) {

		final InternalBuffer internalBuffer = new FileBuffer(bufferSize, ownerID, fileBufferManager);
		return new Buffer(internalBuffer);
	}

	public static Buffer createFromCheckpoint(final int bufferSize, final FileID fileID, final long offset,
			final AbstractID ownerID, final FileBufferManager fileBufferManager) {

		final InternalBuffer internalBuffer = new FileBuffer(bufferSize, fileID, offset, ownerID, fileBufferManager);
		
		return new Buffer(internalBuffer);
	}

	public static Buffer createFromMemory(final int bufferSize, final ByteBuffer byteBuffer,
			final Queue<ByteBuffer> queueForRecycledBuffers) {

		final InternalBuffer internalBuffer = new MemoryBuffer(bufferSize, byteBuffer, queueForRecycledBuffers);
		return new Buffer(internalBuffer);
	}
}
