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

import eu.stratosphere.nephele.io.AbstractID;

public final class BufferFactory {

	public static FileBuffer createFromFile(final int bufferSize, final AbstractID ownerID,
			final FileBufferManager fileBufferManager, final boolean distributed) throws IOException {

		return new FileBuffer(bufferSize, ownerID, fileBufferManager, distributed);
	}

	public static FileBuffer createFromCheckpoint(final int bufferSize, final long offset,
			final AbstractID ownerID, final FileBufferManager fileBufferManager, final boolean distributed)
			throws IOException {

		return new FileBuffer(bufferSize, offset, ownerID, fileBufferManager, distributed);
	}

	public static Buffer createFromMemory(final int bufferSize, final ByteBuffer byteBuffer,
			final MemoryBufferPoolConnector bufferPoolConnector) {

		return new MemoryBuffer(bufferSize, byteBuffer, bufferPoolConnector);
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private BufferFactory() {
	}
}
