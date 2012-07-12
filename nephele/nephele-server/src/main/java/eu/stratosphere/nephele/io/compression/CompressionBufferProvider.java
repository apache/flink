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

package eu.stratosphere.nephele.io.compression;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.util.StringUtils;

public final class CompressionBufferProvider {

	private Buffer compressionBuffer;

	private boolean compressionBufferLocked = false;

	private int referenceCounter = 1;

	public CompressionBufferProvider(final BufferProvider bufferProvider) {

		try {
			this.compressionBuffer = bufferProvider.requestEmptyBuffer(bufferProvider.getMaximumBufferSize());
		} catch (IOException ioe) {
			throw new RuntimeException(StringUtils.stringifyException(ioe));
		}

		if (this.compressionBuffer == null) {
			throw new IllegalStateException("Cannot retrieve compression buffer");
		}
	}

	public void increaseReferenceCounter() {

		++this.referenceCounter;
	}

	public Buffer lockCompressionBuffer() throws IOException {

		if (this.compressionBufferLocked) {
			throw new IllegalStateException("Compression buffer is already locked");
		}

		this.compressionBufferLocked = true;

		return this.compressionBuffer;
	}

	public void releaseCompressionBuffer(final Buffer compressionBuffer) {

		if (!this.compressionBufferLocked) {
			throw new IllegalStateException("Compression buffer is not locked");
		}

		this.compressionBuffer = compressionBuffer;
		this.compressionBufferLocked = false;
	}

	public void shutdown() {

		--this.referenceCounter;

		if (this.referenceCounter > 0) {
			return;
		}

		if (this.compressionBufferLocked) {
			throw new IllegalStateException("Shutdown requested but compression buffer is still locked");
		}

		if (this.compressionBuffer != null) {
			this.compressionBuffer.recycleBuffer();
			this.compressionBuffer = null;
		}
	}
}
