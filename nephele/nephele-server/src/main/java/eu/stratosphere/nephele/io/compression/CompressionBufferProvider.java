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

package eu.stratosphere.nephele.io.compression;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.MemoryBuffer;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.util.StringUtils;

public final class CompressionBufferProvider {

	private final int maximumBufferSize;

	private MemoryBuffer compressionBuffer;

	private MemoryBuffer temporaryBuffer;

	private boolean compressionBufferLocked = false;

	private boolean temporaryBufferLocked = false;

	private int referenceCounter = 1;

	public CompressionBufferProvider(final BufferProvider bufferProvider, boolean allocateTempBuffer) {

		this.maximumBufferSize = bufferProvider.getMaximumBufferSize();

		try {

			final Buffer buf = bufferProvider.requestEmptyBuffer(this.maximumBufferSize);
			if (buf == null) {
				throw new IllegalStateException("Cannot retrieve compression buffer");
			}

			if (!buf.isBackedByMemory()) {
				throw new IllegalStateException("Compression buffer is not backed by memory");
			}

			this.compressionBuffer = (MemoryBuffer) buf;
		} catch (IOException ioe) {
			throw new RuntimeException(StringUtils.stringifyException(ioe));
		}

		if (allocateTempBuffer) {
			try {
				final Buffer buf = bufferProvider.requestEmptyBuffer(this.maximumBufferSize);
				if (buf == null) {
					throw new IllegalStateException("Cannot retrieve temporary buffer");
				}

				if (!buf.isBackedByMemory()) {
					throw new IllegalStateException("Temporary buffer is not backed by memory");
				}

				this.temporaryBuffer = (MemoryBuffer) buf;
			} catch (IOException ioe) {
				throw new RuntimeException(StringUtils.stringifyException(ioe));
			}
		} else {
			this.temporaryBuffer = null;
		}
	}

	public void increaseReferenceCounter() {

		++this.referenceCounter;
	}

	public int getMaximumBufferSize() {

		return this.maximumBufferSize;
	}

	public MemoryBuffer lockCompressionBuffer() {

		if (this.compressionBufferLocked) {
			throw new IllegalStateException("Compression buffer is already locked");
		}

		this.compressionBufferLocked = true;

		return this.compressionBuffer;
	}

	public MemoryBuffer lockTemporaryBuffer() {

		if (this.temporaryBuffer == null) {
			throw new IllegalStateException("No temporary buffer allocated, so it cannot be locked");
		}

		if (this.temporaryBufferLocked) {
			throw new IllegalStateException("Temporary buffer is already locked");
		}

		this.temporaryBufferLocked = true;

		return this.temporaryBuffer;
	}

	public void releaseCompressionBuffer(final MemoryBuffer compressionBuffer) {

		if (!this.compressionBufferLocked) {
			throw new IllegalStateException("Compression buffer is not locked");
		}

		this.compressionBuffer = compressionBuffer;
		this.compressionBufferLocked = false;
	}

	public void releaseTemporaryBuffer(final MemoryBuffer temporaryBuffer) {

		if (!this.temporaryBufferLocked) {
			throw new IllegalStateException("Temporary buffer is not locked");
		}

		this.temporaryBuffer = temporaryBuffer;
		this.temporaryBufferLocked = false;
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

		if (this.temporaryBufferLocked) {
			throw new IllegalStateException("Shutdown requested but temporary buffer is still locked " +
				this.temporaryBuffer);
		}

		if (this.temporaryBuffer != null) {
			this.temporaryBuffer.recycleBuffer();
			this.temporaryBuffer = null;
		}
	}
}
