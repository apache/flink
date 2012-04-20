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

package eu.stratosphere.nephele.io.channels.bytebuffered;

public final class BufferPairRequest {

	private final int compressedDataBufferSize;

	private final int uncompressedDataBufferSize;

	private final boolean requireMemoryBuffers;

	public BufferPairRequest(int compressedDataBufferSize, int uncompressedDataBufferSize, boolean requireMemoryBuffers) {

		this.compressedDataBufferSize = compressedDataBufferSize;
		this.uncompressedDataBufferSize = uncompressedDataBufferSize;
		this.requireMemoryBuffers = requireMemoryBuffers;
	}

	public int getCompressedDataBufferSize() {
		return this.compressedDataBufferSize;
	}

	public int getUncompressedDataBufferSize() {
		return this.uncompressedDataBufferSize;
	}

	public int getNumberOfRequestedByteBuffers() {

		if (this.compressedDataBufferSize > 0 && this.uncompressedDataBufferSize > 0) {
			return 2;
		}

		if (this.compressedDataBufferSize > 0 || this.uncompressedDataBufferSize > 0) {
			return 1;
		}

		return 0;
	}

	public boolean requireMemoryBuffers() {
		return this.requireMemoryBuffers;
	}
}
