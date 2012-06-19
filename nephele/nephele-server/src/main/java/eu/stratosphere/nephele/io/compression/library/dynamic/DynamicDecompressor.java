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

package eu.stratosphere.nephele.io.compression.library.dynamic;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.compression.CompressionBufferProvider;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLibrary;
import eu.stratosphere.nephele.io.compression.Decompressor;

public class DynamicDecompressor implements Decompressor {

	private final Decompressor[] decompressors;

	private int selectedDecompressor = 0;

	public DynamicDecompressor(final CompressionLibrary[] compressionLibraries,
			final CompressionBufferProvider bufferProvider) throws CompressionException {

		// Initialize the different compressors
		this.decompressors = new Decompressor[compressionLibraries.length];
		for (int i = 0; i < this.decompressors.length; i++) {
			this.decompressors[i] = compressionLibraries[i].createNewDecompressor(bufferProvider);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer decompress(final Buffer compressedData) throws IOException {

		return this.decompressors[this.selectedDecompressor].decompress(compressedData);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setCurrentInternalDecompressionLibraryIndex(int index) {

		this.selectedDecompressor = index;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {

		for (int i = 0; i < this.decompressors.length; i++) {
			this.decompressors[i].shutdown();
		}
	}
}
