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

import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.compression.CompressionLibrary;
import eu.stratosphere.nephele.io.compression.CompressionLoader;
import eu.stratosphere.nephele.io.compression.Compressor;
import eu.stratosphere.nephele.io.compression.Decompressor;

public class DynamicLibrary implements CompressionLibrary {

	private static final int NUMBER_OF_COMPRESSION_LEVELS = 4;

	private final CompressionLibrary[] libraries;

	private int lastCompressedBufferSize = -1;

	private int lastUncompressedBufferSize = 0;

	public DynamicLibrary(String nativeLibraryDir)
													throws CompressionException {

		libraries = new CompressionLibrary[NUMBER_OF_COMPRESSION_LEVELS];
		libraries[0] = new StubCompressionLibrary();
		libraries[1] = CompressionLoader.getCompressionLibraryByCompressionLevel(CompressionLevel.LIGHT_COMPRESSION);
		libraries[2] = CompressionLoader.getCompressionLibraryByCompressionLevel(CompressionLevel.MEDIUM_COMPRESSION);
		libraries[3] = CompressionLoader.getCompressionLibraryByCompressionLevel(CompressionLevel.HEAVY_COMPRESSION);
	}

	@Override
	public Compressor getCompressor() throws CompressionException {

		return new DynamicCompressor(this.libraries);
	}

	@Override
	public Decompressor getDecompressor() throws CompressionException {

		return new DynamicDecompressor(this.libraries);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getUncompressedBufferSize(int compressedBufferSize) {

		// Do some caching to avoid unnecessary calculations
		if (compressedBufferSize == lastCompressedBufferSize) {
			return lastUncompressedBufferSize;
		}

		/**
		 * Since we do not know which internal compression level the
		 * compression will pick, we are pessimistic and take the smallest
		 * calculated buffer.
		 */
		int retVal = Integer.MAX_VALUE;
		for (int i = 0; i < NUMBER_OF_COMPRESSION_LEVELS; i++) {
			retVal = Math.min(retVal, this.libraries[i].getUncompressedBufferSize(compressedBufferSize));
		}

		this.lastCompressedBufferSize = compressedBufferSize;
		this.lastUncompressedBufferSize = retVal;

		return retVal;
	}

	@Override
	public String getLibraryName() {
		return "DYNAMIC";
	}
}
