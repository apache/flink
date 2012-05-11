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

package eu.stratosphere.nephele.io.compression.library.lzma;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.compression.AbstractCompressionLibrary;
import eu.stratosphere.nephele.io.compression.AbstractDecompressor;

/**
 * This class provides an interface for decompressing byte-buffers with the native lzma library
 * http://www.7-zip.org/sdk.html
 * 
 * @author akli
 */
public class LzmaDecompressor extends AbstractDecompressor {

	/*
	 * size of compressed and uncompressed data
	 * native library writes this at start of each block
	 * private static final int SIZE_LENGTH = 13;
	 * private static final int UNCOMPRESSED_BLOCKSIZE_LENGTH = 4;
	 * private static final int LZMA_PROPS_SIZE = 5;
	 */

	public LzmaDecompressor(AbstractCompressionLibrary compressionLibrary) {
		super(compressionLibrary);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setCompressedDataBuffer(Buffer buffer) {

		if (buffer == null) {
			this.compressedBuffer = null;
			this.compressedDataBuffer = null;
			this.compressedDataBufferLength = 0;
		} else {
			this.compressedDataBuffer = getInternalByteBuffer(buffer);
			// this.compressedDataBufferLength = this.compressedDataBuffer.limit();
			this.compressedBuffer = buffer;

			// Extract length of uncompressed buffer from the compressed buffer
			this.compressedDataBufferLength = bufferToInt(this.compressedDataBuffer, 0);
			this.uncompressedDataBufferLength = bufferToInt(this.compressedDataBuffer, 4);
		}
	}

	native static void initIDs();

	protected native int decompressBytesDirect(int offset);

	@Override
	protected void freeInternalResources() {
		// TODO Auto-generated method stub

	}
}
