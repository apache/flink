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

package eu.stratosphere.nephele.io.compression.library.lzma;

import eu.stratosphere.nephele.io.compression.CompressionBufferProvider;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLibrary;
import eu.stratosphere.nephele.io.compression.Compressor;
import eu.stratosphere.nephele.io.compression.Decompressor;
import eu.stratosphere.nephele.util.NativeCodeLoader;
import eu.stratosphere.nephele.util.StringUtils;

public class LzmaLibrary implements CompressionLibrary {

	/**
	 * The file name of the native lzma library.
	 */
	private static final String NATIVELIBRARYFILENAME = "liblzmacompression.so.1.0";

	public LzmaLibrary(final String nativeLibraryDir) throws CompressionException {

		if (!NativeCodeLoader.isLibraryLoaded(NATIVELIBRARYFILENAME)) {
			try {
				NativeCodeLoader.loadLibraryFromFile(nativeLibraryDir, NATIVELIBRARYFILENAME);

				LzmaCompressor.initIDs();

				LzmaDecompressor.initIDs();
			} catch (Exception e) {
				throw new CompressionException(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getUncompressedBufferSize(final int compressedBufferSize) {

		/*
		 * Calculate size of compressed data buffer according to
		 * http://gpwiki.org/index.php/LZO
		 */
		// TODO check that for LZMA
		return (compressedBufferSize / 65) * 64;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getLibraryName() {
		return "LZMA";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Compressor createNewCompressor(final CompressionBufferProvider bufferProvider) throws CompressionException {

		return new LzmaCompressor(bufferProvider);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Decompressor createNewDecompressor(final CompressionBufferProvider bufferProvider)
			throws CompressionException {

		return new LzmaDecompressor(bufferProvider);
	}
}
