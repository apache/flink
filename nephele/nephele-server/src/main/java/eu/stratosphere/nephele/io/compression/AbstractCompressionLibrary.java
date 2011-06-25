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

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;

public abstract class AbstractCompressionLibrary implements CompressionLibrary {

	private final Map<OutputGate<?>, CompressorCacheEntry> compressorCache = new HashMap<OutputGate<?>, CompressorCacheEntry>();

	private final Map<InputGate<?>, DecompressorCacheEntry> decompressorCache = new HashMap<InputGate<?>, DecompressorCacheEntry>();

	@Override
	public final synchronized Compressor getCompressor(final AbstractByteBufferedOutputChannel<?> outputChannel)
			throws CompressionException {

		final OutputGate<?> outputGate = outputChannel.getOutputGate();

		CompressorCacheEntry cacheEntry = this.compressorCache.get(outputGate);
		if (cacheEntry == null) {
			Compressor compressor = initNewCompressor(outputChannel);
			cacheEntry = new CompressorCacheEntry(compressor, outputGate);
			this.compressorCache.put(outputGate, cacheEntry);
		}

		return cacheEntry.getCompressor();
	}

	@Override
	public final synchronized Decompressor getDecompressor(
			final AbstractByteBufferedInputChannel<?> inputChannel) throws CompressionException {

		final InputGate<?> inputGate = inputChannel.getInputGate();

		DecompressorCacheEntry cacheEntry = this.decompressorCache.get(inputGate);
		if (cacheEntry == null) {
			Decompressor decompressor = initNewDecompressor(inputChannel);
			cacheEntry = new DecompressorCacheEntry(decompressor, inputGate);
			this.decompressorCache.put(inputGate, cacheEntry);
		}

		return cacheEntry.getDecompressor();
	}

	protected abstract Compressor initNewCompressor(AbstractByteBufferedOutputChannel<?> outputChannel)
			throws CompressionException;

	protected abstract Decompressor initNewDecompressor(AbstractByteBufferedInputChannel<?> inputChannel)
			throws CompressionException;

	synchronized boolean canBeShutDown(final Compressor compressor) {

		System.out.println("can be shut down called (compressor)");
		
		return false;
	}

	synchronized boolean canBeShutDown(final Decompressor decompressor) {

		System.out.println("can be shut down called (compressor)");
		
		return false;
	}
}
