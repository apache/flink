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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;

public abstract class AbstractCompressionLibrary implements CompressionLibrary {

	private static final Log LOG = LogFactory.getLog(AbstractCompressionLibrary.class);

	private final Map<OutputGate<?>, CompressorCacheEntry> compressorCache = new HashMap<OutputGate<?>, CompressorCacheEntry>();

	private final Map<Compressor, OutputGate<?>> compressorMap = new HashMap<Compressor, OutputGate<?>>();

	private final Map<InputGate<?>, DecompressorCacheEntry> decompressorCache = new HashMap<InputGate<?>, DecompressorCacheEntry>();

	private final Map<Decompressor, InputGate<?>> decompressorMap = new HashMap<Decompressor, InputGate<?>>();

	@Override
	public final synchronized Compressor getCompressor(final AbstractByteBufferedOutputChannel<?> outputChannel)
			throws CompressionException {

		final OutputGate<?> outputGate = outputChannel.getOutputGate();

		CompressorCacheEntry cacheEntry = this.compressorCache.get(outputGate);
		if (cacheEntry == null) {
			Compressor compressor = initNewCompressor(outputChannel);
			cacheEntry = new CompressorCacheEntry(compressor, outputGate);
			this.compressorCache.put(outputGate, cacheEntry);
			this.compressorMap.put(compressor, outputGate);
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
			this.decompressorMap.put(decompressor, inputGate);
		}

		return cacheEntry.getDecompressor();
	}

	protected abstract Compressor initNewCompressor(AbstractByteBufferedOutputChannel<?> outputChannel)
			throws CompressionException;

	protected abstract Decompressor initNewDecompressor(AbstractByteBufferedInputChannel<?> inputChannel)
			throws CompressionException;

	synchronized boolean canBeShutDown(final Compressor compressor, final ChannelID channelID) {

		final OutputGate<?> outputGate = this.compressorMap.get(compressor);
		if (outputGate == null) {
			LOG.error("Cannot find output gate to compressor " + compressor);
			return false;
		}

		final CompressorCacheEntry cacheEntry = this.compressorCache.get(outputGate);
		if (cacheEntry == null) {
			LOG.error("Cannot find compressor cache entry to output gate " + outputGate);
		}

		cacheEntry.removeAssignedChannel(channelID);

		if (!cacheEntry.hasAssignedChannels()) {
			// Clean up
			this.compressorCache.remove(outputGate);
			this.compressorMap.remove(compressor);
			return true;
		}

		return false;
	}

	synchronized boolean canBeShutDown(final Decompressor decompressor, final ChannelID channelID) {

		final InputGate<?> inputGate = this.decompressorMap.get(decompressor);
		if (inputGate == null) {
			LOG.error("Cannot find input gate to decompressor " + decompressor);
			return false;
		}

		final DecompressorCacheEntry cacheEntry = this.decompressorCache.get(inputGate);
		if (cacheEntry == null) {
			LOG.error("Cannot find decompressor cache entry to input gate " + inputGate);
		}

		cacheEntry.removeAssignedChannel(channelID);

		if (!cacheEntry.hasAssignedChannels()) {
			// Clean up
			this.decompressorCache.remove(inputGate);
			this.decompressorMap.remove(decompressor);
			return true;
		}

		return false;
	}
}
