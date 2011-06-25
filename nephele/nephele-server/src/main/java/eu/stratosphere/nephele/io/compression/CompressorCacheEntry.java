package eu.stratosphere.nephele.io.compression;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;

final class CompressorCacheEntry extends AbstractCacheEntry {

	private final Compressor compressor;

	CompressorCacheEntry(final Compressor compressor, final OutputGate<?> outputGate) {
		this.compressor = compressor;
		for (int i = 0; i < outputGate.getNumberOfOutputChannels(); i++) {
			final AbstractOutputChannel<?> outputChannel = outputGate.getOutputChannel(i);
			addAssignedChannel(outputChannel.getID());
		}
	}

	Compressor getCompressor() {
		return this.compressor;
	}
}
