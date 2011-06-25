package eu.stratosphere.nephele.io.compression;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;

final class DecompressorCacheEntry extends AbstractCacheEntry {

	private final Decompressor decompressor;

	DecompressorCacheEntry(final Decompressor decompressor, final InputGate<?> inputGate) {

		this.decompressor = decompressor;
		for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
			final AbstractInputChannel<?> inputChannel = inputGate.getInputChannel(i);
			addAssignedChannel(inputChannel.getID());
		}
	}

	Decompressor getDecompressor() {
		return this.decompressor;
	}
}
