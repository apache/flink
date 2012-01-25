package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProviderBroker;

public final class DefaultDeserializer extends AbstractDeserializer {

	private final BufferProviderBroker bufferProviderBroker;

	private BufferProvider bufferProvider = null;

	private JobID lastDeserializedJobID = null;

	private ChannelID lastDeserializedSourceID = null;

	public DefaultDeserializer(final BufferProviderBroker bufferProviderBroker) {
		this.bufferProviderBroker = bufferProviderBroker;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean readBufferData(final ReadableByteChannel readableByteChannel) throws IOException {

		if (getBuffer() == null) {

			try {
				// Find buffer provider for this channel
				if (!getDeserializedJobID().equals(this.lastDeserializedJobID)
					|| !getDeserializedSourceID().equals(this.lastDeserializedSourceID)) {

					this.bufferProvider = this.bufferProviderBroker.getBufferProvider(getDeserializedJobID(),
						getDeserializedSourceID());

					this.lastDeserializedJobID = getDeserializedJobID();
					this.lastDeserializedSourceID = getDeserializedSourceID();
				}

				setBuffer(this.bufferProvider.requestEmptyBufferBlocking(getSizeOfBuffer()));

				if (getBuffer() == null) {

					Thread.sleep(100);
					// Wait for 100 milliseconds, so the NIO thread won't do busy
					// waiting...

					return true;
				}
			} catch (InterruptedException e) {
				return true;
			}

		} else {

			final Buffer buffer = getBuffer();

			final int bytesWritten = buffer.write(readableByteChannel);

			if (!buffer.hasRemaining()) {
				// We are done, the buffer has been fully read
				buffer.finishWritePhase();
				return false;
			} else {
				if (bytesWritten == -1) {
					throw new IOException("Deserialization error: Expected at least " + buffer.remaining()
						+ " more bytes to follow");
				}
			}
		}

		return true;
	}

	public BufferProvider getBufferProvider() {

		return this.bufferProvider;
	}
}
