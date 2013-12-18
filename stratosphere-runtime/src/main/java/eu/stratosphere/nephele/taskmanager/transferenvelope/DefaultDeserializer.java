/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

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


	@Override
	protected boolean readBufferData(final ReadableByteChannel readableByteChannel) throws IOException,
			NoBufferAvailableException {

		if (getBuffer() == null) {

			// Find buffer provider for this channel
			if (!getDeserializedJobID().equals(this.lastDeserializedJobID)
				|| !getDeserializedSourceID().equals(this.lastDeserializedSourceID)) {

				try {
					this.bufferProvider = this.bufferProviderBroker.getBufferProvider(getDeserializedJobID(),
						getDeserializedSourceID());
				} catch (InterruptedException e) {
					return true;
				}

				this.lastDeserializedJobID = getDeserializedJobID();
				this.lastDeserializedSourceID = getDeserializedSourceID();
			}

			final Buffer buf = this.bufferProvider.requestEmptyBuffer(getSizeOfBuffer());

			if (buf == null) {
				throw new NoBufferAvailableException(this.bufferProvider);
			}

			setBuffer(buf);

		} else {

			final Buffer buffer = getBuffer();

			final int bytesWritten = buffer.write(readableByteChannel);

			if (!buffer.hasRemaining()) {
				// We are done, the buffer has been fully read
				buffer.flip();
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
