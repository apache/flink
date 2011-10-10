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
package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author marrus
 *
 */
public class CheckpointConnection extends IncomingConnection {
	/**
	 * The log object used to report debug information and possible errors.
	 */
	private static final Log LOG = LogFactory.getLog(CheckpointConnection.class);
	private ReadableByteChannel readableByteChannel;
	
	private ByteBufferedChannelManager byteBufferedChannelManager;
	private TransferEnvelopeDeserializer deserializer;

	/**
	 * Initializes CheckpointConnection.
	 *
	 * @param byteBufferedChannelManager
	 * @param readableByteChannel
	 */
	public CheckpointConnection(ByteBufferedChannelManager byteBufferedChannelManager,
			ReadableByteChannel readableByteChannel) {
		super(byteBufferedChannelManager, readableByteChannel);
		this.readableByteChannel = readableByteChannel;
		if( readableByteChannel instanceof FileChannel){
			System.out.println("is filechannel "  );
			}
		this.byteBufferedChannelManager = byteBufferedChannelManager;

		this.deserializer = new TransferEnvelopeDeserializer(byteBufferedChannelManager, true);
	}
	
	public void read() throws IOException, InterruptedException {
		this.byteBufferedChannelManager.logBufferUtilization();
		LOG.info("read in checkpoint connection"  );
		this.deserializer.read(this.readableByteChannel);

		final TransferEnvelope transferEnvelope = this.deserializer.getFullyDeserializedTransferEnvelope();
		LOG.info("read " + transferEnvelope.getSequenceNumber() + " from " + transferEnvelope.getSource() + " to " +transferEnvelope.getTarget());
		if (transferEnvelope != null) {
			LOG.info("Queueing in checkpointconnection");
			this.byteBufferedChannelManager.queueOutgoingTransferEnvelope(transferEnvelope);
		}

	}

}
