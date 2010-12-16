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

package eu.stratosphere.nephele.taskmanager.checkpointing;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingConnection;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TransferEnvelopeSerializer;

public class ChannelCheckpoint {

	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	private final ExecutionVertexID executionVertexID;

	private final ChannelID sourceChannelID;

	private final String tmpDir;

	private boolean isPersistent = false;

	private int expectedSequenceNumber = 0;

	private boolean checkpointFinished = false;

	private FileChannel fileChannel = null;

	private TransferEnvelopeSerializer transferEnvelopeSerializer = null;

	ChannelCheckpoint(ExecutionVertexID executionVertexID, ChannelID sourceChannelID, String tmpDir) {
		this.executionVertexID = executionVertexID;
		this.sourceChannelID = sourceChannelID;
		this.tmpDir = tmpDir;
	}

	public synchronized void addToCheckpoint(TransferEnvelope transferEnvelope) throws IOException {

		if (transferEnvelope.getSequenceNumber() != this.expectedSequenceNumber) {
			throw new IOException("Expected envelope with sequence number " + this.expectedSequenceNumber
				+ " but received " + transferEnvelope.getSequenceNumber());
		}

		// Increase expected sequence number
		++this.expectedSequenceNumber;

		if (!transferEnvelope.getSource().equals(this.sourceChannelID)) {
			throw new IOException("Received envelope with unexpected source channel ID " + transferEnvelope.getSource());
		}

		if (this.isPersistent) {
			this.writeEnvelopeToDisk(transferEnvelope);
		} else {
			// Queue the buffer until there is a checkpointing decision
			this.queuedEnvelopes.add(transferEnvelope);
		}
	}

	private String getFilename() throws IOException {

		return (this.tmpDir + File.separator + "checkpoint_" + this.executionVertexID.toString() + "_" + this.sourceChannelID
			.toString());
	}

	private void writeEnvelopeToDisk(TransferEnvelope transferEnvelope) throws IOException {

		if (this.fileChannel == null) {

			final FileOutputStream fos = new FileOutputStream(getFilename());
			this.fileChannel = fos.getChannel();
		}

		if (this.transferEnvelopeSerializer == null) {
			this.transferEnvelopeSerializer = new TransferEnvelopeSerializer();
		}

		this.transferEnvelopeSerializer.setTransferEnvelope(transferEnvelope);

		while (this.transferEnvelopeSerializer.write(this.fileChannel))
			;

		transferEnvelope.getProcessingLog().setWrittenToCheckpoint();
	}

	public synchronized void makePersistent() throws IOException {

		while (!this.queuedEnvelopes.isEmpty()) {

			final TransferEnvelope transferEnvelope = this.queuedEnvelopes.poll();
			writeEnvelopeToDisk(transferEnvelope);
		}

		this.isPersistent = true;
	}

	public synchronized void markChannelCheckpointAsFinished() throws IOException {

		if (this.fileChannel != null) {
			this.fileChannel.close();
		}

		this.checkpointFinished = true;
	}

	public synchronized boolean isChannelCheckpointFinished() {
		return this.checkpointFinished;
	}

	public synchronized void discard() {

		final Iterator<TransferEnvelope> it = this.queuedEnvelopes.iterator();
		while (it.hasNext()) {

			final TransferEnvelope transferEnvelope = it.next();
			transferEnvelope.getProcessingLog().setWrittenToCheckpoint();
		}

		this.queuedEnvelopes.clear();

		// TODO: Remove any files that may have been written
	}

	public synchronized void recover(ByteBufferedChannelManager byteBufferedChannelManager) throws IOException {

		if (!this.checkpointFinished) {
			throw new IOException("Checkpoint is not finished!");
		}

		if (this.fileChannel.isOpen()) {
			throw new IOException("File channel is still open!");
		}

		// The name of the file which contains the checkpoint
		final String filename = getFilename();

		// Register an external data source at the file buffer manager
		byteBufferedChannelManager.getFileBufferManager().registerExternalDataSourceForChannel(this.sourceChannelID,
			filename);

		// Open file channel for recovery
		final FileInputStream fis = new FileInputStream(filename);
		this.fileChannel = fis.getChannel();

		// Start recovering
		final IncomingConnection incomingConnection = byteBufferedChannelManager
			.registerIncomingConnectionFromCheckpoint();

		try {
			while (true) {
				incomingConnection.read(this.fileChannel);
			}
		} catch (EOFException e) {
			// EOF Exception is expected here
		}

		this.fileChannel.close();
	}
}
