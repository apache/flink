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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingConnection;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingConnectionID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.NetworkConnectionManager;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeSerializer;

public class ChannelCheckpoint {

	/**
	 * The log object used to report debug information and possible errors.
	 */
	private static final Log LOG = LogFactory.getLog(ChannelCheckpoint.class);

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

	private String getFilename() {

		return (this.tmpDir + File.separator + "checkpoint_" + this.executionVertexID.toString() + "_" + this.sourceChannelID
			.toString());
	}

	private void writeEnvelopeToDisk(TransferEnvelope transferEnvelope) throws IOException {
		TransferEnvelope duplicatTransferEnvelope = transferEnvelope;
		if (transferEnvelope.getProcessingLog().mustBeSentViaNetwork()) {
			// Continue working with a copy of the transfer envelope
			duplicatTransferEnvelope = transferEnvelope.duplicate();
		}
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

		//TODO: Mark transfer envelope as processed
		//transferEnvelope.getProcessingLog().setWrittenToCheckpoint();
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
			//TODO: Mark transfer envelope as processed
			//transferEnvelope.getProcessingLog().setWrittenToCheckpoint();
		}

		this.queuedEnvelopes.clear();

		// Remove any files that may have been written
		final File file = new File(getFilename());
		if (file.exists()) {
			try {
				file.delete();
			} catch (SecurityException e) {
				LOG.error(e);
			}
		}
	}

	public synchronized void recover(final NetworkConnectionManager networkConnectionManager,
			final FileBufferManager fileBufferManager) {

		if (!this.checkpointFinished) {
			LOG.error("Checkpoint is not finished!");
		}

		if (this.fileChannel.isOpen()) {
			LOG.error("File channel is still open!");
		}

		// The name of the file which contains the checkpoint
		final String filename = getFilename();

		// Open file channel for recovery
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(filename);
		} catch (IOException ioe) {
			LOG.error(ioe);
			return;
		}

		// Register an external data source at the file buffer manager
		try {
			fileBufferManager.registerExternalDataSourceForChannel(
				this.sourceChannelID,
				filename);
		} catch (IOException ioe) {
			LOG.error(ioe);
		}

		this.fileChannel = fis.getChannel();

		// Start recovering
		final IncomingConnection incomingConnection = networkConnectionManager.registerIncomingConnection(connectionID,
			this.fileChannel);

		try {
			while (true) {
				incomingConnection.read();
			}
		} catch (EOFException e) {
			// EOF Exception is expected here
		} catch (IOException e) {
			incomingConnection.reportTransmissionProblem(null, e);
			// TODO: Unregister external data source
			return;
		} catch (InterruptedException e) {
			LOG.info("Caught interrupted exception: " + StringUtils.stringifyException(e));
		}

		// TODO: Unregister external data source
		incomingConnection.closeConnection(null);
	}
}
