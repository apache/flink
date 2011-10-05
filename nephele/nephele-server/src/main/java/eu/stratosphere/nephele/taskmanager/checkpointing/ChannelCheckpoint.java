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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.CheckpointOutgoingConnection;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingConnection;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TransferEnvelopeSerializer;
import eu.stratosphere.nephele.util.StringUtils;

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

	private boolean finishCheckpoint =false;

	private FileChannel fileInputChannel;

	private ByteBufferedChannelManager byteBufferedChannelManager;
	
	private CheckpointOutgoingConnection outgoingConnection;
	
	private ArrayList<Long> offsets = new ArrayList<Long>();

	ChannelCheckpoint(ExecutionVertexID executionVertexID, ChannelID sourceChannelID, String tmpDir, ByteBufferedChannelManager byteBufferedChannelManager) {
		this.executionVertexID = executionVertexID;
		this.sourceChannelID = sourceChannelID;
		this.tmpDir = tmpDir;
	}

	public synchronized void addToCheckpoint(TransferEnvelope transferEnvelope) throws IOException {
		if(this.checkpointFinished){
			LOG.error("Recieved Envelope" + transferEnvelope.getSequenceNumber() +" for a finished Checkpoint from " +transferEnvelope.getSource());
		}
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
			Iterator<AbstractEvent> iter = transferEnvelope.getEventList().iterator();
			while(iter.hasNext()){
				if(iter.next() instanceof ByteBufferedChannelCloseEvent){
					LOG.info("Envelope " + transferEnvelope.getSequenceNumber() + "contains Close Event");
					markChannelCheckpointAsFinished(transferEnvelope.getSequenceNumber());
				}
			}
		} else {
			if(!this.queuedEnvelopes.contains(transferEnvelope)){
			// Queue the buffer until there is a checkpointing decision
			this.queuedEnvelopes.add(transferEnvelope);
			}
		}
	}

	private String getFilename() {

		return (this.tmpDir + File.separator + "checkpoint_" + this.executionVertexID.toString() + "_" + this.sourceChannelID
			.toString());
	}

	private synchronized void writeEnvelopeToDisk(TransferEnvelope transferEnvelope) throws IOException {
		TransferEnvelope duplicatTransferEnvelope = transferEnvelope;
		if (transferEnvelope.getProcessingLog().mustBeSentViaNetwork()) {
			// Continue working with a copy of the transfer envelope
			duplicatTransferEnvelope = transferEnvelope.duplicate();
		}
		if (this.fileChannel == null) {

			final FileOutputStream fos = new FileOutputStream(getFilename(),true);
			this.fileChannel = fos.getChannel();

		}

		if (this.transferEnvelopeSerializer == null) {
			this.transferEnvelopeSerializer = new TransferEnvelopeSerializer();
		}


		synchronized (this.transferEnvelopeSerializer) {
			this.transferEnvelopeSerializer.setTransferEnvelope(duplicatTransferEnvelope);
			System.out.println("writing envelope " + duplicatTransferEnvelope.getSequenceNumber()); 

			while (this.transferEnvelopeSerializer.write(this.fileChannel));
			transferEnvelope.getProcessingLog().setWrittenToCheckpoint();
			if(this.finishCheckpoint){
				transferEnvelope.getProcessingLog().setSentViaNetwork();
			}
			this.offsets.add(this.fileChannel.size());
		}
	}

	public synchronized void makePersistent() throws IOException {
		System.out.println("Make Persistent writting " + this.queuedEnvelopes.size() + " envelops for " + this.getFilename() );
		while (!this.queuedEnvelopes.isEmpty()) {

			final TransferEnvelope transferEnvelope = this.queuedEnvelopes.poll();
			writeEnvelopeToDisk(transferEnvelope);
		}

		this.isPersistent = true;
	}

	public synchronized void markChannelCheckpointAsFinished(int sequenzNumber) throws IOException {
		LOG.info("Marking checkpoint as finished after " + (this.expectedSequenceNumber -1) + " for " + sequenzNumber);

		if (this.fileChannel != null) {
			this.fileChannel.force(true);
			this.fileChannel.close();
			this.fileChannel = null;
		}
//		if(this.outgoingConnection != null){
//			this.outgoingConnection.markfinished();
//		}
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


	public synchronized void recover(final ByteBufferedChannelManager byteBufferedChannelManager) {
		if(!this.checkpointFinished){
			LOG.debug("Checkpoint is not yet finished!");

		}

		if (this.fileChannel.isOpen()) {
			LOG.debug("File channel is still open!");
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
			byteBufferedChannelManager.getFileBufferManager().registerExternalDataSourceForChannel(
				this.sourceChannelID,
				filename);
		} catch (IOException ioe) {
			LOG.error(ioe);
		}

		this.fileInputChannel = fis.getChannel();
		if(this.fileInputChannel == null ){
			LOG.error("FileChannel is null");
		}

		// Start recovering


		final CheckpointOutgoingConnection outgoingConnection = byteBufferedChannelManager
		.createOutgoingCheckpointConnection(byteBufferedChannelManager,
			this.fileInputChannel, this.sourceChannelID);
		LOG.info("Recovering from " + filename + " to " + outgoingConnection.getConnectionAddress().getHostName()
			+ ":" + outgoingConnection.getConnectionAddress().getPort() + "//"
			+ outgoingConnection.getConnectionAddress());
		try {
			outgoingConnection.write(this.fileChannel.size());

		}catch (IOException e) {
			e.printStackTrace();
			outgoingConnection.reportTransmissionProblem(e);
			// TODO: Unregister external data source
			return;
		}
		LOG.info("Finished Recovery for channel " + this.sourceChannelID +" to " + outgoingConnection.getConnectionAddress().getHostName());
		System.out.println("Finished Recovery");
		//set ChannelCheckpoint back to normal state
		this.finishCheckpoint = false;
		
	}
	public synchronized void read(ByteBufferedChannelManager byteBufferedChannelManager) {	  
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
			byteBufferedChannelManager.getFileBufferManager().registerExternalDataSourceForChannel(
				this.sourceChannelID,
				filename);
		} catch (IOException ioe) {
			LOG.error(ioe);
		}
		this.fileChannel = fis.getChannel();
		// Start recovering
		final IncomingConnection incomingConnection = new IncomingConnection(byteBufferedChannelManager,
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
	/**
	 * 
	 */
	public void finishCheckpoint() {
		this.finishCheckpoint = true;
	}
	public boolean setFinishCeckpoint(){
		return this.finishCheckpoint;
	}
	
	public ChannelID getSourceChannelID(){
		return this.sourceChannelID;
	}


}
