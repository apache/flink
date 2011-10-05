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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairRequest;
import eu.stratosphere.nephele.io.channels.bytebuffered.BufferPairResponse;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.taskmanager.checkpointing.CheckpointManager;
import eu.stratosphere.nephele.taskmanager.checkpointing.EphemeralCheckpoint;

/**
 * A byte buffered output channel group forwards all outgoing {@link TransferEnvelope} objects from
 * all the output channels that belong to the same {@link Environment} at runtime. Its purpose is to
 * introduce a central object for each task that forwards all buffers to the {@link Environment} object's
 * checkpoint if necessary.
 * 
 * @author warneke
 */
public class ByteBufferedOutputChannelGroup implements WriteBufferRequestor {

	/**
	 * The byte buffered channel manager.
	 */
	private final ByteBufferedChannelManager byteBufferedChannelManager;

	/**
	 * The ephemeral checkpoint assigned to this {@link Environment}, possibly <code>null</code>.
	 */
	private final EphemeralCheckpoint ephemeralCheckpoint;

	/**
	 * The common channel type of all of the task's output channels, possibly <code>null</code>.
	 */
	private final ChannelType commonChannelType;

	/**
	 * Stores those channels which current hold at least one write buffer
	 */
	private final Set<ByteBufferedOutputChannelWrapper> channelsWithWriteBuffers = new HashSet<ByteBufferedOutputChannelWrapper>();
	/**
	 * Indicates whether we want to finish the Checkpoint without sending via Network, after a connection error.
	 */
	private boolean finishCheckpoint = false;


	/**
	 * Constructs a new byte buffered output channel group object.
	 * 
	 * @param byteBufferedChannelManager
	 *        the byte buffered channel manager this object is attached to
	 * @param checkpointManager
	 *        the checkpoint manager used to create ephemeral checkpoints
	 * @param commonChannelType
	 *        the channel type all of the channels attached to this group have in common, possibly <code>null</code>
	 * @param executionVertexID
	 *        the id of the execution vertex this channel group object belongs to
	 */
	public ByteBufferedOutputChannelGroup(ByteBufferedChannelManager byteBufferedChannelManager,
			CheckpointManager checkpointManager, ChannelType commonChannelType, ExecutionVertexID executionVertexID) {

		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.commonChannelType = commonChannelType;
		if (commonChannelType == ChannelType.FILE) {
			// For file channels, we always store data in the checkpoint
			this.ephemeralCheckpoint = EphemeralCheckpoint.forFileChannel(checkpointManager, executionVertexID);
		} else if (commonChannelType == ChannelType.NETWORK) {
			// For network channels, we decide online whether to use checkpoints
			this.ephemeralCheckpoint = EphemeralCheckpoint.forNetworkChannel(checkpointManager, executionVertexID);
		} else {
			// Otherwise, we do not use checkpoints
			this.ephemeralCheckpoint = null;
		}

		// Register checkpoint as a listener to receive out-of-buffer notifications
		if (this.ephemeralCheckpoint != null) {
			this.byteBufferedChannelManager.registerOutOfWriterBuffersListener(this.ephemeralCheckpoint);
		}
	}

	/**
	 * Called by the attached output channel wrapper to forward a {@link TransferEnvelope} object
	 * to its final destination. Within this method the provided transfer envelope is possibly also
	 * forwarded to the assigned ephemeral checkpoint.
	 * 
	 * @param channelWrapper
	 *        the channel wrapper which called this method
	 * @param outgoingTransferEnvelope
	 *        the transfer envelope to be forwarded
	 * @throws IOException
	 *         thrown if an I/O error occurs while processing the envelope
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the envelope to be processed
	 */
	public void processEnvelope(ByteBufferedOutputChannelWrapper channelWrapper,
			TransferEnvelope outgoingTransferEnvelope) throws IOException, InterruptedException {

		final TransferEnvelopeProcessingLog processingLog = outgoingTransferEnvelope.getProcessingLog();

		// Check if the provided envelope must be written to the checkpoint
		if (this.ephemeralCheckpoint != null && processingLog.mustBeWrittenToCheckpoint()) {

			this.ephemeralCheckpoint.addTransferEnvelope(outgoingTransferEnvelope);
			
			// Look for a close event
			final EventList eventList = outgoingTransferEnvelope.getEventList();
			if (!eventList.isEmpty()) {

				final Iterator<AbstractEvent> it = eventList.iterator();
				while (it.hasNext()) {

					if (it.next() instanceof ByteBufferedChannelCloseEvent && this.commonChannelType == ChannelType.FILE) {;
						System.out.println("Close Event in " + outgoingTransferEnvelope.getSequenceNumber() + " source " + outgoingTransferEnvelope.getSource());
						// Mark corresponding channel as closed
						this.ephemeralCheckpoint.markChannelAsFinished(outgoingTransferEnvelope.getSource(), outgoingTransferEnvelope.getSequenceNumber());

						// If checkpoint is persistent it is save to acknowledge the close event
						if (this.ephemeralCheckpoint.isPersistent() && this.commonChannelType == ChannelType.FILE) {
							channelWrapper.processEvent(new ByteBufferedChannelCloseEvent());
						}

						break;
					}
				}
			}
		}

		// Check if the provided envelope must be sent via the network
		if (processingLog.mustBeSentViaNetwork() && !processingLog.isSentViaNetwork()) {
			//If we just want to finish the checkpoint mark the envelope as send.
				this.byteBufferedChannelManager.queueOutgoingTransferEnvelope(outgoingTransferEnvelope);

		}

		if (outgoingTransferEnvelope.getBuffer() != null) {
			this.channelsWithWriteBuffers.remove(channelWrapper);
		}
	}

	/**
	 * Called by the channel wrapper to retrieve a new processing log for a
	 * transfer envelope. The processing log determines whether the envelope
	 * is later written to the checkpoint, sent via the network, or both.
	 * 
	 * @param individualChannelType
	 *        the type of the individual channel asking for the processing log
	 * @return the newly created processing log.
	 */
	public TransferEnvelopeProcessingLog getProcessingLog(final ChannelType individualChannelType) {

		return new TransferEnvelopeProcessingLog((individualChannelType == ChannelType.NETWORK),
			(this.ephemeralCheckpoint != null));
	}

	/**
	 * Returns the maximum size of available write buffers in bytes.
	 * 
	 * @return the maximum size of available write buffers in bytes
	 */
	public int getMaximumBufferSize() {
		return this.byteBufferedChannelManager.getMaximumBufferSize();
	}

	/**
	 * Called by the framework to register an output channel with
	 * this channel group.
	 * 
	 * @param channelID
	 *        the ID of the output channel to be registered
	 */
	public void registerOutputChannel(ChannelID channelID) {

		if (this.ephemeralCheckpoint != null) {
			this.ephemeralCheckpoint.registerOutputChannel(channelID, byteBufferedChannelManager);
		}
	}

	/**
	 * Forwards a buffer request to the byte buffered channel manager. This method
	 * blocks until the buffer request can be served.
	 * 
	 * @param byteBufferPair
	 *        the buffer request to be forwarded
	 * @return the buffers wrapped in a {@link BufferPairResponse} object
	 * @throws InterruptedException
	 *         thrown if the task thread is interrupted while waiting for the buffers
	 */
	public BufferPairResponse requestEmptyWriteBuffers(ByteBufferedOutputChannelWrapper wrapper,
			BufferPairRequest byteBufferPair) throws InterruptedException {

		final BufferPairResponse bufferPairResponse = this.byteBufferedChannelManager
			.requestEmptyWriteBuffers(this, byteBufferPair);

		if (bufferPairResponse.getCompressedDataBuffer() != null
			|| bufferPairResponse.getUncompressedDataBuffer() != null) {

			this.channelsWithWriteBuffers.add(wrapper);
		}

		return bufferPairResponse;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void outOfWriteBuffers() throws InterruptedException {

		final Iterator<ByteBufferedOutputChannelWrapper> it = this.channelsWithWriteBuffers.iterator();
		int minRemaining = -1;
		ByteBufferedOutputChannelWrapper minWrapper = null;

		while (it.hasNext()) {

			final ByteBufferedOutputChannelWrapper wrapper = it.next();
			final int remaining = wrapper.getRemainingBytesOfWorkingBuffer();
			if (remaining > 0) {

				if (minWrapper == null) {
					minWrapper = wrapper;
					minRemaining = remaining;
				} else {
					if (remaining < minRemaining) {
						minRemaining = remaining;
						minWrapper = wrapper;
					}
				}
			}
		}

		if (minWrapper != null) {

			try {
				minWrapper.flush();
			} catch (IOException ioe) {
				minWrapper.reportIOException(ioe);
			}
		}
	}
	/**
	 * returns whether the ChannelGroup is a Checkpoint
	 * @return boolean indicating whether the checkpoint is persistent
	 */
	public boolean isCheckpoint(){
		return this.ephemeralCheckpoint.isPersistent();
	}

	/**
	 * this method is called if a connection error occurred,
	 *  to finish the Checkpoint without sending Envelopes to over the Network.
	 *  
	 */
	public void finishCheckpoint() {
		this.finishCheckpoint  = true;
		ephemeralCheckpoint.finishCheckpoint();
	}
}
