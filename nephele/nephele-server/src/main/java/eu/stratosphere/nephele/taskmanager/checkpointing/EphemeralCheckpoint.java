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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.taskmanager.bufferprovider.OutOfByteBuffersListener;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.NetworkConnectionManager;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.execution.Environment;

/**
 * An ephemeral checkpoint is a checkpoint that can be used to recover from
 * crashed tasks within a processing pipeline. An ephemeral checkpoint is created
 * for each task (more precisely its {@link Environment} object). For file channels
 * an ephemeral checkpoint is always persistent, i.e. data is immediately written to disk.
 * For network channels the ephemeral checkpoint is held into main memory until a checkpoint
 * decision is made. Based on this decision the checkpoint is either made permanent or discarded.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class EphemeralCheckpoint implements OutOfByteBuffersListener {

	/**
	 * The log object used to report problems.
	 */
	private static final Log LOG = LogFactory.getLog(EphemeralCheckpoint.class);

	/**
	 * This enumeration reflects the possible states an ephemeral
	 * checkpoint can be in.
	 * 
	 * @author warneke
	 */
	private enum CheckpointingDecisionState {
		NO_CHECKPOINTING, UNDECIDED, CHECKPOINTING
	};

	/**
	 * The global checkpoint manager for this task manager.
	 */
	private final CheckpointManager checkpointManager;

	/**
	 * The ID of the execution vertex this ephemeral checkpoint belongs to.
	 */
	private final ExecutionVertexID executionVertexID;

	/**
	 * The current state the ephemeral checkpoint is in.
	 */
	private CheckpointingDecisionState checkpointingDecision;

	/**
	 * A map storing all channel checkpoints which in sum make up this ephemeral checkpoint.
	 */
	private final Map<ChannelID, ChannelCheckpoint> channelCheckpoints = new HashMap<ChannelID, ChannelCheckpoint>();

	/**
	 * Constructs a new ephemeral checkpoint.
	 * 
	 * @param checkpointManager
	 *        the global checkpoint manager for this task manager
	 * @param executionVertexID
	 *        the ID of the execution vertex this ephemeral checkpoint belongs to
	 * @param initialDecisionState
	 *        the initial state the checkpoint shall be in
	 */
	private EphemeralCheckpoint(CheckpointManager checkpointManager, ExecutionVertexID executionVertexID,
			CheckpointingDecisionState initialDecisionState) {
		this.checkpointManager = checkpointManager;
		this.executionVertexID = executionVertexID;
		this.checkpointingDecision = initialDecisionState;
	}

	/**
	 * Creates an ephemeral checkpoint for a task whose output channels are all file channels.
	 * 
	 * @param checkpointManager
	 *        the global checkpoint manager for this task manager
	 * @param executionVertexID
	 *        the ID of the execution vertex this ephemeral checkpoint belongs to
	 * @return the created ephemeral checkpoint
	 */
	public static EphemeralCheckpoint forNetworkChannel(CheckpointManager checkpointManager,
			ExecutionVertexID executionVertexID) {

		return new EphemeralCheckpoint(checkpointManager, executionVertexID,
			CheckpointingDecisionState.NO_CHECKPOINTING); // TODO: Change to undecided
	}

	/**
	 * Creates an ephemeral checkpoint for a task whose output channels are all network channels.
	 * 
	 * @param checkpointManager
	 *        the global checkpoint manager for this task manager
	 * @param executionVertexID
	 *        the ID of the execution vertex this ephemeral checkpoint belongs to
	 * @return the created ephemeral checkpoint
	 */
	public static EphemeralCheckpoint forFileChannel(CheckpointManager checkpointManager,
			ExecutionVertexID executionVertexID) {

		return new EphemeralCheckpoint(checkpointManager, executionVertexID, CheckpointingDecisionState.CHECKPOINTING);
	}

	/**
	 * Returns the ID of the execution vertex this checkpoint belongs to.
	 * 
	 * @return the ID of the execution vertex this checkpoint belongs to
	 */
	public ExecutionVertexID getExecutionVertexID() {
		return this.executionVertexID;
	}

	/**
	 * Returns the IDs of all output channels which are included in this checkpoint.
	 * 
	 * @return the IDs of all output channels which are included in this checkpoint
	 */
	public synchronized ChannelID[] getIDsOfCheckpointedOutputChannels() {

		return this.channelCheckpoints.keySet().toArray(new ChannelID[0]);
	}

	/**
	 * Registers an output channel to be included in this checkpoint.
	 * 
	 * @param sourceChannelID
	 *        the ID of the output channel to be included
	 */
	public synchronized void registerOutputChannel(ChannelID sourceChannelID) {

		if (this.channelCheckpoints.containsKey(sourceChannelID)) {
			LOG.error("Output channel " + sourceChannelID + " is already registered");
			return;
		}

		final ChannelCheckpoint channelCheckpoint = new ChannelCheckpoint(this.executionVertexID, sourceChannelID,
			this.checkpointManager.getTmpDir());
		if (this.checkpointingDecision == CheckpointingDecisionState.CHECKPOINTING) {
			try {
				channelCheckpoint.makePersistent();
			} catch (IOException ioe) {
				LOG.error(ioe);
			}
		}

		this.channelCheckpoints.put(sourceChannelID, channelCheckpoint);
	}

	/**
	 * Adds a transfer envelope to the checkpoint.
	 * 
	 * @param transferEnvelope
	 *        the transfer envelope to be added
	 */
	public synchronized void addTransferEnvelope(TransferEnvelope transferEnvelope) {

		try {
			switch (this.checkpointingDecision) {
			case NO_CHECKPOINTING:
				//TODO: Fix me
				//transferEnvelope.getProcessingLog().setWrittenToCheckpoint();
				return;
			case UNDECIDED:
			case CHECKPOINTING:
				final ChannelCheckpoint channelCheckpoint = this.channelCheckpoints.get(transferEnvelope.getSource());
				if (channelCheckpoint == null) {
					LOG.error("Cannot find output channel with ID " + transferEnvelope.getSource());
					return;
				}

				//TODO: Fix me
				/*if (transferEnvelope.getProcessingLog().mustBeSentViaNetwork()) {
					// Continue working with a copy of the transfer envelope
					transferEnvelope = transferEnvelope.duplicate();
				}*/

				channelCheckpoint.addToCheckpoint(transferEnvelope);
				break;
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			LOG.error(ioe);

			// Checkpoint is now broken anyway, discard it
			discardCheckpoint();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void outOfByteBuffers() {

		if (this.checkpointingDecision != CheckpointingDecisionState.UNDECIDED) {
			return;
		}

		// makeCheckpointPersistent();
		discardCheckpoint();

	}

	/**
	 * Returns whether the checkpoint is persistent.
	 * 
	 * @return <code>true</code> if the checkpoint is persistent, <code>false</code> otherwise
	 */
	public synchronized boolean isPersistent() {

		return (this.checkpointingDecision == CheckpointingDecisionState.CHECKPOINTING);
	}

	/**
	 * Checks if the checkpoint is finished, i.e. all data from all registered output channels
	 * have been fully written to it.
	 * 
	 * @return <code>true</code> if the checkpoint is finished, <code>false</code> otherwise
	 */
	public synchronized boolean isFinished() {

		if (this.checkpointingDecision != CheckpointingDecisionState.CHECKPOINTING) {
			return false;
		}

		final Iterator<ChannelCheckpoint> it = this.channelCheckpoints.values().iterator();
		while (it.hasNext()) {
			if (!it.next().isChannelCheckpointFinished()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Marks an output channel which is included in this checkpoint as finished.
	 * 
	 * @param sourceChannelID
	 *        the ID of the output channel to be marked as finished
	 */
	public synchronized void markChannelAsFinished(ChannelID sourceChannelID) {

		final ChannelCheckpoint channelCheckpoint = this.channelCheckpoints.get(sourceChannelID);
		if (channelCheckpoint == null) {
			LOG.error("Received close notification from unknown channel " + sourceChannelID);
			return;
		}

		try {
			channelCheckpoint.markChannelCheckpointAsFinished();
		} catch (IOException ioe) {
			LOG.error("Error while finishing channel checkpoint sourceChannelID: " + ioe);
		}

		if (this.isFinished()) {
			this.checkpointManager.registerFinished(this);
		}
	}

	/**
	 * Recovers an individual output channel which has been previously been included this checkpoint.
	 * 
	 * @param byteBufferedChannelManager
	 *        the byte buffered channel manager
	 * @param sourceChannelID
	 *        the ID of the output channel which shall be recovered
	 */
	public synchronized void recoverIndividualChannel(ByteBufferedChannelManager byteBufferedChannelManager,
			ChannelID sourceChannelID) {

		final ChannelCheckpoint channelCheckpoint = this.channelCheckpoints.get(sourceChannelID);

		if (channelCheckpoint == null) {
			LOG.error("Cannot find channel checkpoint for channel " + sourceChannelID);
		}

		final NetworkConnectionManager networkConnectionManager = byteBufferedChannelManager
			.getNetworkConnectionManager();
		final FileBufferManager fileBufferManager = byteBufferedChannelManager.getBufferProvider()
			.getFileBufferManager();

		final CheckpointRecoveryThread thread = new CheckpointRecoveryThread(networkConnectionManager,
			fileBufferManager, channelCheckpoint, sourceChannelID);

		thread.start();
	}

	/**
	 * Discards the checkpoint. All data which is either kept in main memory or written to disk
	 * is removed.
	 */
	private void discardCheckpoint() {

		LOG.info("Discarding ephemeral checkpoint for vertex " + this.executionVertexID);

		final Iterator<ChannelCheckpoint> it = this.channelCheckpoints.values().iterator();
		while (it.hasNext()) {
			it.next().discard();
		}

		this.checkpointingDecision = CheckpointingDecisionState.NO_CHECKPOINTING;
	}

	/**
	 * Removes a finished checkpoint. All files included in the checkpoint are deleted.
	 */
	public synchronized void remove() {

		if (!this.isFinished()) {
			LOG.error("Trying to remove an unfinished checkpoint. Request is ignored.");
			return;
		}

		discardCheckpoint();
	}

	/**
	 * Transforms the ephemeral checkpoint into a permanent one. This means all checkpointing data
	 * which is currently held in main memory is written to disk.
	 */
	private void makeCheckpointPersistent() {

		LOG.info("Creating permanent checkpoint for vertex " + this.executionVertexID);

		try {
			final Iterator<ChannelCheckpoint> it = this.channelCheckpoints.values().iterator();
			while (it.hasNext()) {
				it.next().makePersistent();
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			LOG.error(ioe);

			// Checkpoint is now broken anyway, discard it
			discardCheckpoint();
			return;
		}

		// TODO: Uncomment this when feature is full implemented
		// this.checkpointingDecision = CheckpointingDecisionState.CHECKPOINTING;
	}
}
