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
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.FileBufferManager;

/**
 * An ephemeral checkpoint is a checkpoint that can be used to recover from
 * crashed tasks within a processing pipeline. An ephemeral checkpoint is created
 * for each task (more precisely its {@link Environment} object). For file channels
 * an ephemeral checkpoint is always persistent, i.e. data is immediately written to disk.
 * For network channels the ephemeral checkpoint is held into main memory until a checkpoint
 * decision is made. Based on this decision the checkpoint is either made permanent or discarded.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class EphemeralCheckpoint {

	/**
	 * The log object used to report problems.
	 */
	private static final Log LOG = LogFactory.getLog(EphemeralCheckpoint.class);

	/**
	 * The enveloped which are currently queued until the state of the checkpoint is decided
	 */
	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	/**
	 * The ID of the gate this ephemeral checkpoint belongs to.
	 */
	private final GateID gateID;

	/**
	 * The file buffer manager used to allocate file buffers.
	 */
	private final FileBufferManager fileBufferManager;

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
	 * The current state the ephemeral checkpoint is in.
	 */
	private CheckpointingDecisionState checkpointingDecision;

	public EphemeralCheckpoint(final GateID gateID, final boolean ephemeral, final FileBufferManager fileBufferManager) {
		this.checkpointingDecision = (ephemeral ? CheckpointingDecisionState.UNDECIDED
			: CheckpointingDecisionState.NO_CHECKPOINTING);

		this.gateID = gateID;
		this.fileBufferManager = fileBufferManager;
	}

	/**
	 * Adds a transfer envelope to the checkpoint.
	 * 
	 * @param transferEnvelope
	 *        the transfer envelope to be added
	 * @throws IOException
	 *         thrown when an I/O error occurs while writing the envelope to disk
	 */
	public void addTransferEnvelope(TransferEnvelope transferEnvelope) throws IOException {

		if (this.checkpointingDecision == CheckpointingDecisionState.NO_CHECKPOINTING) {
			final Buffer buffer = transferEnvelope.getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}

			return;
		}

		if (this.checkpointingDecision == CheckpointingDecisionState.UNDECIDED) {
			this.queuedEnvelopes.add(transferEnvelope);
			return;
		}

		writeTransferEnvelope(transferEnvelope);
	}

	/**
	 * Returns whether the checkpoint is persistent.
	 * 
	 * @return <code>true</code> if the checkpoint is persistent, <code>false</code> otherwise
	 */
	public boolean isPersistent() {

		return (this.checkpointingDecision == CheckpointingDecisionState.CHECKPOINTING);
	}

	public boolean isDecided() {
		return this.checkpointingDecision != CheckpointingDecisionState.UNDECIDED;
	}

	public boolean isDiscarded() {

		return this.checkpointingDecision == CheckpointingDecisionState.NO_CHECKPOINTING;
	}

	public void destroy() {

		while (!this.queuedEnvelopes.isEmpty()) {

			final TransferEnvelope transferEnvelope = this.queuedEnvelopes.poll();
			final Buffer buffer = transferEnvelope.getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}

		this.checkpointingDecision = CheckpointingDecisionState.NO_CHECKPOINTING;
	}

	public void write() throws IOException {

		while (!this.queuedEnvelopes.isEmpty()) {
			writeTransferEnvelope(this.queuedEnvelopes.poll());
		}

		this.checkpointingDecision = CheckpointingDecisionState.CHECKPOINTING;
	}

	private void writeTransferEnvelope(final TransferEnvelope transferEnvelope) throws IOException {

		final Buffer buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			if (buffer.isBackedByMemory()) {

				// Make sure we transfer the encapsulated buffer to a file and release the memory buffer again
				final Buffer fileBuffer = BufferFactory.createFromFile(buffer.size(), this.gateID,
					this.fileBufferManager);
				buffer.copyToBuffer(fileBuffer);
				transferEnvelope.setBuffer(fileBuffer);
				buffer.recycleBuffer();
			}
		}
		
		//TODO: Implement to write meta data to disk
	}
}
