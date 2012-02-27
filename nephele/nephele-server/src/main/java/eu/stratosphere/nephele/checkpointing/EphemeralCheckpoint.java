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

package eu.stratosphere.nephele.checkpointing;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.FileBufferManager;

/**
 * An ephemeral checkpoint is a checkpoint that can be used to recover from
 * crashed tasks within a processing pipeline. An ephemeral checkpoint is created
 * for each task (more precisely its {@link Environment} object). For file channels
 * an ephemeral checkpoint is always persistent, i.e. data is immediately written to disk.
 * For network channels the ephemeral checkpoint is held into main memory until a checkpoint
 * decision is made. Based on this decision the checkpoint is either made permanent or discarded.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public class EphemeralCheckpoint implements OutputChannelForwarder {

	/**
	 * The log object used to report problems.
	 */
	private static final Log LOG = LogFactory.getLog(EphemeralCheckpoint.class);

	/**
	 * The enveloped which are currently queued until the state of the checkpoint is decided.
	 */
	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	/**
	 * The task this checkpoint is created for.
	 */
	private final RuntimeTask task;

	/**
	 * The number of channels connected to this checkpoint.
	 */
	private final int numberOfConnectedChannels;

	/**
	 * Reference to a write thread that may be spawned to write the checkpoint data asynchronously
	 */
	private WriteThread writeThread = null;

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

	public EphemeralCheckpoint(final RuntimeTask task, final boolean ephemeral) {

		this.task = task;

		// Determine number of output channel
		int nooc = 0;
		final RuntimeEnvironment environment = task.getRuntimeEnvironment();
		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {
			nooc += environment.getOutputGate(i).getNumberOfOutputChannels();
		}

		this.numberOfConnectedChannels = nooc;

		this.checkpointingDecision = (ephemeral ? CheckpointingDecisionState.UNDECIDED
			: CheckpointingDecisionState.CHECKPOINTING);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created checkpoint for vertex " + task.getVertexID() + ", state " + this.checkpointingDecision);
		}

		if (this.checkpointingDecision == CheckpointingDecisionState.CHECKPOINTING) {
			this.task.checkpointStateChanged(CheckpointState.PARTIAL);
			this.writeThread = new WriteThread(FileBufferManager.getInstance(), this.task.getVertexID(),
				this.numberOfConnectedChannels);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy() {

		while (!this.queuedEnvelopes.isEmpty()) {

			final TransferEnvelope transferEnvelope = this.queuedEnvelopes.poll();
			final Buffer buffer = transferEnvelope.getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}

		if (this.writeThread != null) {
			this.writeThread.cancelAndDestroy();
			this.writeThread = null;
		}
	}

	private void write() throws IOException, InterruptedException {

		if (this.writeThread == null) {
			this.writeThread = new WriteThread(FileBufferManager.getInstance(), task.getVertexID(),
				this.numberOfConnectedChannels);
			this.writeThread.start();
		}

		while (!this.queuedEnvelopes.isEmpty()) {
			this.writeThread.write(this.queuedEnvelopes.poll());
		}
	}

	public void setCheckpointDecisionSynchronously(final boolean checkpointDecision) throws IOException,
			InterruptedException {

		if (this.checkpointingDecision != CheckpointingDecisionState.UNDECIDED) {
			return;
		}

		if (checkpointDecision) {
			this.checkpointingDecision = CheckpointingDecisionState.CHECKPOINTING;
			// Write the data which has been queued so far and update checkpoint state
			write();
			this.task.checkpointStateChanged(CheckpointState.PARTIAL);
		} else {
			this.checkpointingDecision = CheckpointingDecisionState.NO_CHECKPOINTING;
			// Simply destroy the checkpoint
			destroy();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean forward(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		if (this.checkpointingDecision == CheckpointingDecisionState.NO_CHECKPOINTING) {
			return true;
		}

		final TransferEnvelope dup = transferEnvelope.duplicate();

		if (this.checkpointingDecision == CheckpointingDecisionState.UNDECIDED) {
			this.queuedEnvelopes.add(dup);
		} else {
			this.writeThread.write(dup);
		}

		return true;
	}

	public boolean isUndecided() {

		return (this.checkpointingDecision == CheckpointingDecisionState.UNDECIDED);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDataLeft() {

		return this.writeThread.hasDataLeft();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(final AbstractEvent event) {
		// TODO Auto-generated method stub

	}
}
