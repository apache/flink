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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.bytebuffered.OutputChannelForwarder;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.taskmanager.transferenvelope.CheckpointSerializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;

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
public class EphemeralCheckpoint implements OutputChannelForwarder {

	/**
	 * The log object used to report problems.
	 */
	private static final Log LOG = LogFactory.getLog(EphemeralCheckpoint.class);

	/**
	 * The number of envelopes to be stored in a single meta data file.
	 */
	private static final int ENVELOPES_PER_META_DATA_FILE = 10000;

	/**
	 * The enveloped which are currently queued until the state of the checkpoint is decided.
	 */
	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	/**
	 * The serializer to convert a transfer envelope into a byte stream.
	 */
	private final CheckpointSerializer transferEnvelopeSerializer = new CheckpointSerializer();

	/**
	 * The task this checkpoint is created for.
	 */
	private final RuntimeTask task;

	/**
	 * The number of channels connected to this checkpoint.
	 */
	private final int numberOfConnectedChannels;

	private final boolean distributed;

	/**
	 * The number of channels which can confirmed not to send any further data.
	 */
	private int numberOfClosedChannels = 0;

	/**
	 * The current suffix for the name of the file containing the meta data.
	 */
	private int metaDataSuffix = 0;

	/**
	 * The file buffer manager used to allocate file buffers.
	 */
	private final FileBufferManager fileBufferManager;

	/**
	 * The file channel to write the checkpoint's meta data.
	 */
	private FileChannel metaDataFileChannel = null;

	/**
	 * A counter for the number of serialized transfer envelopes.
	 */
	private int numberOfSerializedTransferEnvelopes = 0;

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

		this.distributed = CheckpointUtils.createDistributedCheckpoint();

		this.checkpointingDecision = (ephemeral ? CheckpointingDecisionState.UNDECIDED
			: CheckpointingDecisionState.CHECKPOINTING);

		this.fileBufferManager = FileBufferManager.getInstance();

		if (LOG.isDebugEnabled())
			LOG.debug("Created checkpoint for vertex " + task.getVertexID() + ", state " + this.checkpointingDecision);

		if (this.checkpointingDecision == CheckpointingDecisionState.CHECKPOINTING) {
			this.task.checkpointStateChanged(CheckpointState.PARTIAL);
		}
	}

	private void destroy() {

		while (!this.queuedEnvelopes.isEmpty()) {

			final TransferEnvelope transferEnvelope = this.queuedEnvelopes.poll();
			final Buffer buffer = transferEnvelope.getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}
	}

	private void write() throws IOException, InterruptedException {

		while (!this.queuedEnvelopes.isEmpty()) {
			writeTransferEnvelope(this.queuedEnvelopes.poll());
		}
	}

	private boolean renameCheckpointPart(final String checkpointDir) {

		final File oldFile = new File(checkpointDir + File.separator
			+ CheckpointUtils.METADATA_PREFIX + "_"
			+ this.task.getVertexID() + "_part");

		final File newFile = new File(checkpointDir + File.separator
			+ CheckpointUtils.METADATA_PREFIX + "_"
			+ this.task.getVertexID() + "_" + this.metaDataSuffix);

		if (!oldFile.renameTo(newFile)) {
			LOG.error("Unable to rename " + oldFile.getAbsoluteFile() + " to " + newFile.getAbsoluteFile());
			return false;
		}

		return true;
	}

	private void writeTransferEnvelope(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		final String checkpointDir = GlobalConfiguration.getString(
			CheckpointUtils.CHECKPOINT_DIRECTORY_KEY,
			CheckpointUtils.DEFAULT_CHECKPOINT_DIRECTORY);

		final Buffer buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			if (buffer.isBackedByMemory()) {

				// Make sure we transfer the encapsulated buffer to a file and release the memory buffer again
				final Buffer fileBuffer = BufferFactory.createFromFile(buffer.size(), this.task.getVertexID(),
					this.fileBufferManager, this.distributed);
				buffer.copyToBuffer(fileBuffer);
				transferEnvelope.setBuffer(fileBuffer);
				buffer.recycleBuffer();
			}
		}

		// Write the meta data of the transfer envelope to disk
		if (this.numberOfSerializedTransferEnvelopes % ENVELOPES_PER_META_DATA_FILE == 0) {

			if (this.metaDataFileChannel != null) {
				this.metaDataFileChannel.close();
				this.metaDataFileChannel = null;

				// Rename file
				renameCheckpointPart(checkpointDir);

				// Increase the meta data suffix
				++this.metaDataSuffix;
			}
		}

		if (this.metaDataFileChannel == null) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Writing checkpointing meta data to directory " + checkpointDir);
			}
			final FileOutputStream fos = new FileOutputStream(checkpointDir + File.separator
				+ CheckpointUtils.METADATA_PREFIX
				+ "_" + this.task.getVertexID() + "_part");
			this.metaDataFileChannel = fos.getChannel();
		}

		this.transferEnvelopeSerializer.setTransferEnvelope(transferEnvelope);
		while (this.transferEnvelopeSerializer.write(this.metaDataFileChannel)) {
		}

		// Look for close event
		final EventList eventList = transferEnvelope.getEventList();
		if (eventList != null) {
			final Iterator<AbstractEvent> it = eventList.iterator();
			while (it.hasNext()) {
				if (it.next() instanceof ByteBufferedChannelCloseEvent) {
					++this.numberOfClosedChannels;
				}
			}
		}

		// Increase the number of serialized transfer envelopes
		++this.numberOfSerializedTransferEnvelopes;

		if (this.numberOfClosedChannels == this.numberOfConnectedChannels) {

			if (this.metaDataFileChannel != null) {
				this.metaDataFileChannel.close();

				// Rename file
				renameCheckpointPart(checkpointDir);
			}

			new FileOutputStream(checkpointDir + File.separator + CheckpointUtils.METADATA_PREFIX + "_"
				+ this.task.getVertexID() + "_final").close();

			// Since it is unclear whether the underlying physical file will ever be read, we force to close it.
			// TODO: Fix me
			// this.fileBufferManager.forceCloseOfWritableSpillingFile(this.task.getVertexID());

			LOG.info("Finished persistent checkpoint for vertex " + this.task.getVertexID());

			// Send notification that checkpoint is completed
			this.task.checkpointStateChanged(CheckpointState.COMPLETE);
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
			writeTransferEnvelope(dup);
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

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processEvent(final AbstractEvent event) {
		// TODO Auto-generated method stub

	}
}
