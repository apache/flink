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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.fs.FileChannelWrapper;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.io.channels.bytebuffered.ByteBufferedChannelCloseEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.CheckpointSerializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

final class WriteThread extends Thread {

	/**
	 * The log object used to report problems.
	 */
	private static final Log LOG = LogFactory.getLog(WriteThread.class);

	/**
	 * The buffer size in bytes to use for the meta data file channel.
	 */
	private static final int BUFFER_SIZE = 4096;

	private final BlockingQueue<TransferEnvelope> queuedEnvelopes;

	/**
	 * The serializer to convert a transfer envelope into a byte stream.
	 */
	private final CheckpointSerializer transferEnvelopeSerializer = new CheckpointSerializer();

	/**
	 * The current suffix for the name of the file containing the meta data.
	 */
	private int metaDataSuffix = 0;

	/**
	 * The file buffer manager used to allocate file buffers.
	 */
	private final FileBufferManager fileBufferManager;

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
	 * The path to which the checkpoint meta data shall be written to.
	 */
	private final Path checkpointPath;

	private final ExecutionVertexID vertexID;

	/**
	 * The file system to write the checkpoints to.
	 */
	private FileSystem fileSystem;

	/**
	 * The default block size of the file system to write the checkpoints to
	 */
	private long defaultBlockSize = -1L;

	/**
	 * The file channel to write the checkpoint's meta data.
	 */
	private FileChannel metaDataFileChannel = null;

	/**
	 * A counter for the number of bytes in the checkpoint per meta data file.
	 */
	private long numberOfBytesPerMetaDataFile = 0;

	private Buffer firstSerializedFileBuffer = null;

	private volatile boolean hasDataLeft = false;

	private volatile boolean isCanceled = false;

	WriteThread(final FileBufferManager fileBufferManager, final ExecutionVertexID vertexID,
			final int numberOfConnectedChannels) {

		super("Write thread for vertex " + vertexID);

		this.fileBufferManager = fileBufferManager;
		this.vertexID = vertexID;
		this.numberOfConnectedChannels = numberOfConnectedChannels;
		this.queuedEnvelopes = new ArrayBlockingQueue<TransferEnvelope>(256);

		final boolean dist = CheckpointUtils.createDistributedCheckpoint();

		if (dist) {
			final Path p = CheckpointUtils.getDistributedCheckpointPath();
			if (p == null) {
				LOG.error("No distributed checkpoint path configured, writing local checkpoints instead");
				this.checkpointPath = CheckpointUtils.getLocalCheckpointPath();
				this.distributed = false;
			} else {
				this.checkpointPath = p;
				this.distributed = true;
			}
		} else {
			this.checkpointPath = CheckpointUtils.getLocalCheckpointPath();
			this.distributed = false;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		while (!this.isCanceled) {

			TransferEnvelope te = null;

			try {
				te = this.queuedEnvelopes.take();

				try {

					if (writeTransferEnvelope(te)) {
						break;
					}
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}

			} catch (InterruptedException e) {
				if (this.isCanceled) {
					break;
				}
			}
		}

		// Clean up in case we were canceled
		while (!this.queuedEnvelopes.isEmpty()) {
			final TransferEnvelope te = this.queuedEnvelopes.poll();
			final Buffer buffer = te.getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}

		// No more data left to be processed in this write thread
		this.hasDataLeft = false;
	}

	void write(final TransferEnvelope transferEnvelope) throws InterruptedException {

		this.hasDataLeft = true;

		this.queuedEnvelopes.put(transferEnvelope);
	}

	void cancelAndDestroy() {

		this.isCanceled = true;
		interrupt();
		try {
			join();
		} catch (InterruptedException e) {
		}
	}

	private boolean writeTransferEnvelope(final TransferEnvelope transferEnvelope) throws IOException,
			InterruptedException {

		Buffer buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			if (buffer.isBackedByMemory()) {

				// Make sure we transfer the encapsulated buffer to a file and release the memory buffer again
				final Buffer fileBuffer = BufferFactory.createFromFile(buffer.size(), this.vertexID,
					this.fileBufferManager, this.distributed, false);
				buffer.copyToBuffer(fileBuffer);
				transferEnvelope.setBuffer(fileBuffer);
				buffer.recycleBuffer();
			}
		}

		if (this.fileSystem == null) {
			this.fileSystem = this.checkpointPath.getFileSystem();
		}

		if (this.defaultBlockSize < 0L) {
			this.defaultBlockSize = this.fileSystem.getDefaultBlockSize();
		}

		// Finish meta data file when the corresponding checkpoint fraction is 10 times the file system's block size
		if (this.numberOfBytesPerMetaDataFile > 10L * this.defaultBlockSize && !this.distributed) {

			if (this.metaDataFileChannel != null) {
				this.metaDataFileChannel.close();
				this.metaDataFileChannel = null;

				// Rename file
				renameCheckpointPart();

				// Increase the meta data suffix
				++this.metaDataSuffix;
			}

			// Reset counter
			this.numberOfBytesPerMetaDataFile = 0L;
		}

		if (this.metaDataFileChannel == null) {
			this.metaDataFileChannel = getMetaDataFileChannel("_part");
		}

		this.transferEnvelopeSerializer.setTransferEnvelope(transferEnvelope);
		while (this.transferEnvelopeSerializer.write(this.metaDataFileChannel)) {
		}

		// The following code will prevent the underlying file from being closed
		buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			if (this.firstSerializedFileBuffer == null) {
				this.firstSerializedFileBuffer = buffer;
			} else {
				buffer.recycleBuffer();
			}

			// Increase the number of serialized transfer envelopes
			this.numberOfBytesPerMetaDataFile += buffer.size();
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

		if (this.numberOfClosedChannels == this.numberOfConnectedChannels) {

			// Finally, close the underlying file
			if (this.firstSerializedFileBuffer != null) {
				this.firstSerializedFileBuffer.recycleBuffer();
			}

			// Finish meta data file
			if (this.metaDataFileChannel != null) {
				this.metaDataFileChannel.close();

				// Rename file
				renameCheckpointPart();
			}

			// Write the meta data file to indicate the checkpoint is complete
			this.metaDataFileChannel = getMetaDataFileChannel(CheckpointUtils.COMPLETED_CHECKPOINT_SUFFIX);
			this.metaDataFileChannel.write(ByteBuffer.allocate(0));
			this.metaDataFileChannel.close();

			LOG.info("Finished persistent checkpoint for vertex " + this.vertexID);

			return true;
		}

		return false;
	}

	private boolean renameCheckpointPart() throws IOException {

		final Path oldFile = this.checkpointPath.suffix(Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX + "_"
			+ this.vertexID + "_part");

		final Path newFile = this.checkpointPath.suffix(Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX + "_"
			+ this.vertexID + "_" + this.metaDataSuffix);

		if (!this.fileSystem.rename(oldFile, newFile)) {
			LOG.error("Unable to rename " + oldFile + " to " + newFile);
			return false;
		}

		return true;
	}

	private FileChannel getMetaDataFileChannel(final String suffix) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Writing checkpointing meta data to directory " + this.checkpointPath);
		}

		// Bypass FileSystem API for local checkpoints
		if (!this.distributed) {

			final FileOutputStream fos = new FileOutputStream(this.checkpointPath.toUri().getPath()
				+ Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX + "_" + this.vertexID + suffix);

			return fos.getChannel();
		}

		return new FileChannelWrapper(this.fileSystem, this.checkpointPath.suffix(Path.SEPARATOR
			+ CheckpointUtils.METADATA_PREFIX + "_" + this.vertexID + suffix), BUFFER_SIZE, (short) 2); // TODO:
																										// Make
																										// replication
																										// configurable
	}

	boolean hasDataLeft() {

		return this.hasDataLeft;
	}
}
