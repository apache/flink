package eu.stratosphere.nephele.checkpointing;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.fs.FileChannelWrapper;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.transferenvelope.CheckpointDeserializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.util.StringUtils;

final class ReplayThread extends Thread {

	private static final String REPLAY_SUFFIX = " (Replay)";

	/**
	 * The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds).
	 */
	private static final int SLEEPINTERVAL = 100;

	/**
	 * The buffer size in bytes to use for the meta data file channel.
	 */
	private static final int BUFFER_SIZE = 4096;

	private final ExecutionVertexID vertexID;

	private final ExecutionObserver executionObserver;

	private final boolean isCheckpointLocal;

	private final boolean isCheckpointComplete;

	private final Map<ChannelID, ReplayOutputBroker> outputBrokerMap;

	private final AtomicBoolean restartRequested = new AtomicBoolean(false);

	ReplayThread(final ExecutionVertexID vertexID, final ExecutionObserver executionObserver, final String taskName,
			final boolean isCheckpointLocal, final boolean isCheckpointComplete,
			final Map<ChannelID, ReplayOutputBroker> outputBrokerMap) {
		super((taskName == null ? "Unkown" : taskName) + REPLAY_SUFFIX);

		this.vertexID = vertexID;
		this.executionObserver = executionObserver;
		this.isCheckpointLocal = isCheckpointLocal;
		this.isCheckpointComplete = isCheckpointComplete;
		this.outputBrokerMap = outputBrokerMap;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		// Now the actual program starts to run
		changeExecutionState(ExecutionState.REPLAYING, null);

		// If the task has been canceled in the mean time, do not even start it
		if (this.executionObserver.isCanceled()) {
			changeExecutionState(ExecutionState.CANCELED, null);
			return;
		}

		try {

			replayCheckpoint();

			// Make sure, we enter the catch block when the task has been canceled
			if (this.executionObserver.isCanceled()) {
				throw new InterruptedException();
			}

		} catch (Exception e) {

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Task finished running, but there may be some unconsumed data in the brokers
		changeExecutionState(ExecutionState.FINISHING, null);

		try {
			// Wait until all output broker have sent all of their data
			waitForAllOutputBrokerToFinish();
		} catch (Exception e) {

			if (this.executionObserver.isCanceled()) {
				changeExecutionState(ExecutionState.CANCELED, null);
			} else {
				changeExecutionState(ExecutionState.FAILED, StringUtils.stringifyException(e));
			}

			return;
		}

		// Finally, switch execution state to FINISHED and report to job manager
		changeExecutionState(ExecutionState.FINISHED, null);
	}

	private void waitForAllOutputBrokerToFinish() throws IOException, InterruptedException {

		while (!this.executionObserver.isCanceled()) {
			boolean finished = true;
			final Iterator<ReplayOutputBroker> it = this.outputBrokerMap.values().iterator();
			while (it.hasNext()) {

				if (!it.next().hasFinished()) {
					finished = false;
				}
			}

			if (finished) {
				break;
			}

			Thread.sleep(SLEEPINTERVAL);
		}
	}

	private void changeExecutionState(final ExecutionState newExecutionState, final String optionalMessage) {

		if (this.executionObserver != null) {
			this.executionObserver.executionStateChanged(newExecutionState, optionalMessage);
		}
	}

	void restart() {

		this.restartRequested.set(true);
	}

	private void replayCheckpoint() throws Exception {

		final CheckpointDeserializer deserializer = new CheckpointDeserializer(this.vertexID);

		final Path checkpointPath = this.isCheckpointLocal ? CheckpointUtils.getLocalCheckpointPath() : CheckpointUtils
			.getDistributedCheckpointPath();

		if (checkpointPath == null) {
			throw new IOException("Cannot determine checkpoint path for vertex " + this.vertexID);
		}

		// The file system the checkpoint's meta data is stored on
		final FileSystem fileSystem = checkpointPath.getFileSystem();

		int metaDataIndex = 0;

		Buffer firstDeserializedFileBuffer = null;
		FileChannel fileChannel = null;

		try {

			while (true) {

				if (this.restartRequested.compareAndSet(true, false)) {
					metaDataIndex = 0;
				}

				// Try to locate the meta data file
				final Path metaDataFile = checkpointPath.suffix(Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX
					+ "_" + this.vertexID + "_" + metaDataIndex);

				while (!fileSystem.exists(metaDataFile)) {

					// Try to locate the final meta data file
					final Path finalMetaDataFile = checkpointPath.suffix(Path.SEPARATOR
						+ CheckpointUtils.METADATA_PREFIX
						+ "_" + this.vertexID + "_final");

					if (fileSystem.exists(finalMetaDataFile)) {
						return;
					}

					if (this.isCheckpointComplete) {
						throw new FileNotFoundException("Cannot find meta data file " + metaDataIndex
							+ " for checkpoint of vertex " + this.vertexID);
					}

					// Wait for the file to be created
					Thread.sleep(100);

				}

				fileChannel = getFileChannel(fileSystem, metaDataFile);

				while (true) {
					try {
						deserializer.read(fileChannel);

						final TransferEnvelope transferEnvelope = deserializer
								.getFullyDeserializedTransferEnvelope();
						if (transferEnvelope != null) {

							final ReplayOutputBroker broker = this.outputBrokerMap
									.get(transferEnvelope.getSource());
							if (broker == null) {
								throw new IOException("Cannot find output broker for channel "
										+ transferEnvelope.getSource());
							}

							final Buffer srcBuffer = transferEnvelope.getBuffer();
							if (srcBuffer != null) {

								// Prevent underlying file from being closed
								if (firstDeserializedFileBuffer == null) {
									firstDeserializedFileBuffer = srcBuffer.duplicate();
								}

								final Buffer destBuffer = broker.requestEmptyBufferBlocking(srcBuffer.size());
								srcBuffer.copyToBuffer(destBuffer);
								transferEnvelope.setBuffer(destBuffer);
								srcBuffer.recycleBuffer();
							}

							broker.outputEnvelope(transferEnvelope);
						}
					} catch (EOFException eof) {
						// Close the file channel
						fileChannel.close();
						fileChannel = null;
						// Increase the index of the meta data file
						++metaDataIndex;
						break;
					}
				}
			}

		} finally {
			if (firstDeserializedFileBuffer != null) {
				firstDeserializedFileBuffer.recycleBuffer();
				firstDeserializedFileBuffer = null;
			}
			if (fileChannel != null) {
				fileChannel.close();
				fileChannel = null;
			}
		}
	}

	private FileChannel getFileChannel(final FileSystem fs, final Path p) throws IOException {

		// Bypass FileSystem API for local checkpoints
		if (this.isCheckpointLocal) {

			final URI uri = p.toUri();
			return new FileInputStream(uri.getPath()).getChannel();
		}

		return new FileChannelWrapper(fs, p, BUFFER_SIZE, (short) -1);
	}
}
