package eu.stratosphere.nephele.checkpointing;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.transferenvelope.CheckpointDeserializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.util.StringUtils;

final class ReplayThread extends Thread {

	private static final String REPLAY_SUFFIX = " (Replay)";

	private final ExecutionVertexID vertexID;

	private final ExecutionObserver executionObserver;

	private final boolean isCheckpointComplete;

	private final Map<ChannelID, ReplayOutputBroker> outputBrokerMap;

	/**
	 * The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds).
	 */
	private static final int SLEEPINTERVAL = 100;

	private final AtomicBoolean restartRequested = new AtomicBoolean(false);

	ReplayThread(final ExecutionVertexID vertexID, final ExecutionObserver executionObserver, final String taskName,
			final boolean isCheckpointComplete, final Map<ChannelID, ReplayOutputBroker> outputBrokerMap) {
		super((taskName == null ? "Unkown" : taskName) + REPLAY_SUFFIX);

		this.vertexID = vertexID;
		this.executionObserver = executionObserver;
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

		while (true) {
			boolean finished = true;
			final Iterator<ReplayOutputBroker> it = this.outputBrokerMap.values().iterator();
			while (it.hasNext()) {

				if (it.next().hasFinished()) {
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

		int metaDataIndex = 0;

		while (true) {

			if (this.restartRequested.compareAndSet(true, false)) {
				metaDataIndex = 0;
			}

			// Try to locate the meta data file
			final File metaDataFile = new File(CheckpointUtils.getCheckpointDirectory() + File.separator
					+ CheckpointUtils.METADATA_PREFIX + "_" + this.vertexID + "_" + metaDataIndex);

			while (!metaDataFile.exists()) {

				// Try to locate the final meta data file
				final File finalMetaDataFile = new File(CheckpointUtils.getCheckpointDirectory() + File.separator
						+ CheckpointUtils.METADATA_PREFIX + "_" + this.vertexID + "_final");

				if (finalMetaDataFile.exists()) {
					return;
				}

				if (this.isCheckpointComplete) {
					throw new FileNotFoundException("Cannot find meta data file " + metaDataIndex
							+ " for checkpoint of vertex " + this.vertexID);
				}

				// Wait for the file to be created
				Thread.sleep(100);

			}

			FileInputStream fis = null;

			try {

				fis = new FileInputStream(metaDataFile);
				final FileChannel fileChannel = fis.getChannel();

				while (true) {
					try {
						deserializer.read(fileChannel);

						final TransferEnvelope transferEnvelope = deserializer.getFullyDeserializedTransferEnvelope();
						if (transferEnvelope != null) {
							outputEnvelope(transferEnvelope);
						}
					} catch (EOFException eof) {
						// Close the file channel
						fileChannel.close();
						// Increase the index of the meta data file
						++metaDataIndex;
						break;
					}
				}
			} finally {
				if (fis != null) {
					fis.close();
				}
			}
		}
	}

	private void outputEnvelope(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		final ReplayOutputBroker outputBroker = this.outputBrokerMap.get(transferEnvelope.getSource());
		if (outputBroker == null) {
			throw new IOException("Cannot find output broker for channel " + transferEnvelope.getSource());
		}

		outputBroker.outputEnvelope(transferEnvelope);
	}
}
