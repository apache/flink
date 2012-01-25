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

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.taskmanager.transferenvelope.CheckpointDeserializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

class CheckpointReplayTask extends Thread {

	private final ExecutionVertexID vertexID;

	private final String checkpointDirectory;

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	private final boolean isCheckpointComplete;

	CheckpointReplayTask(final ExecutionVertexID vertexID, final String checkpointDirectory,
			final TransferEnvelopeDispatcher transferEnvelopeDispatcher, final boolean isCheckpointComplete) {

		this.vertexID = vertexID;
		this.checkpointDirectory = checkpointDirectory;
		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
		this.isCheckpointComplete = isCheckpointComplete;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		try {
			replayCheckpoint();
		} catch (IOException ioe) {
			// TODO: Handle this correctly
			ioe.printStackTrace();
		} catch (InterruptedException ie) {
			// TODO: Handle this correctly
			ie.printStackTrace();
		}
	}

	private void replayCheckpoint() throws IOException, InterruptedException {

		final CheckpointDeserializer deserializer = new CheckpointDeserializer(this.vertexID);

		int metaDataIndex = 0;
		while (true) {

			// Try to locate the meta data file
			final File metaDataFile = new File(this.checkpointDirectory + File.separator
				+ CheckpointReplayManager.METADATA_PREFIX + "_" + this.vertexID + "_" + metaDataIndex);

			while (!metaDataFile.exists()) {

				// Try to locate the final meta data file
				final File finalMetaDataFile = new File(this.checkpointDirectory + File.separator
					+ CheckpointReplayManager.METADATA_PREFIX + "_" + this.vertexID + "_final");

				if (finalMetaDataFile.exists()) {
					return;
				}

				if (metaDataIndex == 0 || this.isCheckpointComplete) {
					throw new FileNotFoundException("Cannot find meta data file " + metaDataIndex
						+ " for checkpoint of vertex " + this.vertexID);
				}

				// Wait for the file to be created
				Thread.sleep(100);
			}

			final FileInputStream fis = new FileInputStream(metaDataFile);
			final FileChannel fileChannel = fis.getChannel();

			while (true) {
				try {
					deserializer.read(fileChannel);

					final TransferEnvelope transferEnvelope = deserializer.getFullyDeserializedTransferEnvelope();
					if (transferEnvelope != null) {
						this.transferEnvelopeDispatcher.processEnvelopeFromOutputChannel(transferEnvelope);
					}
				} catch (EOFException eof) {
					// Close the file channel
					fileChannel.close();
					// Increase the index of the meta data file
					++metaDataIndex;
					break;
				}
			}
		}

	}
}
