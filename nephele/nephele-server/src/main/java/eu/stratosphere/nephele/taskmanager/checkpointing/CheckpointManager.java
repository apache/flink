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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;

public class CheckpointManager {

	private static final Log LOG = LogFactory.getLog(CheckpointManager.class);

	private final ByteBufferedChannelManager byteBufferedChannelManager;

	private final Map<ExecutionVertexID, EphemeralCheckpoint> checkpoints = new HashMap<ExecutionVertexID, EphemeralCheckpoint>();

	private final Map<ChannelID, ExecutionVertexID> channelIDToVertexIDMap = new HashMap<ChannelID, ExecutionVertexID>();

	private final String tmpDir;

	public CheckpointManager(ByteBufferedChannelManager byteBufferedChannelManager, String tmpDir) {
		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.tmpDir = tmpDir;
	}

	public void registerFinished(EphemeralCheckpoint checkpoint) {

		final ExecutionVertexID vertexID = checkpoint.getExecutionVertexID();

		synchronized (this.checkpoints) {

			if (this.checkpoints.containsKey(vertexID)) {
				LOG.error("Checkpoint for execution vertex ID " + vertexID + " is already registered");
				return;
			}

			LOG.info("Registering finished checkpoint for vertex " + vertexID);
			this.checkpoints.put(vertexID, checkpoint);
		}

		synchronized (this.channelIDToVertexIDMap) {

			final ChannelID[] outputChannelIDs = checkpoint.getIDsOfCheckpointedOutputChannels();
			for (int i = 0; i < outputChannelIDs.length; i++) {
				this.channelIDToVertexIDMap.put(outputChannelIDs[i], vertexID);
			}
		}
	}

	public String getTmpDir() {

		return this.tmpDir;
	}

	public ExecutionVertexID getExecutionVertexIDByOutputChannelID(ChannelID outputChannelID) {

		synchronized (this.channelIDToVertexIDMap) {
			return this.channelIDToVertexIDMap.get(outputChannelID);
		}
	}

	public void recoverChannelCheckpoint(ChannelID sourceChannelID) {

		ExecutionVertexID executionVertexID = null;
		synchronized (this.channelIDToVertexIDMap) {
			executionVertexID = this.channelIDToVertexIDMap.get(sourceChannelID);
		}

		if (executionVertexID == null) {
			LOG.error("Cannot find execution vertex ID for output channel with ID " + sourceChannelID);
			return;
		}

		EphemeralCheckpoint checkpoint = null;
		synchronized (this.checkpoints) {
			checkpoint = this.checkpoints.get(executionVertexID);
		}

		if (checkpoint == null) {
			LOG.error("Cannot find checkpoint for vertex " + executionVertexID);
			return;
		}

		LOG.info("Recovering checkpoint for channel " + sourceChannelID);
		checkpoint.recoverIndividualChannel(this.byteBufferedChannelManager, sourceChannelID);
	}
}
