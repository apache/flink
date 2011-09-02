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
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.profiling.CheckpointProfilingData;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;

public class CheckpointManager {

	private static final Log LOG = LogFactory.getLog(CheckpointManager.class);

	private final ByteBufferedChannelManager byteBufferedChannelManager;

	private final Map<ExecutionVertexID, EphemeralCheckpoint> finishedCheckpoints = new HashMap<ExecutionVertexID, EphemeralCheckpoint>();
	private final Map<ExecutionVertexID, EphemeralCheckpoint> ephemeralCheckpoints = new HashMap<ExecutionVertexID, EphemeralCheckpoint>();
	private final Map<ChannelID, ExecutionVertexID> channelIDToVertexIDMap = new HashMap<ChannelID, ExecutionVertexID>();
	private final Map<ChannelID, ExecutionVertexID> channelIDToVertexIDMapUnfinished = new HashMap<ChannelID, ExecutionVertexID>();
	private final String tmpDir;

	private TaskManager taskManager;

	public CheckpointManager(ByteBufferedChannelManager byteBufferedChannelManager, String tmpDir) {
		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.tmpDir = tmpDir;
	}
	public CheckpointManager(ByteBufferedChannelManager byteBufferedChannelManager, String tmpDir,
			TaskManager taskManager) {
		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.tmpDir = tmpDir;
		this.taskManager = taskManager;
	}
	public void registerFinished(EphemeralCheckpoint checkpoint) {

		final ExecutionVertexID vertexID = checkpoint.getExecutionVertexID();

		synchronized (this.finishedCheckpoints) {

			if (this.finishedCheckpoints.containsKey(vertexID)) {
				LOG.error("Checkpoint for execution vertex ID " + vertexID + " is already registered");
				return;
			}

			LOG.info("Registering finished checkpoint for vertex " + vertexID);
			this.finishedCheckpoints.put(vertexID, checkpoint);
		}

		synchronized (this.channelIDToVertexIDMap) {

			final ChannelID[] outputChannelIDs = checkpoint.getIDsOfCheckpointedOutputChannels();
			for (int i = 0; i < outputChannelIDs.length; i++) {
				this.channelIDToVertexIDMap.put(outputChannelIDs[i], vertexID);
			}
		}
	}

	/**
	 * Removes the checkpoint of the vertex with the given ID. All files contained in the checkpoint are deleted.
	 * 
	 * @param vertexID
	 *        the vertex whose checkpoint shall be removed
	 */
	public void removeCheckpoint(ExecutionVertexID vertexID) {

		EphemeralCheckpoint checkpoint = null;
		
		// Remove checkpoint from list of available checkpoints
		synchronized(this.finishedCheckpoints) {
			
			checkpoint = this.finishedCheckpoints.remove(vertexID);
		}
		
		if(checkpoint == null) {
			LOG.error("Cannot find checkpoint for vertex " + vertexID);
			return;
		}
		
		// Clean up channel ID to vertex ID map
		ChannelID[] checkpointedChannels = checkpoint.getIDsOfCheckpointedOutputChannels();
		
		if(checkpointedChannels == null) {
			LOG.error("Cannot remove checkpoint for vertex " + vertexID + ": list of checkpointed channels is null");
			return;
		}
		
		synchronized(this.channelIDToVertexIDMap) {
			
			for(int i = 0; i < checkpointedChannels.length; i++) {
				this.channelIDToVertexIDMap.remove(checkpointedChannels[i]);
			}
		}
		
		// Finally, trigger deletion of files
		checkpoint.remove();
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
		synchronized (this.finishedCheckpoints) {
			checkpoint = this.finishedCheckpoints.get(executionVertexID);
		}

		if (checkpoint == null) {
			LOG.error("Cannot find checkpoint for vertex " + executionVertexID);
			return;
		}

		LOG.info("Recovering checkpoint for channel " + sourceChannelID);
		checkpoint.recoverIndividualChannel(this.byteBufferedChannelManager, sourceChannelID);
		
		
		
	}
	public void readChannelCheckpoint(ChannelID sourceChannelID) {

		ExecutionVertexID executionVertexID = null;
		synchronized (this.channelIDToVertexIDMap) {
			executionVertexID = this.channelIDToVertexIDMap.get(sourceChannelID);
		}

		if (executionVertexID == null) {
			LOG.error("Cannot find execution vertex ID for output channel with ID " + sourceChannelID);
			return;
		}

		EphemeralCheckpoint checkpoint = null;
		synchronized (this.finishedCheckpoints) {
			checkpoint = this.finishedCheckpoints.get(executionVertexID);
		}

		if (checkpoint == null) {
			LOG.error("Cannot find checkpoint for vertex " + executionVertexID);
			return;
		}

		LOG.info("Recovering checkpoint for channel " + sourceChannelID);
		checkpoint.readIndividualChannel(this.byteBufferedChannelManager, sourceChannelID);
		
		
		
	}
	public CheckpointProfilingData getProfilingData() throws ProfilingException{
		if(this.taskManager != null){
			return this.taskManager.getCheckpointProfilingData();
		}
		return null;
	}
	
	public void registerEphermalCheckpoint(EphemeralCheckpoint checkpoint){
		this.byteBufferedChannelManager.registerOutOfWriterBuffersListener(checkpoint);
		this.ephemeralCheckpoints.put(checkpoint.getExecutionVertexID(), checkpoint);
	}
	public void unregisterEphermalCheckpoint(EphemeralCheckpoint checkpoint){
		this.byteBufferedChannelManager.unregisterOutOfWriterBuffersLister(checkpoint);
		this.ephemeralCheckpoints.remove(checkpoint.getExecutionVertexID());
	}
	/**
	 * @param executionVertexID
	 */
	public void reportPersistenCheckpoint(ExecutionVertexID executionVertexID,ChannelID sourceChannelID) {
		this.taskManager.reportPersistenCheckpoint(executionVertexID);
		this.channelIDToVertexIDMap.put(sourceChannelID,executionVertexID);
		
		
	}
	/**
	 * @param sourceChannelID
	 */
	public void recoverAllChannelCheckpoints(ChannelID sourceChannelID) {
		ExecutionVertexID executionVertexID = null;
		synchronized (this.channelIDToVertexIDMap) {
			executionVertexID = this.channelIDToVertexIDMap.get(sourceChannelID);
		}

		if (executionVertexID == null) {
			LOG.error("Cannot find execution vertex ID for output channel with ID " + sourceChannelID);
			return;
		}

		EphemeralCheckpoint checkpoint = null;
		synchronized (this.ephemeralCheckpoints) {
			checkpoint = this.ephemeralCheckpoints.get(executionVertexID);
		}

		if (checkpoint == null) {
			LOG.error("Cannot find checkpoint for vertex " + executionVertexID);
			return;
		}

		LOG.info("Recovering all checkpoints for vertex " + executionVertexID);
		checkpoint.recoverAllChannels(this.byteBufferedChannelManager);
	}
}
