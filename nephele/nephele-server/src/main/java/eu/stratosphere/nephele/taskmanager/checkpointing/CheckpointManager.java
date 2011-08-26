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
import eu.stratosphere.nephele.profiling.CheckpointProfilingData;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;

public class CheckpointManager {

	private static final Log LOG = LogFactory.getLog(CheckpointManager.class);

	private final ByteBufferedChannelManager byteBufferedChannelManager;

	private final Map<ExecutionVertexID, EphemeralCheckpoint> checkpoints = new HashMap<ExecutionVertexID, EphemeralCheckpoint>();

	private final Map<ChannelID, ExecutionVertexID> channelIDToVertexIDMap = new HashMap<ChannelID, ExecutionVertexID>();

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

	/**
	 * Removes the checkpoint of the vertex with the given ID. All files contained in the checkpoint are deleted.
	 * 
	 * @param vertexID
	 *        the vertex whose checkpoint shall be removed
	 */
	public void removeCheckpoint(ExecutionVertexID vertexID) {

		EphemeralCheckpoint checkpoint = null;
		
		// Remove checkpoint from list of available checkpoints
		synchronized(this.checkpoints) {
			
			checkpoint = this.checkpoints.remove(vertexID);
		}
		
		if(checkpoint == null) {
			LOG.error("Cannot find checkpoint for vertex " + vertexID);
			return;
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

	
	public CheckpointProfilingData getProfilingData() throws ProfilingException{
		if(this.taskManager != null){
			return this.taskManager.getCheckpointProfilingData();
		}
		return null;
	}
}
