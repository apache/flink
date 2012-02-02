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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;

public class CheckpointReplayManager {
	
	private static final Log LOG = LogFactory.getLog(CheckpointReplayManager.class);

	private final TransferEnvelopeDispatcher transferEnvelopeDispatcher;

	public CheckpointReplayManager(final TransferEnvelopeDispatcher transferEnvelopeDispatcher) {

		this.transferEnvelopeDispatcher = transferEnvelopeDispatcher;
	}

	

	/*public void replayCheckpoint(final ExecutionVertexID vertexID) {

		final CheckpointReplayTask newReplayTask = new CheckpointReplayTask(this, vertexID, this.checkpointDirectory,
			this.transferEnvelopeDispatcher, hasCompleteCheckpointAvailable(vertexID));

		final CheckpointReplayTask runningReplayTask = this.runningReplayTasks.put(vertexID, newReplayTask);
		if (runningReplayTask != null) {
			LOG.info("There is already a replay task running for task " + vertexID + ", cancelling it first...");
			runningReplayTask.cancelAndWait();
		}

		LOG.info("Replaying checkpoint for vertex " + vertexID);
		newReplayTask.start();
	}*/

	
}
