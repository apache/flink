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

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;

/**
 * This notifier will be called by a {@link CheckpointReplayTask} after the replay of a checkpoint has been finished,
 * either because all data has been replayed, the same checkpoint shall be replayed by another
 * {@link CheckpointReplayTask} object, or an error occurred.
 * 
 * @author warneke
 */
public interface ReplayFinishedNotifier {

	/**
	 * Indicates the {@link CheckpointReplayTask} for the task represented by the given vertex ID has finished.
	 * 
	 * @param vertexID
	 *        the ID identifying the {@link CheckpointReplayTask} that has finished
	 */
	public void replayFinished(ExecutionVertexID vertexID);
}
