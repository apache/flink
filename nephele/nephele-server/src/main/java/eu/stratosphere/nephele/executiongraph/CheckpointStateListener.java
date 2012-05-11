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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Classes implementing the {@link CheckpointStateListener} interface can register for notifications about state changes
 * of
 * a vertex's checkpoint.
 * 
 * @author warneke
 */
public interface CheckpointStateListener {

	/**
	 * Called to notify a change in the vertex's checkpoint state
	 * 
	 * @param jobID
	 *        the ID of the job the event belongs to
	 * @param vertexID
	 *        the ID of the vertex whose checkpoint state has changed
	 * @param newCheckpointState
	 *        the new state of the vertex's checkpoint
	 */
	void checkpointStateChanged(JobID jobID, ExecutionVertexID vertexID, CheckpointState newCheckpointState);
}
