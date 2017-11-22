/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * This class will service as a task-manager-level local storage for local checkpointed state. The purpose is to provide
 * access to a state that is stored locally for a faster recovery compared to the state that is stored remotely in a
 * stable store DFS. For now, this storage is only complementary to the stable storage and local state is typically
 * lost in case of machine failures. In such cases (and others), client code of this class must fall back to using the
 * slower but highly available store.
 *
 * TODO this is currently a placeholder / mock that still must be implemented!
 */
public class TaskLocalStateStore {

	/** */
	private final JobID jobID;

	/** */
	private final JobVertexID jobVertexID;

	/** */
	private final int subtaskIndex;

	public TaskLocalStateStore(
		JobID jobID,
		JobVertexID jobVertexID,
		int subtaskIndex) {

		this.jobID = jobID;
		this.jobVertexID = jobVertexID;
		this.subtaskIndex = subtaskIndex;
	}

	public void storeSnapshot(/* TODO */) {
		throw new UnsupportedOperationException("TODO!");
	}

	public void dispose() {
		throw new UnsupportedOperationException("TODO!");
	}
}
