/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A successful checkpoint describes a checkpoint after all required tasks acknowledged it (with their state)
 * and that is considered completed.
 */
public class SuccessfulCheckpoint {
	
	private static final Logger LOG = LoggerFactory.getLogger(SuccessfulCheckpoint.class);
	
	private final JobID job;
	
	private final long checkpointID;
	
	private final long timestamp;
	
	private final List<StateForTask> states;


	public SuccessfulCheckpoint(JobID job, long checkpointID, long timestamp, List<StateForTask> states) {
		this.job = job;
		this.checkpointID = checkpointID;
		this.timestamp = timestamp;
		this.states = states;
	}

	public JobID getJobId() {
		return job;
	}

	public long getCheckpointID() {
		return checkpointID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public List<StateForTask> getStates() {
		return states;
	}

	// --------------------------------------------------------------------------------------------
	
	public void discard(ClassLoader userClassLoader) {
		for(StateForTask state: states){
			state.discard(userClassLoader);
		}
		states.clear();
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("Checkpoint %d @ %d for %s", checkpointID, timestamp, job);
	}
}
