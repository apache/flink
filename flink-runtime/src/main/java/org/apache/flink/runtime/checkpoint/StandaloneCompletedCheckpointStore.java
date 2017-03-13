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

import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link HighAvailabilityMode#NONE}.
 */
public class StandaloneCompletedCheckpointStore implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(StandaloneCompletedCheckpointStore.class);

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/** The registry for completed checkpoints to register used state objects. */
	private final StateRegistry stateRegistry;

	/** The completed checkpoints. */
	private final ArrayDeque<CompletedCheckpoint> checkpoints;

	/**
	 * Creates {@link StandaloneCompletedCheckpointStore}.
	 *
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
	 *                                       least 1). Adding more checkpoints than this results
	 *                                       in older checkpoints being discarded.
	 */
	public StandaloneCompletedCheckpointStore(int maxNumberOfCheckpointsToRetain) {
		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
		this.checkpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);
		this.stateRegistry = new StateRegistry();
	}

	@Override
	public void recover() throws Exception {
		// Nothing to do
	}

	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {

		checkpoint.register(stateRegistry);
		checkpoints.addLast(checkpoint);

		if (checkpoints.size() > maxNumberOfCheckpointsToRetain) {
			try {
				CompletedCheckpoint oldCheckpoint = checkpoints.remove();
				List<StateObject> discardedStates = oldCheckpoint.unregister(stateRegistry);
				oldCheckpoint.subsume(discardedStates);
			} catch (Exception e) {
				LOG.warn("Fail to subsume the old checkpoint.", e);
			}
		}
	}

	@Override
	public CompletedCheckpoint getLatestCheckpoint() {
		return checkpoints.isEmpty() ? null : checkpoints.getLast();
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() {
		return new ArrayList<>(checkpoints);
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return checkpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxNumberOfCheckpointsToRetain;
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		try {
			LOG.info("Shutting down");

			for (CompletedCheckpoint checkpoint : checkpoints) {
				List<StateObject> discardedStates = checkpoint.unregister(stateRegistry);
				checkpoint.discard(jobStatus, discardedStates);
			}
		} finally {
			checkpoints.clear();
		}
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return false;
	}
}
