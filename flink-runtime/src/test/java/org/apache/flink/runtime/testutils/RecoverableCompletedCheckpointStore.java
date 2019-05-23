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

package org.apache.flink.runtime.testutils;

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * A checkpoint store, which supports shutdown and suspend. You can use this to test HA
 * as long as the factory always returns the same store instance.
 */
public class RecoverableCompletedCheckpointStore implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(RecoverableCompletedCheckpointStore.class);

	private final ArrayDeque<CompletedCheckpoint> checkpoints = new ArrayDeque<>(2);

	private final ArrayDeque<CompletedCheckpoint> suspended = new ArrayDeque<>(2);

	private final int maxRetainedCheckpoints;

	public RecoverableCompletedCheckpointStore() {
		this(1);
	}

	public RecoverableCompletedCheckpointStore(int maxRetainedCheckpoints) {
		Preconditions.checkArgument(maxRetainedCheckpoints > 0);
		this.maxRetainedCheckpoints = maxRetainedCheckpoints;
	}

	@Override
	public void recover() throws Exception {
		checkpoints.addAll(suspended);
		suspended.clear();
	}

	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {

		checkpoints.addLast(checkpoint);

		if (checkpoints.size() > maxRetainedCheckpoints) {
			removeOldestCheckpoint();
		}
	}

	public void removeOldestCheckpoint() throws Exception {
		CompletedCheckpoint checkpointToSubsume = checkpoints.removeFirst();
		checkpointToSubsume.discardOnSubsume();
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			checkpoints.clear();
			suspended.clear();
		} else {
			suspended.clear();

			for (CompletedCheckpoint checkpoint : checkpoints) {
				suspended.add(checkpoint);
			}

			checkpoints.clear();
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		return new ArrayList<>(checkpoints);
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return checkpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxRetainedCheckpoints;
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return false;
	}
}
