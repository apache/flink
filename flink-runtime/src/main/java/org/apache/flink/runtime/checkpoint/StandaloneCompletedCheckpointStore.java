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

import org.apache.flink.runtime.jobmanager.RecoveryMode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link RecoveryMode#STANDALONE}.
 */
class StandaloneCompletedCheckpointStore implements CompletedCheckpointStore {

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/** User class loader for discarding {@link CompletedCheckpoint} instances. */
	private final ClassLoader userClassLoader;

	/** The completed checkpoints. */
	private final ArrayDeque<CompletedCheckpoint> checkpoints;

	/**
	 * Creates {@link StandaloneCompletedCheckpointStore}.
	 *
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
	 *                                       least 1). Adding more checkpoints than this results
	 *                                       in older checkpoints being discarded.
	 * @param userClassLoader                The user class loader used to discard checkpoints
	 */
	public StandaloneCompletedCheckpointStore(
			int maxNumberOfCheckpointsToRetain,
			ClassLoader userClassLoader) {

		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");

		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
		this.userClassLoader = checkNotNull(userClassLoader, "User class loader");

		this.checkpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);
	}

	@Override
	public void recover() throws Exception {
		// Nothing to do
	}

	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) {
		checkpoints.addLast(checkpoint);
		if (checkpoints.size() > maxNumberOfCheckpointsToRetain) {
			checkpoints.removeFirst().discard(userClassLoader);
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
	public void discardAllCheckpoints() {
		for (CompletedCheckpoint checkpoint : checkpoints) {
			checkpoint.discard(userClassLoader);
		}

		checkpoints.clear();
	}
}
