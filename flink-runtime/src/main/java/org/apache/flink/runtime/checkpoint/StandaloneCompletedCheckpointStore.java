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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link HighAvailabilityMode#NONE}.
 */
@ThreadSafe
public class StandaloneCompletedCheckpointStore implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(StandaloneCompletedCheckpointStore.class);

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/** The completed checkpoints. */
	private final ArrayDeque<CompletedCheckpoint> checkpoints;

	private boolean shutdown = false;

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
	}

	@Override
	public void recover() throws Exception {
		// Nothing to do
	}

	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {

		synchronized (checkpoints) {
			checkState(!shutdown, "StandaloneCompletedCheckpointStore has been shut down");

			checkpoints.addLast(checkpoint);

			if (checkpoints.size() > maxNumberOfCheckpointsToRetain) {
				try {
					CompletedCheckpoint checkpointToSubsume = checkpoints.removeFirst();
					checkpointToSubsume.discardOnSubsume();
				} catch (Exception e) {
					LOG.warn("Fail to subsume the old checkpoint.", e);
				}
			}
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() {
		synchronized (checkpoints) {
			return new ArrayList<>(checkpoints);
		}
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		synchronized (checkpoints) {
			return checkpoints.size();
		}
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxNumberOfCheckpointsToRetain;
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		synchronized (checkpoints) {
			if (!shutdown) {
				shutdown = true;
				try {
					LOG.info("Shutting down");

					for (CompletedCheckpoint checkpoint : checkpoints) {
						checkpoint.discardOnShutdown(jobStatus);
					}
				} finally {
					checkpoints.clear();
				}
			}
		}
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return false;
	}
}
