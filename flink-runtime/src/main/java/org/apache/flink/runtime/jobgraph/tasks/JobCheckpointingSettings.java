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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The JobCheckpointingSettings are attached to a JobGraph and describe the settings
 * for the asynchronous checkpoints of the JobGraph, such as interval, and which vertices
 * need to participate.
 */
public class JobCheckpointingSettings implements java.io.Serializable {

	private static final long serialVersionUID = -2593319571078198180L;

	private final List<JobVertexID> verticesToTrigger;

	private final List<JobVertexID> verticesToAcknowledge;

	private final List<JobVertexID> verticesToConfirm;

	private final long checkpointInterval;

	private final long checkpointTimeout;

	private final long minPauseBetweenCheckpoints;

	private final int maxConcurrentCheckpoints;

	/** Settings for externalized checkpoints. */
	private final ExternalizedCheckpointSettings externalizedCheckpointSettings;

	/** The default state backend, if configured by the user in the job */
	@Nullable
	private final SerializedValue<StateBackend> defaultStateBackend;

	/** (Factories for) hooks that are executed on the checkpoint coordinator */
	@Nullable
	private final SerializedValue<MasterTriggerRestoreHook.Factory[]> masterHooks;

	/**
	 * Flag indicating whether exactly once checkpoint mode has been configured.
	 * If <code>false</code>, at least once mode has been configured. This is
	 * not a necessary attribute, because the checkpointing mode is only relevant
	 * for the stream tasks, but we expose it here to forward it to the web runtime
	 * UI.
	 */
	private final boolean isExactlyOnce;

	public JobCheckpointingSettings(
			List<JobVertexID> verticesToTrigger,
			List<JobVertexID> verticesToAcknowledge,
			List<JobVertexID> verticesToConfirm,
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			ExternalizedCheckpointSettings externalizedCheckpointSettings,
			@Nullable SerializedValue<StateBackend> defaultStateBackend,
			boolean isExactlyOnce) {

		this(verticesToTrigger, verticesToAcknowledge, verticesToConfirm,
				checkpointInterval, checkpointTimeout, minPauseBetweenCheckpoints, maxConcurrentCheckpoints,
				externalizedCheckpointSettings, defaultStateBackend, null, isExactlyOnce);
	}

	public JobCheckpointingSettings(
			List<JobVertexID> verticesToTrigger,
			List<JobVertexID> verticesToAcknowledge,
			List<JobVertexID> verticesToConfirm,
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			ExternalizedCheckpointSettings externalizedCheckpointSettings,
			@Nullable SerializedValue<StateBackend> defaultStateBackend,
			@Nullable SerializedValue<MasterTriggerRestoreHook.Factory[]> masterHooks,
			boolean isExactlyOnce) {

		// sanity checks
		if (checkpointInterval < 1 || checkpointTimeout < 1 ||
				minPauseBetweenCheckpoints < 0 || maxConcurrentCheckpoints < 1) {
			throw new IllegalArgumentException();
		}

		this.verticesToTrigger = requireNonNull(verticesToTrigger);
		this.verticesToAcknowledge = requireNonNull(verticesToAcknowledge);
		this.verticesToConfirm = requireNonNull(verticesToConfirm);
		this.checkpointInterval = checkpointInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
		this.externalizedCheckpointSettings = requireNonNull(externalizedCheckpointSettings);
		this.defaultStateBackend = defaultStateBackend;
		this.isExactlyOnce = isExactlyOnce;
		this.masterHooks = masterHooks;
	}

	// --------------------------------------------------------------------------------------------

	public List<JobVertexID> getVerticesToTrigger() {
		return verticesToTrigger;
	}

	public List<JobVertexID> getVerticesToAcknowledge() {
		return verticesToAcknowledge;
	}

	public List<JobVertexID> getVerticesToConfirm() {
		return verticesToConfirm;
	}

	public long getCheckpointInterval() {
		return checkpointInterval;
	}

	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	public long getMinPauseBetweenCheckpoints() {
		return minPauseBetweenCheckpoints;
	}

	public int getMaxConcurrentCheckpoints() {
		return maxConcurrentCheckpoints;
	}

	public ExternalizedCheckpointSettings getExternalizedCheckpointSettings() {
		return externalizedCheckpointSettings;
	}

	@Nullable
	public SerializedValue<StateBackend> getDefaultStateBackend() {
		return defaultStateBackend;
	}

	@Nullable
	public SerializedValue<MasterTriggerRestoreHook.Factory[]> getMasterHooks() {
		return masterHooks;
	}

	public boolean isExactlyOnce() {
		return isExactlyOnce;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("SnapshotSettings: interval=%d, timeout=%d, pause-between=%d, " +
						"maxConcurrent=%d, trigger=%s, ack=%s, commit=%s",
						checkpointInterval, checkpointTimeout,
						minPauseBetweenCheckpoints, maxConcurrentCheckpoints,
						verticesToTrigger, verticesToAcknowledge, verticesToConfirm);
	}
}
