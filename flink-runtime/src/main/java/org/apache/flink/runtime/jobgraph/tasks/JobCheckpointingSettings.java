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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The JobCheckpointingSettings are attached to a JobGraph and describe the settings
 * for the asynchronous checkpoints of the JobGraph, such as interval, and which vertices
 * need to participate.
 */
public class JobCheckpointingSettings implements Serializable {

	private static final long serialVersionUID = -2593319571078198180L;

	private final List<JobVertexID> verticesToTrigger;

	private final List<JobVertexID> verticesToAcknowledge;

	private final List<JobVertexID> verticesToConfirm;

	/** Contains configuration settings for the CheckpointCoordinator */
	private final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration;

	/** The default state backend, if configured by the user in the job */
	@Nullable
	private final SerializedValue<StateBackend> defaultStateBackend;

	/** (Factories for) hooks that are executed on the checkpoint coordinator */
	@Nullable
	private final SerializedValue<MasterTriggerRestoreHook.Factory[]> masterHooks;

	public JobCheckpointingSettings(
			List<JobVertexID> verticesToTrigger,
			List<JobVertexID> verticesToAcknowledge,
			List<JobVertexID> verticesToConfirm,
			CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration,
			@Nullable SerializedValue<StateBackend> defaultStateBackend) {

		this(
			verticesToTrigger,
			verticesToAcknowledge,
			verticesToConfirm,
			checkpointCoordinatorConfiguration,
			defaultStateBackend,
			null);
	}

	public JobCheckpointingSettings(
			List<JobVertexID> verticesToTrigger,
			List<JobVertexID> verticesToAcknowledge,
			List<JobVertexID> verticesToConfirm,
			CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration,
			@Nullable SerializedValue<StateBackend> defaultStateBackend,
			@Nullable SerializedValue<MasterTriggerRestoreHook.Factory[]> masterHooks) {


		this.verticesToTrigger = requireNonNull(verticesToTrigger);
		this.verticesToAcknowledge = requireNonNull(verticesToAcknowledge);
		this.verticesToConfirm = requireNonNull(verticesToConfirm);
		this.checkpointCoordinatorConfiguration = Preconditions.checkNotNull(checkpointCoordinatorConfiguration);
		this.defaultStateBackend = defaultStateBackend;
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

	public CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration() {
		return checkpointCoordinatorConfiguration;
	}

	@Nullable
	public SerializedValue<StateBackend> getDefaultStateBackend() {
		return defaultStateBackend;
	}

	@Nullable
	public SerializedValue<MasterTriggerRestoreHook.Factory[]> getMasterHooks() {
		return masterHooks;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("SnapshotSettings: config=%s, trigger=%s, ack=%s, commit=%s",
			checkpointCoordinatorConfiguration,
			verticesToTrigger,
			verticesToAcknowledge,
			verticesToConfirm);
	}
}
