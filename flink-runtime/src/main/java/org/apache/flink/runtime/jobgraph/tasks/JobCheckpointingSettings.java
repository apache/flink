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
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * The JobCheckpointingSettings are attached to a JobGraph and describe the settings for the
 * asynchronous checkpoints of the JobGraph, such as interval.
 */
public class JobCheckpointingSettings implements Serializable {

    private static final long serialVersionUID = -2593319571078198180L;

    /** Contains configuration settings for the CheckpointCoordinator */
    private final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration;

    /** The default state backend, if configured by the user in the job */
    @Nullable private final SerializedValue<StateBackend> defaultStateBackend;

    /** The default checkpoint storage, if configured by the user in the job */
    @Nullable private final SerializedValue<CheckpointStorage> defaultCheckpointStorage;

    /** (Factories for) hooks that are executed on the checkpoint coordinator */
    @Nullable private final SerializedValue<MasterTriggerRestoreHook.Factory[]> masterHooks;

    public JobCheckpointingSettings(
            CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration,
            @Nullable SerializedValue<StateBackend> defaultStateBackend) {

        this(checkpointCoordinatorConfiguration, defaultStateBackend, null, null);
    }

    public JobCheckpointingSettings(
            CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration,
            @Nullable SerializedValue<StateBackend> defaultStateBackend,
            @Nullable SerializedValue<CheckpointStorage> defaultCheckpointStorage,
            @Nullable SerializedValue<MasterTriggerRestoreHook.Factory[]> masterHooks) {

        this.checkpointCoordinatorConfiguration =
                Preconditions.checkNotNull(checkpointCoordinatorConfiguration);
        this.defaultStateBackend = defaultStateBackend;
        this.defaultCheckpointStorage = defaultCheckpointStorage;
        this.masterHooks = masterHooks;
    }

    // --------------------------------------------------------------------------------------------

    public CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration() {
        return checkpointCoordinatorConfiguration;
    }

    @Nullable
    public SerializedValue<StateBackend> getDefaultStateBackend() {
        return defaultStateBackend;
    }

    @Nullable
    public SerializedValue<CheckpointStorage> getDefaultCheckpointStorage() {
        return defaultCheckpointStorage;
    }

    @Nullable
    public SerializedValue<MasterTriggerRestoreHook.Factory[]> getMasterHooks() {
        return masterHooks;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("SnapshotSettings: config=%s", checkpointCoordinatorConfiguration);
    }
}
