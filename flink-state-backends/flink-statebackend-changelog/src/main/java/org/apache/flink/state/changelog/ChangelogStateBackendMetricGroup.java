/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

/**
 * Metrics related to the Changelog State Backend.
 *
 * <p>Note: This class is not thread-safe and should be just used in main thread of task.
 */
@NotThreadSafe
class ChangelogStateBackendMetricGroup extends ProxyMetricGroup<MetricGroup> {

    private static final String PREFIX = "ChangelogStateBackend";

    @VisibleForTesting
    static final String LATEST_FULL_SIZE_OF_MATERIALIZATION =
            PREFIX + ".lastFullSizeOfMaterialization";

    @VisibleForTesting
    static final String LATEST_INC_SIZE_OF_MATERIALIZATION =
            PREFIX + ".lastIncSizeOfMaterialization";

    @VisibleForTesting
    static final String LATEST_FULL_SIZE_OF_NON_MATERIALIZATION =
            PREFIX + ".lastFullSizeOfNonMaterialization";

    @VisibleForTesting
    static final String LATEST_INC_SIZE_OF_NON_MATERIALIZATION =
            PREFIX + ".lastIncSizeOfNonMaterialization";

    private long lastFullSizeOfMaterialization;
    private long lastIncSizeOfMaterialization;
    private long lastFullSizeOfNonMaterialization;
    private long lastIncSizeOfNonMaterialization;

    ChangelogStateBackendMetricGroup(MetricGroup parentMetricGroup) {
        super(parentMetricGroup);
        gauge(LATEST_FULL_SIZE_OF_MATERIALIZATION, () -> lastFullSizeOfMaterialization);
        gauge(LATEST_INC_SIZE_OF_MATERIALIZATION, () -> lastIncSizeOfMaterialization);
        gauge(LATEST_FULL_SIZE_OF_NON_MATERIALIZATION, () -> lastFullSizeOfNonMaterialization);
        gauge(LATEST_INC_SIZE_OF_NON_MATERIALIZATION, () -> lastIncSizeOfNonMaterialization);
        setSize(0, 0, 0, 0);
    }

    synchronized void reportSnapshotResult(
            SnapshotResult<ChangelogStateBackendHandle> snapshotResult) {
        ChangelogStateBackendHandle changelogStateBackendHandle =
                snapshotResult.getJobManagerOwnedSnapshot();
        if (changelogStateBackendHandle == null) {
            setSize(0, 0, 0, 0);
        } else {
            setSize(
                    sumStateSize(changelogStateBackendHandle.getMaterializedStateHandles()),
                    sumCheckpointSize(changelogStateBackendHandle.getMaterializedStateHandles()),
                    sumStateSize(changelogStateBackendHandle.getNonMaterializedStateHandles()),
                    sumCheckpointSize(
                            changelogStateBackendHandle.getNonMaterializedStateHandles()));
        }
    }

    private long sumStateSize(List<? extends KeyedStateHandle> keyedStateHandles) {
        return keyedStateHandles.stream().mapToLong(KeyedStateHandle::getStateSize).sum();
    }

    private long sumCheckpointSize(List<? extends KeyedStateHandle> keyedStateHandles) {
        return keyedStateHandles.stream().mapToLong(KeyedStateHandle::getCheckpointedSize).sum();
    }

    private void setSize(
            long lastFullSizeOfMaterialization,
            long lastIncSizeOfMaterialization,
            long lastFullSizeOfNonMaterialization,
            long lastIncSizeOfNonMaterialization) {
        this.lastFullSizeOfMaterialization = lastFullSizeOfMaterialization;
        this.lastIncSizeOfMaterialization = lastIncSizeOfMaterialization;
        this.lastFullSizeOfNonMaterialization = lastFullSizeOfNonMaterialization;
        this.lastIncSizeOfNonMaterialization = lastIncSizeOfNonMaterialization;
    }
}
