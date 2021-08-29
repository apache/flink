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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link TestTaskStateManager} builder. */
public class TestTaskStateManagerBuilder {

    private JobID jobID = new JobID();
    private ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();
    private CheckpointResponder checkpointResponder = new TestCheckpointResponder();
    private LocalRecoveryConfig localRecoveryConfig = TestLocalRecoveryConfig.disabled();
    private StateChangelogStorage<?> stateChangelogStorage = new InMemoryStateChangelogStorage();
    private final Map<Long, TaskStateSnapshot> jobManagerTaskStateSnapshotsByCheckpointId =
            new HashMap<>();
    private long reportedCheckpointId = -1L;
    private OneShotLatch waitForReportLatch = new OneShotLatch();

    public TestTaskStateManagerBuilder setJobID(JobID jobID) {
        this.jobID = checkNotNull(jobID);
        return this;
    }

    public TestTaskStateManagerBuilder setExecutionAttemptID(
            ExecutionAttemptID executionAttemptID) {
        this.executionAttemptID = checkNotNull(executionAttemptID);
        return this;
    }

    public TestTaskStateManagerBuilder setCheckpointResponder(
            CheckpointResponder checkpointResponder) {
        this.checkpointResponder = checkNotNull(checkpointResponder);
        return this;
    }

    public TestTaskStateManagerBuilder setLocalRecoveryConfig(
            LocalRecoveryConfig localRecoveryConfig) {
        this.localRecoveryConfig = checkNotNull(localRecoveryConfig);
        return this;
    }

    public TestTaskStateManagerBuilder setStateChangelogStorage(
            StateChangelogStorage<?> stateChangelogStorage) {
        Preconditions.checkState(
                this.stateChangelogStorage == null
                        || this.stateChangelogStorage instanceof InMemoryStateChangelogStorage,
                "StateChangelogStorage was already initialized to " + this.stateChangelogStorage);
        this.stateChangelogStorage = checkNotNull(stateChangelogStorage);
        return this;
    }

    public TestTaskStateManagerBuilder setJobManagerTaskStateSnapshotsByCheckpointId(
            Map<Long, TaskStateSnapshot> jobManagerTaskStateSnapshotsByCheckpointId) {
        this.jobManagerTaskStateSnapshotsByCheckpointId.clear();
        this.jobManagerTaskStateSnapshotsByCheckpointId.putAll(
                jobManagerTaskStateSnapshotsByCheckpointId);
        return this;
    }

    public TestTaskStateManagerBuilder setReportedCheckpointId(long reportedCheckpointId) {
        this.reportedCheckpointId = reportedCheckpointId;
        return this;
    }

    public TestTaskStateManagerBuilder setWaitForReportLatch(OneShotLatch waitForReportLatch) {
        this.waitForReportLatch = checkNotNull(waitForReportLatch);
        return this;
    }

    public TestTaskStateManager build() {
        return new TestTaskStateManager(
                jobID,
                executionAttemptID,
                checkpointResponder,
                localRecoveryConfig,
                stateChangelogStorage,
                jobManagerTaskStateSnapshotsByCheckpointId,
                reportedCheckpointId,
                waitForReportLatch);
    }
}
