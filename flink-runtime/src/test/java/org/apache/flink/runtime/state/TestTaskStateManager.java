/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Implementation of {@link TaskStateManager} for tests. */
public class TestTaskStateManager implements TaskStateManager {

    private long reportedCheckpointId;
    private long notifiedCompletedCheckpointId;
    private long notifiedAbortedCheckpointId;

    private final JobID jobId;
    private final ExecutionAttemptID executionAttemptID;

    private final Map<Long, TaskStateSnapshot> jobManagerTaskStateSnapshotsByCheckpointId;
    private final Map<Long, TaskStateSnapshot> taskManagerTaskStateSnapshotsByCheckpointId;
    private final CheckpointResponder checkpointResponder;
    private final OneShotLatch waitForReportLatch;
    private final LocalRecoveryConfig localRecoveryDirectoryProvider;
    private final StateChangelogStorage<?> stateChangelogStorage;

    public TestTaskStateManager() {
        this(TestLocalRecoveryConfig.disabled());
    }

    public TestTaskStateManager(LocalRecoveryConfig localRecoveryConfig) {
        this(
                new JobID(),
                new ExecutionAttemptID(),
                new TestCheckpointResponder(),
                localRecoveryConfig,
                new InMemoryStateChangelogStorage(),
                new HashMap<>(),
                -1L,
                new OneShotLatch());
    }

    public TestTaskStateManager(
            JobID jobId,
            ExecutionAttemptID executionAttemptID,
            CheckpointResponder checkpointResponder,
            LocalRecoveryConfig localRecoveryConfig,
            StateChangelogStorage<?> changelogStorage,
            Map<Long, TaskStateSnapshot> jobManagerTaskStateSnapshotsByCheckpointId,
            long reportedCheckpointId,
            OneShotLatch waitForReportLatch) {
        this.jobId = checkNotNull(jobId);
        this.executionAttemptID = checkNotNull(executionAttemptID);
        this.checkpointResponder = checkNotNull(checkpointResponder);
        this.localRecoveryDirectoryProvider = checkNotNull(localRecoveryConfig);
        this.stateChangelogStorage = checkNotNull(changelogStorage);
        this.jobManagerTaskStateSnapshotsByCheckpointId =
                checkNotNull(jobManagerTaskStateSnapshotsByCheckpointId);
        this.taskManagerTaskStateSnapshotsByCheckpointId = new HashMap<>();
        this.reportedCheckpointId = reportedCheckpointId;
        this.notifiedCompletedCheckpointId = -1L;
        this.notifiedAbortedCheckpointId = -1L;
        this.waitForReportLatch = checkNotNull(waitForReportLatch);
    }

    @Override
    public void reportTaskStateSnapshots(
            @Nonnull CheckpointMetaData checkpointMetaData,
            @Nonnull CheckpointMetrics checkpointMetrics,
            @Nullable TaskStateSnapshot acknowledgedState,
            @Nullable TaskStateSnapshot localState) {

        jobManagerTaskStateSnapshotsByCheckpointId.put(
                checkpointMetaData.getCheckpointId(), acknowledgedState);

        taskManagerTaskStateSnapshotsByCheckpointId.put(
                checkpointMetaData.getCheckpointId(), localState);

        if (checkpointResponder != null) {
            checkpointResponder.acknowledgeCheckpoint(
                    jobId,
                    executionAttemptID,
                    checkpointMetaData.getCheckpointId(),
                    checkpointMetrics,
                    acknowledgedState);
        }

        this.reportedCheckpointId = checkpointMetaData.getCheckpointId();

        if (waitForReportLatch != null) {
            waitForReportLatch.trigger();
        }
    }

    @Override
    public InflightDataRescalingDescriptor getInputRescalingDescriptor() {
        return InflightDataRescalingDescriptor.NO_RESCALE;
    }

    @Override
    public InflightDataRescalingDescriptor getOutputRescalingDescriptor() {
        return InflightDataRescalingDescriptor.NO_RESCALE;
    }

    @Override
    public void reportIncompleteTaskStateSnapshots(
            CheckpointMetaData checkpointMetaData, CheckpointMetrics checkpointMetrics) {
        reportedCheckpointId = checkpointMetaData.getCheckpointId();
    }

    @Override
    public boolean isTaskDeployedAsFinished() {
        TaskStateSnapshot jmTaskStateSnapshot = getLastJobManagerTaskStateSnapshot();
        if (jmTaskStateSnapshot != null) {
            return jmTaskStateSnapshot.isTaskDeployedAsFinished();
        }

        return false;
    }

    @Override
    public Optional<Long> getRestoreCheckpointId() {
        TaskStateSnapshot jmTaskStateSnapshot = getLastJobManagerTaskStateSnapshot();

        if (jmTaskStateSnapshot == null) {
            return Optional.empty();
        }

        return Optional.of(reportedCheckpointId);
    }

    @Nonnull
    @Override
    public PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID) {
        TaskStateSnapshot jmTaskStateSnapshot = getLastJobManagerTaskStateSnapshot();
        TaskStateSnapshot tmTaskStateSnapshot = getLastTaskManagerTaskStateSnapshot();

        if (jmTaskStateSnapshot == null) {

            return PrioritizedOperatorSubtaskState.emptyNotRestored();
        } else {

            OperatorSubtaskState jmOpState =
                    jmTaskStateSnapshot.getSubtaskStateByOperatorID(operatorID);

            if (jmOpState == null) {

                return PrioritizedOperatorSubtaskState.emptyNotRestored();
            } else {

                List<OperatorSubtaskState> tmStateCollection = Collections.emptyList();

                if (tmTaskStateSnapshot != null) {
                    OperatorSubtaskState tmOpState =
                            tmTaskStateSnapshot.getSubtaskStateByOperatorID(operatorID);
                    if (tmOpState != null) {
                        tmStateCollection = Collections.singletonList(tmOpState);
                    }
                }
                PrioritizedOperatorSubtaskState.Builder builder =
                        new PrioritizedOperatorSubtaskState.Builder(
                                jmOpState, tmStateCollection, reportedCheckpointId);
                return builder.build();
            }
        }
    }

    @Nonnull
    @Override
    public LocalRecoveryConfig createLocalRecoveryConfig() {
        return checkNotNull(
                localRecoveryDirectoryProvider,
                "Local state directory was never set for this test object!");
    }

    @Override
    public SequentialChannelStateReader getSequentialChannelStateReader() {
        return SequentialChannelStateReader.NO_OP;
    }

    @Nullable
    @Override
    public StateChangelogStorage<?> getStateChangelogStorage() {
        return stateChangelogStorage;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        this.notifiedCompletedCheckpointId = checkpointId;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        this.notifiedAbortedCheckpointId = checkpointId;
    }

    public JobID getJobId() {
        return jobId;
    }

    public ExecutionAttemptID getExecutionAttemptID() {
        return executionAttemptID;
    }

    public CheckpointResponder getCheckpointResponder() {
        return checkpointResponder;
    }

    public Map<Long, TaskStateSnapshot> getJobManagerTaskStateSnapshotsByCheckpointId() {
        return jobManagerTaskStateSnapshotsByCheckpointId;
    }

    public void setJobManagerTaskStateSnapshotsByCheckpointId(
            Map<Long, TaskStateSnapshot> jobManagerTaskStateSnapshotsByCheckpointId) {
        this.jobManagerTaskStateSnapshotsByCheckpointId.clear();
        this.jobManagerTaskStateSnapshotsByCheckpointId.putAll(
                jobManagerTaskStateSnapshotsByCheckpointId);
    }

    public Map<Long, TaskStateSnapshot> getTaskManagerTaskStateSnapshotsByCheckpointId() {
        return taskManagerTaskStateSnapshotsByCheckpointId;
    }

    public void setTaskManagerTaskStateSnapshotsByCheckpointId(
            Map<Long, TaskStateSnapshot> taskManagerTaskStateSnapshotsByCheckpointId) {
        this.taskManagerTaskStateSnapshotsByCheckpointId.clear();
        this.taskManagerTaskStateSnapshotsByCheckpointId.putAll(
                taskManagerTaskStateSnapshotsByCheckpointId);
    }

    public long getReportedCheckpointId() {
        return reportedCheckpointId;
    }

    public long getNotifiedCompletedCheckpointId() {
        return notifiedCompletedCheckpointId;
    }

    public long getNotifiedAbortedCheckpointId() {
        return notifiedAbortedCheckpointId;
    }

    public void setReportedCheckpointId(long reportedCheckpointId) {
        this.reportedCheckpointId = reportedCheckpointId;
    }

    public TaskStateSnapshot getLastJobManagerTaskStateSnapshot() {
        return jobManagerTaskStateSnapshotsByCheckpointId != null
                ? jobManagerTaskStateSnapshotsByCheckpointId.get(reportedCheckpointId)
                : null;
    }

    public TaskStateSnapshot getLastTaskManagerTaskStateSnapshot() {
        return taskManagerTaskStateSnapshotsByCheckpointId != null
                ? taskManagerTaskStateSnapshotsByCheckpointId.get(reportedCheckpointId)
                : null;
    }

    public OneShotLatch getWaitForReportLatch() {
        return waitForReportLatch;
    }

    public void restoreLatestCheckpointState(
            Map<Long, TaskStateSnapshot> taskStateSnapshotsByCheckpointId) {

        if (taskStateSnapshotsByCheckpointId == null
                || taskStateSnapshotsByCheckpointId.isEmpty()) {
            return;
        }

        long latestId = -1;

        for (long id : taskStateSnapshotsByCheckpointId.keySet()) {
            if (id > latestId) {
                latestId = id;
            }
        }

        setReportedCheckpointId(latestId);
        setJobManagerTaskStateSnapshotsByCheckpointId(taskStateSnapshotsByCheckpointId);
    }

    @Override
    public void close() throws Exception {}

    public static TestTaskStateManagerBuilder builder() {
        return new TestTaskStateManagerBuilder();
    }
}
