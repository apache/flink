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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link TaskStateManager} for tests.
 */
public class TestTaskStateManager implements TaskStateManager {

	private long reportedCheckpointId;

	private JobID jobId;
	private ExecutionAttemptID executionAttemptID;

	private final Map<Long, TaskStateSnapshot> taskStateSnapshotsByCheckpointId;
	private CheckpointResponder checkpointResponder;
	private OneShotLatch waitForReportLatch;

	public TestTaskStateManager() {
		this(new JobID(), new ExecutionAttemptID(), new TestCheckpointResponder());
	}

	public TestTaskStateManager(
		JobID jobId,
		ExecutionAttemptID executionAttemptID) {

		this(jobId, executionAttemptID, null);
	}

	public TestTaskStateManager(
		JobID jobId,
		ExecutionAttemptID executionAttemptID,
		CheckpointResponder checkpointResponder) {
		this.jobId = jobId;
		this.executionAttemptID = executionAttemptID;
		this.checkpointResponder = checkpointResponder;
		this.taskStateSnapshotsByCheckpointId = new HashMap<>();
		this.reportedCheckpointId = -1L;
	}

	@Override
	public void reportStateHandles(
		@Nonnull CheckpointMetaData checkpointMetaData,
		@Nonnull CheckpointMetrics checkpointMetrics,
		@Nullable TaskStateSnapshot acknowledgedState) {

		if (taskStateSnapshotsByCheckpointId != null) {
			taskStateSnapshotsByCheckpointId.put(
				checkpointMetaData.getCheckpointId(),
				acknowledgedState);
		}

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
	public OperatorSubtaskState operatorStates(OperatorID operatorID) {
		TaskStateSnapshot taskStateSnapshot = getLastTaskStateSnapshot();
		return taskStateSnapshot != null ? taskStateSnapshot.getSubtaskStateByOperatorID(operatorID) : null;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {

	}

	public JobID getJobId() {
		return jobId;
	}

	public void setJobId(JobID jobId) {
		this.jobId = jobId;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public void setExecutionAttemptID(ExecutionAttemptID executionAttemptID) {
		this.executionAttemptID = executionAttemptID;
	}

	public CheckpointResponder getCheckpointResponder() {
		return checkpointResponder;
	}

	public void setCheckpointResponder(CheckpointResponder checkpointResponder) {
		this.checkpointResponder = checkpointResponder;
	}

	public Map<Long, TaskStateSnapshot> getTaskStateSnapshotsByCheckpointId() {
		return taskStateSnapshotsByCheckpointId;
	}

	public void setTaskStateSnapshotsByCheckpointId(Map<Long, TaskStateSnapshot> taskStateSnapshotsByCheckpointId) {
		this.taskStateSnapshotsByCheckpointId.clear();
		this.taskStateSnapshotsByCheckpointId.putAll(taskStateSnapshotsByCheckpointId);
	}

	public long getReportedCheckpointId() {
		return reportedCheckpointId;
	}

	public void setReportedCheckpointId(long reportedCheckpointId) {
		this.reportedCheckpointId = reportedCheckpointId;
	}

	public TaskStateSnapshot getLastTaskStateSnapshot() {
		return taskStateSnapshotsByCheckpointId != null ?
			taskStateSnapshotsByCheckpointId.get(reportedCheckpointId)
			: null;
	}

	public OneShotLatch getWaitForReportLatch() {
		return waitForReportLatch;
	}

	public void setWaitForReportLatch(OneShotLatch waitForReportLatch) {
		this.waitForReportLatch = waitForReportLatch;
	}

	public void restoreLatestCheckpointState(Map<Long, TaskStateSnapshot> taskStateSnapshotsByCheckpointId) {

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
		setTaskStateSnapshotsByCheckpointId(taskStateSnapshotsByCheckpointId);
	}
}
