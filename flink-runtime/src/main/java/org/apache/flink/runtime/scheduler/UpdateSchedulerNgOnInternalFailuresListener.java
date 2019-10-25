/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Calls {@link SchedulerNG#updateTaskExecutionState(TaskExecutionState)} on task failure.
 * Calls {@link SchedulerNG#handleGlobalFailure(Throwable)} on global failures.
 */
class UpdateSchedulerNgOnInternalFailuresListener implements InternalFailuresListener {

	private final SchedulerNG schedulerNg;

	private final JobID jobId;

	public UpdateSchedulerNgOnInternalFailuresListener(
		final SchedulerNG schedulerNg,
		final JobID jobId) {

		this.schedulerNg = checkNotNull(schedulerNg);
		this.jobId = checkNotNull(jobId);
	}

	@Override
	public void notifyTaskFailure(final ExecutionAttemptID attemptId, final Throwable t) {
		schedulerNg.updateTaskExecutionState(new TaskExecutionState(
			jobId,
			attemptId,
			ExecutionState.FAILED,
			t));
	}

	@Override
	public void notifyGlobalFailure(Throwable t) {
		schedulerNg.handleGlobalFailure(t);
	}
}
