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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.job.SubtaskAllExecutionAttemptsDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Tests for (un)marshalling of {@link SubtaskAllExecutionAttemptsDetailsInfo}.
 */
public class SubtaskAllExecutionAttemptsDetailsInfoTest extends RestResponseMarshallingTestBase<SubtaskAllExecutionAttemptsDetailsInfo> {

	@Override
	protected Class<SubtaskAllExecutionAttemptsDetailsInfo> getTestResponseClass() {
		return SubtaskAllExecutionAttemptsDetailsInfo.class;
	}

	@Override
	protected SubtaskAllExecutionAttemptsDetailsInfo getTestResponseInstance() throws Exception {
		final Random random = new Random();
		final IOMetricsInfo jobVertexMetrics = new IOMetricsInfo(
			random.nextLong(),
			random.nextBoolean(),
			random.nextLong(),
			random.nextBoolean(),
			random.nextLong(),
			random.nextBoolean(),
			random.nextLong(),
			random.nextBoolean());
		final int subtaskIndex = 1;
		String host = "local1";
		long duration = 1024L;
		String taskmanagerId = "taskmanagerId1";
		List<SubtaskExecutionAttemptDetailsInfo> subtaskExecutionAttemptDetailsInfoList = new ArrayList<>();
		subtaskExecutionAttemptDetailsInfoList.add(new SubtaskExecutionAttemptDetailsInfo(
			subtaskIndex,
			ExecutionState.FAILED,
			0,
			host,
			System.currentTimeMillis() - duration * 2,
			System.currentTimeMillis() - duration,
			duration,
			jobVertexMetrics,
			taskmanagerId));
		subtaskExecutionAttemptDetailsInfoList.add(new SubtaskExecutionAttemptDetailsInfo(
			subtaskIndex,
			ExecutionState.RUNNING,
			1,
			host,
			System.currentTimeMillis(),
			-1L,
			-1L,
			jobVertexMetrics,
			taskmanagerId));
		return new SubtaskAllExecutionAttemptsDetailsInfo(subtaskExecutionAttemptDetailsInfoList);
	}
}
