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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.runtime.rest.messages.JobVertexTaskManagersInfo.TaskManagersInfo;

/**
 * Tests that the {@link JobVertexTaskManagersInfo} can be marshalled and unmarshalled.
 */
public class JobVertexTaskManagersInfoTest extends RestResponseMarshallingTestBase<JobVertexTaskManagersInfo> {
	@Override
	protected Class<JobVertexTaskManagersInfo> getTestResponseClass() {
		return JobVertexTaskManagersInfo.class;
	}

	@Override
	protected JobVertexTaskManagersInfo getTestResponseInstance() throws Exception {
		final Random random = new Random();
		List<TaskManagersInfo> taskManagersInfoList = new ArrayList<>();

		final Map<ExecutionState, Integer> statusCounts = new HashMap<>(ExecutionState.values().length);
		final IOMetricsInfo jobVertexMetrics = new IOMetricsInfo(
			random.nextLong(),
			random.nextBoolean(),
			random.nextLong(),
			random.nextBoolean(),
			random.nextLong(),
			random.nextBoolean(),
			random.nextLong(),
			random.nextBoolean());
		int count = 100;
		for (ExecutionState executionState : ExecutionState.values()) {
			statusCounts.put(executionState, count++);
		}
		taskManagersInfoList.add(new TaskManagersInfo("host1", ExecutionState.CANCELING, 1L, 2L, 3L, jobVertexMetrics, statusCounts));

		return new JobVertexTaskManagersInfo(new JobVertexID(), "test", System.currentTimeMillis(), taskManagersInfoList);
	}
}
