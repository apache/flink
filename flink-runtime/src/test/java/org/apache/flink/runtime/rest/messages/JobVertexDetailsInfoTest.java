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
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.SubTaskIOMetricsInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Tests that the {@link JobVertexDetailsInfo} can be marshalled and unmarshalled.
 */
public class JobVertexDetailsInfoTest extends RestResponseMarshallingTestBase<JobVertexDetailsInfo> {
	@Override
	protected Class<JobVertexDetailsInfo> getTestResponseClass() {
		return JobVertexDetailsInfo.class;
	}

	@Override
	protected JobVertexDetailsInfo getTestResponseInstance() throws Exception {
		final Random random = new Random();
		final SubTaskIOMetricsInfo subTaskIOMetricsInfo = new SubTaskIOMetricsInfo(
			Math.abs(random.nextLong()),
			random.nextBoolean(),
			Math.abs(random.nextLong()),
			random.nextBoolean(),
			Math.abs(random.nextLong()),
			random.nextBoolean(),
			Math.abs(random.nextLong()),
			random.nextBoolean(),
			Math.abs(random.nextFloat()), random.nextBoolean(), Math.abs(random.nextFloat()), random.nextBoolean(), Math.abs(random.nextFloat()), random.nextBoolean(), random.nextBoolean(),
			random.nextBoolean()
		);
		List<SubtaskExecutionAttemptDetailsInfo> vertexTaskDetailList = new ArrayList<>();
		vertexTaskDetailList.add(new SubtaskExecutionAttemptDetailsInfo(
			0,
			ExecutionState.CREATED,
			random.nextInt(),
			"local1",
			System.currentTimeMillis(),
			System.currentTimeMillis(),
			1L,
			subTaskIOMetricsInfo,
			"taskmanagerId1"));
		vertexTaskDetailList.add(new SubtaskExecutionAttemptDetailsInfo(
			1,
			ExecutionState.FAILED,
			random.nextInt(),
			"local2",
			System.currentTimeMillis(),
			System.currentTimeMillis(),
			1L,
			subTaskIOMetricsInfo,
			"taskmanagerId2"));
		vertexTaskDetailList.add(new SubtaskExecutionAttemptDetailsInfo(
			2,
			ExecutionState.FINISHED,
			random.nextInt(),
			"local3",
			System.currentTimeMillis(),
			System.currentTimeMillis(),
			1L,
			subTaskIOMetricsInfo,
			"taskmanagerId3"));

		return new JobVertexDetailsInfo(
			new JobVertexID(),
			"jobVertex" + random.nextLong(),
			random.nextInt(),
			System.currentTimeMillis(),
			vertexTaskDetailList);
	}
}
