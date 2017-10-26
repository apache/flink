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

package org.apache.flink.runtime.rest.handler.legacy.messages;

import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link CheckpointingStatistics}.
 */
public class CheckpointingStatisticsTest extends RestResponseMarshallingTestBase<CheckpointingStatistics> {
	@Override
	protected Class<CheckpointingStatistics> getTestResponseClass() {
		return CheckpointingStatistics.class;
	}

	@Override
	protected CheckpointingStatistics getTestResponseInstance() throws Exception {

		final CheckpointingStatistics.Counts counts = new CheckpointingStatistics.Counts(1, 2, 3, 4, 5);
		final CheckpointingStatistics.Summary summary = new CheckpointingStatistics.Summary(
			new CheckpointingStatistics.MinMaxAvgStatistics(1L, 1L, 1L),
			new CheckpointingStatistics.MinMaxAvgStatistics(2L, 2L, 2L),
			new CheckpointingStatistics.MinMaxAvgStatistics(3L, 3L, 3L));

		final Map<JobVertexID, CheckpointStatistics.TaskCheckpointStatistics> checkpointStatisticsPerTask = new HashMap<>(2);

		checkpointStatisticsPerTask.put(
			new JobVertexID(),
			new CheckpointStatistics.TaskCheckpointStatistics(
				1L,
				2L,
				3L,
				4L,
				5,
				6));

		checkpointStatisticsPerTask.put(
			new JobVertexID(),
			new CheckpointStatistics.TaskCheckpointStatistics(
				2L,
				3L,
				4L,
				5L,
				6,
				7));

		final CheckpointStatistics.CompletedCheckpointStatistics completed = new CheckpointStatistics.CompletedCheckpointStatistics(
			1L,
			CheckpointStatsStatus.COMPLETED,
			false,
			42L,
			41L,
			1337L,
			1L,
			0L,
			10,
			10,
			Collections.emptyMap(),
			null,
			false);

		final CheckpointStatistics.CompletedCheckpointStatistics savepoint = new CheckpointStatistics.CompletedCheckpointStatistics(
			2L,
			CheckpointStatsStatus.COMPLETED,
			true,
			11L,
			10L,
			43L,
			1L,
			0L,
			9,
			9,
			checkpointStatisticsPerTask,
			"externalPath",
			false);

		final CheckpointStatistics.FailedCheckpointStatistics failed = new CheckpointStatistics.FailedCheckpointStatistics(
			3L,
			CheckpointStatsStatus.FAILED,
			false,
			5L,
			10L,
			4L,
			2L,
			0L,
			11,
			9,
			Collections.emptyMap(),
			100L,
			"Test failure");

		CheckpointingStatistics.RestoredCheckpointStatistics restored = new CheckpointingStatistics.RestoredCheckpointStatistics(
			4L,
			1445L,
			true,
			"foobar");

		final CheckpointingStatistics.LatestCheckpoints latestCheckpoints = new CheckpointingStatistics.LatestCheckpoints(
			completed,
			savepoint,
			failed,
			restored);

		return new CheckpointingStatistics(
			counts,
			summary,
			latestCheckpoints,
			Arrays.asList(completed, savepoint, failed));
	}
}
