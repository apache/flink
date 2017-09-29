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
import org.apache.flink.runtime.rest.messages.CheckpointStatistics;

import java.util.Arrays;

/**
 * Tests for {@link CheckpointStatistics}.
 */
public class CheckpointStatisticsTest extends RestResponseMarshallingTestBase<CheckpointStatistics> {
	@Override
	protected Class<CheckpointStatistics> getTestResponseClass() {
		return CheckpointStatistics.class;
	}

	@Override
	protected CheckpointStatistics getTestResponseInstance() throws Exception {

		final CheckpointStatistics.Counts counts = new CheckpointStatistics.Counts(1, 2, 3, 4, 5);
		final CheckpointStatistics.Summary summary = new CheckpointStatistics.Summary(
			new CheckpointStatistics.MinMaxAvgStatistics(1L, 1L, 1L),
			new CheckpointStatistics.MinMaxAvgStatistics(2L, 2L, 2L),
			new CheckpointStatistics.MinMaxAvgStatistics(3L, 3L, 3L));

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
			100L,
			"Test failure");

		CheckpointStatistics.RestoredCheckpointStatistics restored = new CheckpointStatistics.RestoredCheckpointStatistics(
			4L,
			1445L,
			true,
			"foobar");

		final CheckpointStatistics.LatestCheckpoints latestCheckpoints = new CheckpointStatistics.LatestCheckpoints(
			completed,
			savepoint,
			failed,
			restored);

		return new CheckpointStatistics(
			counts,
			summary,
			latestCheckpoints,
			Arrays.asList(completed, savepoint, failed));
	}
}
