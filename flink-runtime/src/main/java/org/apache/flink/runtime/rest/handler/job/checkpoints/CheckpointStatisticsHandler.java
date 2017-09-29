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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStatsSummary;
import org.apache.flink.runtime.checkpoint.FailedCheckpointStats;
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.RestoredCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractExecutionGraphHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.CheckpointStatistics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handler which serves the checkpoint statistics.
 */
public class CheckpointStatisticsHandler extends AbstractExecutionGraphHandler<CheckpointStatistics> {

	public CheckpointStatisticsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			MessageHeaders<EmptyRequestBody, CheckpointStatistics, JobMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(localRestAddress, leaderRetriever, timeout, messageHeaders, executionGraphCache, executor);
	}

	@Override
	protected CheckpointStatistics handleRequest(AccessExecutionGraph executionGraph) throws RestHandlerException {

		final CheckpointStatsSnapshot checkpointStatsSnapshot = executionGraph.getCheckpointStatsSnapshot();

		if (checkpointStatsSnapshot == null) {
			throw new RestHandlerException("Checkpointing has not been enabled.", HttpResponseStatus.NOT_FOUND);
		} else {
			final CheckpointStatsCounts checkpointStatsCounts = checkpointStatsSnapshot.getCounts();

			final CheckpointStatistics.Counts counts = new CheckpointStatistics.Counts(
				checkpointStatsCounts.getNumberOfRestoredCheckpoints(),
				checkpointStatsCounts.getTotalNumberOfCheckpoints(),
				checkpointStatsCounts.getNumberOfInProgressCheckpoints(),
				checkpointStatsCounts.getNumberOfCompletedCheckpoints(),
				checkpointStatsCounts.getNumberOfFailedCheckpoints());

			final CompletedCheckpointStatsSummary checkpointStatsSummary = checkpointStatsSnapshot.getSummaryStats();
			final MinMaxAvgStats stateSize = checkpointStatsSummary.getStateSizeStats();
			final MinMaxAvgStats duration = checkpointStatsSummary.getEndToEndDurationStats();
			final MinMaxAvgStats alignment = checkpointStatsSummary.getAlignmentBufferedStats();

			final CheckpointStatistics.Summary summary = new CheckpointStatistics.Summary(
				new CheckpointStatistics.MinMaxAvgStatistics(
					stateSize.getMinimum(),
					stateSize.getMaximum(),
					stateSize.getAverage()),
				new CheckpointStatistics.MinMaxAvgStatistics(
					duration.getMinimum(),
					duration.getMaximum(),
					duration.getAverage()),
				new CheckpointStatistics.MinMaxAvgStatistics(
					alignment.getMinimum(),
					alignment.getMaximum(),
					alignment.getAverage()));

			final CheckpointStatsHistory checkpointStatsHistory = checkpointStatsSnapshot.getHistory();

			final CheckpointStatistics.CompletedCheckpointStatistics completed = (CheckpointStatistics.CompletedCheckpointStatistics) generateCheckpointStatistics(checkpointStatsHistory.getLatestCompletedCheckpoint());
			final CheckpointStatistics.CompletedCheckpointStatistics savepoint = (CheckpointStatistics.CompletedCheckpointStatistics) generateCheckpointStatistics(checkpointStatsHistory.getLatestSavepoint());
			final CheckpointStatistics.FailedCheckpointStatistics failed = (CheckpointStatistics.FailedCheckpointStatistics) generateCheckpointStatistics(checkpointStatsHistory.getLatestFailedCheckpoint());

			final RestoredCheckpointStats restoredCheckpointStats = checkpointStatsSnapshot.getLatestRestoredCheckpoint();

			final CheckpointStatistics.RestoredCheckpointStatistics restored;

			if (restoredCheckpointStats == null) {
				restored = null;
			} else {
				restored = new CheckpointStatistics.RestoredCheckpointStatistics(
					restoredCheckpointStats.getCheckpointId(),
					restoredCheckpointStats.getRestoreTimestamp(),
					restoredCheckpointStats.getProperties().isSavepoint(),
					restoredCheckpointStats.getExternalPath());
			}

			final CheckpointStatistics.LatestCheckpoints latestCheckpoints = new CheckpointStatistics.LatestCheckpoints(
				completed,
				savepoint,
				failed,
				restored);

			final List<CheckpointStatistics.BaseCheckpointStatistics> history = new ArrayList<>(16);

			for (AbstractCheckpointStats abstractCheckpointStats : checkpointStatsSnapshot.getHistory().getCheckpoints()) {
				history.add(generateCheckpointStatistics(abstractCheckpointStats));
			}

			return new CheckpointStatistics(
				counts,
				summary,
				latestCheckpoints,
				history);
		}
	}

	private static CheckpointStatistics.BaseCheckpointStatistics generateCheckpointStatistics(AbstractCheckpointStats checkpointStats) {
		if (checkpointStats != null) {
			if (checkpointStats instanceof CompletedCheckpointStats) {
				final CompletedCheckpointStats completedCheckpointStats = ((CompletedCheckpointStats) checkpointStats);

				return new CheckpointStatistics.CompletedCheckpointStatistics(
					completedCheckpointStats.getCheckpointId(),
					completedCheckpointStats.getStatus(),
					completedCheckpointStats.getProperties().isSavepoint(),
					completedCheckpointStats.getTriggerTimestamp(),
					completedCheckpointStats.getLatestAckTimestamp(),
					completedCheckpointStats.getStateSize(),
					completedCheckpointStats.getEndToEndDuration(),
					completedCheckpointStats.getAlignmentBuffered(),
					completedCheckpointStats.getNumberOfSubtasks(),
					completedCheckpointStats.getNumberOfAcknowledgedSubtasks(),
					completedCheckpointStats.getExternalPath(),
					completedCheckpointStats.isDiscarded());
			} else if (checkpointStats instanceof FailedCheckpointStats) {
				final FailedCheckpointStats failedCheckpointStats = ((FailedCheckpointStats) checkpointStats);

				return new CheckpointStatistics.FailedCheckpointStatistics(
					failedCheckpointStats.getCheckpointId(),
					failedCheckpointStats.getStatus(),
					failedCheckpointStats.getProperties().isSavepoint(),
					failedCheckpointStats.getTriggerTimestamp(),
					failedCheckpointStats.getLatestAckTimestamp(),
					failedCheckpointStats.getStateSize(),
					failedCheckpointStats.getEndToEndDuration(),
					failedCheckpointStats.getAlignmentBuffered(),
					failedCheckpointStats.getNumberOfSubtasks(),
					failedCheckpointStats.getNumberOfAcknowledgedSubtasks(),
					failedCheckpointStats.getFailureTimestamp(),
					failedCheckpointStats.getFailureMessage());
			} else {
				throw new IllegalArgumentException("Given checkpoint stats object of type " + checkpointStats.getClass().getName() + " cannot be converted.");
			}
		} else {
			return null;
		}
	}
}
