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
import org.apache.flink.runtime.checkpoint.MinMaxAvgStats;
import org.apache.flink.runtime.checkpoint.SubtaskStateStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.MinMaxAvgStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.SubtaskCheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatisticsWithSubtaskDetails;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * REST handler which serves checkpoint statistics for subtasks.
 */
public class TaskCheckpointStatisticDetailsHandler extends AbstractCheckpointHandler<TaskCheckpointStatisticsWithSubtaskDetails, TaskCheckpointMessageParameters> {

	public TaskCheckpointStatisticDetailsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, TaskCheckpointStatisticsWithSubtaskDetails, TaskCheckpointMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			CheckpointStatsCache checkpointStatsCache) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor,
			checkpointStatsCache);
	}

	@Override
	protected TaskCheckpointStatisticsWithSubtaskDetails handleCheckpointRequest(
			HandlerRequest<EmptyRequestBody, TaskCheckpointMessageParameters> request,
			AbstractCheckpointStats checkpointStats) throws RestHandlerException {

		final JobVertexID jobVertexId = request.getPathParameter(JobVertexIdPathParameter.class);

		final TaskStateStats taskStatistics = checkpointStats.getTaskStateStats(jobVertexId);

		if (taskStatistics != null) {

			final TaskCheckpointStatisticsWithSubtaskDetails.Summary summary = createSummary(
				taskStatistics.getSummaryStats(),
				checkpointStats.getTriggerTimestamp());

			final List<SubtaskCheckpointStatistics> subtaskCheckpointStatistics = createSubtaskCheckpointStatistics(
				taskStatistics.getSubtaskStats(),
				checkpointStats.getTriggerTimestamp());

			return new TaskCheckpointStatisticsWithSubtaskDetails(
				checkpointStats.getCheckpointId(),
				checkpointStats.getStatus(),
				taskStatistics.getLatestAckTimestamp(),
				taskStatistics.getStateSize(),
				taskStatistics.getEndToEndDuration(checkpointStats.getTriggerTimestamp()),
				taskStatistics.getAlignmentBuffered(),
				taskStatistics.getNumberOfSubtasks(),
				taskStatistics.getNumberOfAcknowledgedSubtasks(),
				summary,
				subtaskCheckpointStatistics);
		} else {
			throw new RestHandlerException("There is no checkpoint statistics for task " + jobVertexId + '.', HttpResponseStatus.NOT_FOUND);
		}
	}

	private static TaskCheckpointStatisticsWithSubtaskDetails.Summary createSummary(TaskStateStats.TaskStateStatsSummary taskStatisticsSummary, long triggerTimestamp) {
		final MinMaxAvgStats stateSizeStats = taskStatisticsSummary.getStateSizeStats();
		final MinMaxAvgStats ackTSStats = taskStatisticsSummary.getAckTimestampStats();
		final MinMaxAvgStats syncDurationStats = taskStatisticsSummary.getSyncCheckpointDurationStats();
		final MinMaxAvgStats asyncDurationStats = taskStatisticsSummary.getAsyncCheckpointDurationStats();

		final TaskCheckpointStatisticsWithSubtaskDetails.CheckpointDuration checkpointDuration = new TaskCheckpointStatisticsWithSubtaskDetails.CheckpointDuration(
			new MinMaxAvgStatistics(syncDurationStats.getMinimum(), syncDurationStats.getMaximum(), syncDurationStats.getAverage()),
			new MinMaxAvgStatistics(asyncDurationStats.getMinimum(), asyncDurationStats.getMaximum(), asyncDurationStats.getAverage()));

		final MinMaxAvgStats alignmentBufferedStats = taskStatisticsSummary.getAlignmentBufferedStats();
		final MinMaxAvgStats alignmentDurationStats = taskStatisticsSummary.getAlignmentDurationStats();

		final TaskCheckpointStatisticsWithSubtaskDetails.CheckpointAlignment checkpointAlignment = new TaskCheckpointStatisticsWithSubtaskDetails.CheckpointAlignment(
			new MinMaxAvgStatistics(alignmentBufferedStats.getMinimum(), alignmentBufferedStats.getMaximum(), alignmentBufferedStats.getAverage()),
			new MinMaxAvgStatistics(alignmentDurationStats.getMinimum(), alignmentDurationStats.getMaximum(), alignmentDurationStats.getAverage()));

		return new TaskCheckpointStatisticsWithSubtaskDetails.Summary(
			new MinMaxAvgStatistics(stateSizeStats.getMinimum(), stateSizeStats.getMaximum(), stateSizeStats.getAverage()),
			new MinMaxAvgStatistics(
				Math.max(0L, ackTSStats.getMinimum() - triggerTimestamp),
				Math.max(0L, ackTSStats.getMaximum() - triggerTimestamp),
				Math.max(0L, ackTSStats.getAverage() - triggerTimestamp)),
			checkpointDuration,
			checkpointAlignment);
	}

	private static List<SubtaskCheckpointStatistics> createSubtaskCheckpointStatistics(SubtaskStateStats[] subtaskStateStats, long triggerTimestamp) {
		final List<SubtaskCheckpointStatistics> result = new ArrayList<>(subtaskStateStats.length);

		for (int i = 0; i < subtaskStateStats.length; i++) {
			final SubtaskStateStats subtask = subtaskStateStats[i];

			if (subtask == null) {
				result.add(new SubtaskCheckpointStatistics.PendingSubtaskCheckpointStatistics(i));
			} else {
				result.add(new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics(
					i,
					subtask.getAckTimestamp(),
					subtask.getEndToEndDuration(triggerTimestamp),
					subtask.getStateSize(),
					new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics.CheckpointDuration(
						subtask.getSyncCheckpointDuration(),
						subtask.getAsyncCheckpointDuration()),
					new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics.CheckpointAlignment(
						subtask.getAlignmentBuffered(),
						subtask.getAlignmentDuration())
				));
			}
		}

		return result;
	}
}
