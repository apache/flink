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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsHistory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.StatsSummary;
import org.apache.flink.runtime.checkpoint.SubtaskStateStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointIdPathParameter;
import org.apache.flink.runtime.rest.messages.checkpoints.StatsSummaryDto;
import org.apache.flink.runtime.rest.messages.checkpoints.SubtaskCheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatisticsWithSubtaskDetails;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.OnlyExecutionGraphJsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** REST handler which serves checkpoint statistics for subtasks. */
public class TaskCheckpointStatisticDetailsHandler
        extends AbstractCheckpointHandler<
                TaskCheckpointStatisticsWithSubtaskDetails, TaskCheckpointMessageParameters>
        implements OnlyExecutionGraphJsonArchivist {

    public TaskCheckpointStatisticDetailsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            EmptyRequestBody,
                            TaskCheckpointStatisticsWithSubtaskDetails,
                            TaskCheckpointMessageParameters>
                    messageHeaders,
            Executor executor,
            Cache<JobID, CompletableFuture<CheckpointStatsSnapshot>> checkpointStatsSnapshotCache,
            CheckpointStatsCache checkpointStatsCache) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executor,
                checkpointStatsSnapshotCache,
                checkpointStatsCache);
    }

    @Override
    protected TaskCheckpointStatisticsWithSubtaskDetails handleCheckpointRequest(
            HandlerRequest<EmptyRequestBody> request, AbstractCheckpointStats checkpointStats)
            throws RestHandlerException {

        final JobVertexID jobVertexId = request.getPathParameter(JobVertexIdPathParameter.class);

        final TaskStateStats taskStatistics = checkpointStats.getTaskStateStats(jobVertexId);

        if (taskStatistics == null) {
            throw new NotFoundException(
                    "There is no checkpoint statistics for task " + jobVertexId + '.');
        }

        return createCheckpointDetails(checkpointStats, taskStatistics);
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph)
            throws IOException {
        CheckpointStatsSnapshot stats = graph.getCheckpointStatsSnapshot();
        if (stats == null) {
            return Collections.emptyList();
        }
        CheckpointStatsHistory history = stats.getHistory();
        List<ArchivedJson> archive = new ArrayList<>(history.getCheckpoints().size());
        for (AbstractCheckpointStats checkpoint : history.getCheckpoints()) {
            for (TaskStateStats subtaskStats : checkpoint.getAllTaskStateStats()) {
                ResponseBody json = createCheckpointDetails(checkpoint, subtaskStats);
                String path =
                        getMessageHeaders()
                                .getTargetRestEndpointURL()
                                .replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString())
                                .replace(
                                        ':' + CheckpointIdPathParameter.KEY,
                                        String.valueOf(checkpoint.getCheckpointId()))
                                .replace(
                                        ':' + JobVertexIdPathParameter.KEY,
                                        subtaskStats.getJobVertexId().toString());
                archive.add(new ArchivedJson(path, json));
            }
        }
        return archive;
    }

    private static TaskCheckpointStatisticsWithSubtaskDetails createCheckpointDetails(
            AbstractCheckpointStats checkpointStats, TaskStateStats taskStatistics) {
        final TaskCheckpointStatisticsWithSubtaskDetails.Summary summary =
                createSummary(
                        taskStatistics.getSummaryStats(), checkpointStats.getTriggerTimestamp());

        final List<SubtaskCheckpointStatistics> subtaskCheckpointStatistics =
                createSubtaskCheckpointStatistics(
                        taskStatistics.getSubtaskStats(), checkpointStats.getTriggerTimestamp());

        return new TaskCheckpointStatisticsWithSubtaskDetails(
                checkpointStats.getCheckpointId(),
                checkpointStats.getStatus(),
                taskStatistics.getLatestAckTimestamp(),
                taskStatistics.getCheckpointedSize(),
                taskStatistics.getStateSize(),
                taskStatistics.getEndToEndDuration(checkpointStats.getTriggerTimestamp()),
                0,
                taskStatistics.getProcessedDataStats(),
                taskStatistics.getPersistedDataStats(),
                taskStatistics.getNumberOfSubtasks(),
                taskStatistics.getNumberOfAcknowledgedSubtasks(),
                summary,
                subtaskCheckpointStatistics);
    }

    private static TaskCheckpointStatisticsWithSubtaskDetails.Summary createSummary(
            TaskStateStats.TaskStateStatsSummary taskStatisticsSummary, long triggerTimestamp) {
        final StatsSummary ackTSStats = taskStatisticsSummary.getAckTimestampStats();

        final TaskCheckpointStatisticsWithSubtaskDetails.CheckpointDuration checkpointDuration =
                new TaskCheckpointStatisticsWithSubtaskDetails.CheckpointDuration(
                        StatsSummaryDto.valueOf(
                                taskStatisticsSummary.getSyncCheckpointDurationStats()),
                        StatsSummaryDto.valueOf(
                                taskStatisticsSummary.getAsyncCheckpointDurationStats()));

        final TaskCheckpointStatisticsWithSubtaskDetails.CheckpointAlignment checkpointAlignment =
                new TaskCheckpointStatisticsWithSubtaskDetails.CheckpointAlignment(
                        new StatsSummaryDto(0, 0, 0, 0, 0, 0, 0, 0),
                        StatsSummaryDto.valueOf(taskStatisticsSummary.getProcessedDataStats()),
                        StatsSummaryDto.valueOf(taskStatisticsSummary.getPersistedDataStats()),
                        StatsSummaryDto.valueOf(taskStatisticsSummary.getAlignmentDurationStats()));

        return new TaskCheckpointStatisticsWithSubtaskDetails.Summary(
                StatsSummaryDto.valueOf(taskStatisticsSummary.getCheckpointedSize()),
                StatsSummaryDto.valueOf(taskStatisticsSummary.getStateSizeStats()),
                new StatsSummaryDto(
                        Math.max(0L, ackTSStats.getMinimum() - triggerTimestamp),
                        Math.max(0L, ackTSStats.getMaximum() - triggerTimestamp),
                        Math.max(0L, ackTSStats.getAverage() - triggerTimestamp),
                        ackTSStats.createSnapshot().getQuantile(.50d),
                        ackTSStats.createSnapshot().getQuantile(.90d),
                        ackTSStats.createSnapshot().getQuantile(.95d),
                        ackTSStats.createSnapshot().getQuantile(.99d),
                        ackTSStats.createSnapshot().getQuantile(.999d)),
                checkpointDuration,
                checkpointAlignment,
                StatsSummaryDto.valueOf(taskStatisticsSummary.getCheckpointStartDelayStats()));
    }

    private static List<SubtaskCheckpointStatistics> createSubtaskCheckpointStatistics(
            SubtaskStateStats[] subtaskStateStats, long triggerTimestamp) {
        final List<SubtaskCheckpointStatistics> result = new ArrayList<>(subtaskStateStats.length);

        for (int i = 0; i < subtaskStateStats.length; i++) {
            final SubtaskStateStats subtask = subtaskStateStats[i];

            if (subtask == null) {
                result.add(new SubtaskCheckpointStatistics.PendingSubtaskCheckpointStatistics(i));
            } else {
                result.add(
                        new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics(
                                i,
                                subtask.getAckTimestamp(),
                                subtask.getEndToEndDuration(triggerTimestamp),
                                subtask.getCheckpointedSize(),
                                subtask.getStateSize(),
                                new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics
                                        .CheckpointDuration(
                                        subtask.getSyncCheckpointDuration(),
                                        subtask.getAsyncCheckpointDuration()),
                                new SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics
                                        .CheckpointAlignment(
                                        0,
                                        subtask.getProcessedData(),
                                        subtask.getPersistedData(),
                                        subtask.getAlignmentDuration()),
                                subtask.getCheckpointStartDelay(),
                                subtask.getUnalignedCheckpoint(),
                                !subtask.isCompleted()));
            }
        }

        return result;
    }
}
