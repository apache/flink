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
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStatsSummary;
import org.apache.flink.runtime.checkpoint.RestoredCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractAccessExecutionGraphHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.MinMaxAvgStatistics;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/** Handler which serves the checkpoint statistics. */
public class CheckpointingStatisticsHandler
        extends AbstractAccessExecutionGraphHandler<CheckpointingStatistics, JobMessageParameters>
        implements JsonArchivist {

    public CheckpointingStatisticsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, CheckpointingStatistics, JobMessageParameters>
                    messageHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executionGraphCache,
                executor);
    }

    @Override
    protected CheckpointingStatistics handleRequest(
            HandlerRequest<EmptyRequestBody, JobMessageParameters> request,
            AccessExecutionGraph executionGraph)
            throws RestHandlerException {
        return createCheckpointingStatistics(executionGraph);
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph)
            throws IOException {
        ResponseBody json;
        try {
            json = createCheckpointingStatistics(graph);
        } catch (RestHandlerException rhe) {
            json = new ErrorResponseBody(rhe.getMessage());
        }
        String path =
                getMessageHeaders()
                        .getTargetRestEndpointURL()
                        .replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());
        return Collections.singletonList(new ArchivedJson(path, json));
    }

    private static CheckpointingStatistics createCheckpointingStatistics(
            AccessExecutionGraph executionGraph) throws RestHandlerException {
        final CheckpointStatsSnapshot checkpointStatsSnapshot =
                executionGraph.getCheckpointStatsSnapshot();

        if (checkpointStatsSnapshot == null) {
            throw new RestHandlerException(
                    "Checkpointing has not been enabled.",
                    HttpResponseStatus.NOT_FOUND,
                    RestHandlerException.LoggingBehavior.IGNORE);
        } else {
            final CheckpointStatsCounts checkpointStatsCounts = checkpointStatsSnapshot.getCounts();

            final CheckpointingStatistics.Counts counts =
                    new CheckpointingStatistics.Counts(
                            checkpointStatsCounts.getNumberOfRestoredCheckpoints(),
                            checkpointStatsCounts.getTotalNumberOfCheckpoints(),
                            checkpointStatsCounts.getNumberOfInProgressCheckpoints(),
                            checkpointStatsCounts.getNumberOfCompletedCheckpoints(),
                            checkpointStatsCounts.getNumberOfFailedCheckpoints());

            final CompletedCheckpointStatsSummary checkpointStatsSummary =
                    checkpointStatsSnapshot.getSummaryStats();

            final CheckpointingStatistics.Summary summary =
                    new CheckpointingStatistics.Summary(
                            MinMaxAvgStatistics.valueOf(checkpointStatsSummary.getStateSizeStats()),
                            MinMaxAvgStatistics.valueOf(
                                    checkpointStatsSummary.getEndToEndDurationStats()),
                            new MinMaxAvgStatistics(0, 0, 0),
                            MinMaxAvgStatistics.valueOf(
                                    checkpointStatsSummary.getProcessedDataStats()),
                            MinMaxAvgStatistics.valueOf(
                                    checkpointStatsSummary.getPersistedDataStats()));

            final CheckpointStatsHistory checkpointStatsHistory =
                    checkpointStatsSnapshot.getHistory();

            final CheckpointStatistics.CompletedCheckpointStatistics completed =
                    checkpointStatsHistory.getLatestCompletedCheckpoint() != null
                            ? (CheckpointStatistics.CompletedCheckpointStatistics)
                                    CheckpointStatistics.generateCheckpointStatistics(
                                            checkpointStatsHistory.getLatestCompletedCheckpoint(),
                                            false)
                            : null;

            final CheckpointStatistics.CompletedCheckpointStatistics savepoint =
                    checkpointStatsHistory.getLatestSavepoint() != null
                            ? (CheckpointStatistics.CompletedCheckpointStatistics)
                                    CheckpointStatistics.generateCheckpointStatistics(
                                            checkpointStatsHistory.getLatestSavepoint(), false)
                            : null;

            final CheckpointStatistics.FailedCheckpointStatistics failed =
                    checkpointStatsHistory.getLatestFailedCheckpoint() != null
                            ? (CheckpointStatistics.FailedCheckpointStatistics)
                                    CheckpointStatistics.generateCheckpointStatistics(
                                            checkpointStatsHistory.getLatestFailedCheckpoint(),
                                            false)
                            : null;

            final RestoredCheckpointStats restoredCheckpointStats =
                    checkpointStatsSnapshot.getLatestRestoredCheckpoint();

            final CheckpointingStatistics.RestoredCheckpointStatistics restored;

            if (restoredCheckpointStats == null) {
                restored = null;
            } else {
                restored =
                        new CheckpointingStatistics.RestoredCheckpointStatistics(
                                restoredCheckpointStats.getCheckpointId(),
                                restoredCheckpointStats.getRestoreTimestamp(),
                                restoredCheckpointStats.getProperties().isSavepoint(),
                                restoredCheckpointStats.getExternalPath());
            }

            final CheckpointingStatistics.LatestCheckpoints latestCheckpoints =
                    new CheckpointingStatistics.LatestCheckpoints(
                            completed, savepoint, failed, restored);

            final List<CheckpointStatistics> history = new ArrayList<>(16);

            for (AbstractCheckpointStats abstractCheckpointStats :
                    checkpointStatsSnapshot.getHistory().getCheckpoints()) {
                history.add(
                        CheckpointStatistics.generateCheckpointStatistics(
                                abstractCheckpointStats, false));
            }

            return new CheckpointingStatistics(counts, summary, latestCheckpoints, history);
        }
    }
}
