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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/** Tests for the {@link AggregatingSubtasksMetricsHandler}. */
public class AggregatingSubtasksMetricsHandlerTest
        extends AggregatingMetricsHandlerTestBase<
                AggregatingSubtasksMetricsHandler, AggregatedSubtaskMetricsParameters> {

    private static final JobID JOB_ID = JobID.generate();
    private static final JobVertexID TASK_ID = new JobVertexID();

    @Override
    protected Tuple2<String, List<String>> getFilter() {
        return Tuple2.of("subtasks", Arrays.asList("1", "3"));
    }

    @Override
    protected Map<String, String> getPathParameters() {
        Map<String, String> pathParameters = new HashMap<>(4);
        pathParameters.put(JobIDPathParameter.KEY, JOB_ID.toString());
        pathParameters.put(JobVertexIdPathParameter.KEY, TASK_ID.toString());
        return pathParameters;
    }

    @Override
    protected Collection<MetricDump> getMetricDumps() {
        Collection<MetricDump> dumps = new ArrayList<>(3);
        QueryScopeInfo.TaskQueryScopeInfo task1 =
                new QueryScopeInfo.TaskQueryScopeInfo(
                        JOB_ID.toString(), TASK_ID.toString(), 1, "abc");
        MetricDump.CounterDump cd1 = new MetricDump.CounterDump(task1, "metric1", 1);
        dumps.add(cd1);

        QueryScopeInfo.TaskQueryScopeInfo task2 =
                new QueryScopeInfo.TaskQueryScopeInfo(
                        JOB_ID.toString(), TASK_ID.toString(), 2, "abc");
        MetricDump.CounterDump cd2 = new MetricDump.CounterDump(task2, "metric1", 3);
        dumps.add(cd2);

        QueryScopeInfo.TaskQueryScopeInfo task3 =
                new QueryScopeInfo.TaskQueryScopeInfo(
                        JOB_ID.toString(), TASK_ID.toString(), 3, "abc");
        MetricDump.CounterDump cd3 = new MetricDump.CounterDump(task3, "metric2", 5);
        dumps.add(cd3);

        return dumps;
    }

    @Override
    protected AggregatingSubtasksMetricsHandler getHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        return new AggregatingSubtasksMetricsHandler(
                leaderRetriever, timeout, responseHeaders, executor, fetcher);
    }
}
