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
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedJobMetricsParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/** Tests for the {@link AggregatingJobsMetricsHandler}. */
public class AggregatingJobsMetricsHandlerTest
        extends AggregatingMetricsHandlerTestBase<
                AggregatingJobsMetricsHandler, AggregatedJobMetricsParameters> {

    private static final JobID JOB_ID_1 = JobID.generate();
    private static final JobID JOB_ID_2 = JobID.generate();
    private static final JobID JOB_ID_3 = JobID.generate();

    @Override
    protected Tuple2<String, List<String>> getFilter() {
        return Tuple2.of("jobs", Arrays.asList(JOB_ID_1.toString(), JOB_ID_3.toString()));
    }

    @Override
    protected Collection<MetricDump> getMetricDumps() {
        Collection<MetricDump> dumps = new ArrayList<>(3);
        QueryScopeInfo.JobQueryScopeInfo job =
                new QueryScopeInfo.JobQueryScopeInfo(JOB_ID_1.toString(), "abc");
        MetricDump.CounterDump cd1 = new MetricDump.CounterDump(job, "metric1", 1);
        dumps.add(cd1);

        QueryScopeInfo.JobQueryScopeInfo job2 =
                new QueryScopeInfo.JobQueryScopeInfo(JOB_ID_2.toString(), "abc");
        MetricDump.CounterDump cd2 = new MetricDump.CounterDump(job2, "metric1", 3);
        dumps.add(cd2);

        QueryScopeInfo.JobQueryScopeInfo job3 =
                new QueryScopeInfo.JobQueryScopeInfo(JOB_ID_3.toString(), "abc");
        MetricDump.CounterDump cd3 = new MetricDump.CounterDump(job3, "metric2", 5);
        dumps.add(cd3);
        return dumps;
    }

    @Override
    protected AggregatingJobsMetricsHandler getHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        return new AggregatingJobsMetricsHandler(
                leaderRetriever, timeout, responseHeaders, executor, fetcher);
    }
}
