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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.SubtasksFilterQueryParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.UnionIterator;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;

/**
 * Request handler that returns, aggregated across subtasks, a list of all available metrics or the
 * values for a set of metrics.
 *
 * <p>Specific subtasks can be selected for aggregation by specifying a comma-separated list of
 * integer ranges. {@code /metrics?get=X,Y&subtasks=0-2,4-5}
 */
public class AggregatingSubtasksMetricsHandler
        extends AbstractAggregatingMetricsHandler<AggregatedSubtaskMetricsParameters> {

    public AggregatingSubtasksMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                AggregatedSubtaskMetricsHeaders.getInstance(),
                executor,
                fetcher);
    }

    @Nonnull
    @Override
    Collection<? extends MetricStore.ComponentMetricStore> getStores(
            MetricStore store,
            HandlerRequest<EmptyRequestBody, AggregatedSubtaskMetricsParameters> request) {
        JobID jobID = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID taskID = request.getPathParameter(JobVertexIdPathParameter.class);

        Collection<String> subtaskRanges =
                request.getQueryParameter(SubtasksFilterQueryParameter.class);
        if (subtaskRanges.isEmpty()) {
            MetricStore.TaskMetricStore taskMetricStore =
                    store.getTaskMetricStore(jobID.toString(), taskID.toString());
            if (taskMetricStore != null) {
                return taskMetricStore.getAllSubtaskMetricStores().values();
            } else {
                return Collections.emptyList();
            }
        } else {
            Iterable<Integer> subtasks = getIntegerRangeFromString(subtaskRanges);
            Collection<MetricStore.ComponentMetricStore> subtaskStores = new ArrayList<>(8);
            for (int subtask : subtasks) {
                MetricStore.ComponentMetricStore subtaskMetricStore =
                        store.getSubtaskMetricStore(jobID.toString(), taskID.toString(), subtask);
                if (subtaskMetricStore != null) {
                    subtaskStores.add(subtaskMetricStore);
                }
            }
            return subtaskStores;
        }
    }

    private Iterable<Integer> getIntegerRangeFromString(Collection<String> ranges) {
        UnionIterator<Integer> iterators = new UnionIterator<>();

        for (String rawRange : ranges) {
            try {
                Iterator<Integer> rangeIterator;
                String range = rawRange.trim();
                int dashIdx = range.indexOf('-');
                if (dashIdx == -1) {
                    // only one value in range:
                    rangeIterator = Collections.singleton(Integer.valueOf(range)).iterator();
                } else {
                    // evaluate range
                    final int start = Integer.valueOf(range.substring(0, dashIdx));
                    final int end = Integer.valueOf(range.substring(dashIdx + 1, range.length()));
                    rangeIterator = IntStream.rangeClosed(start, end).iterator();
                }
                iterators.add(rangeIterator);
            } catch (NumberFormatException nfe) {
                log.warn(
                        "Invalid value {} specified for integer range. Not a number.",
                        rawRange,
                        nfe);
            }
        }

        return iterators;
    }
}
