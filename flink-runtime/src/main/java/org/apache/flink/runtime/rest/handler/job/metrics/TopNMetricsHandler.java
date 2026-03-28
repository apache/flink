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
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.messages.TopNMetricsResponseBody;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TopNMetricsMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Request handler that returns Top N metrics for a job, including CPU consumers, backpressured
 * operators, and GC-intensive tasks.
 */
public class TopNMetricsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                TopNMetricsResponseBody,
                TopNMetricsMessageParameters> {

    private static final int DEFAULT_TOP_N = 5;

    private static final String CPU_METRIC = "Status.JVM.CPU.Load";
    private static final String GC_TIME_METRIC = "Status.JVM.GarbageCollector.All.Time";

    private final Executor executor;
    private final MetricFetcher fetcher;

    public TopNMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        super(leaderRetriever, timeout, responseHeaders, TopNMetricsHeaders.getInstance());
        this.executor = executor;
        this.fetcher = fetcher;
    }

    @Override
    protected CompletableFuture<TopNMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        fetcher.update();
                        MetricStore store = fetcher.getMetricStore();

                        JobID jobId = request.getPathParameter(JobIDPathParameter.class);

                        // Get Top N CPU consumers from TaskManagers
                        List<TopNMetricsResponseBody.CpuConsumerInfo> topCpuConsumers =
                                getTopCpuConsumers(store, jobId);

                        // Get Top N GC-intensive TaskManagers
                        List<TopNMetricsResponseBody.GcTaskInfo> topGcIntensiveTasks =
                                getTopGcIntensiveTasks(store, jobId);

                        return new TopNMetricsResponseBody(
                                topCpuConsumers, Collections.emptyList(), topGcIntensiveTasks);

                    } catch (Exception e) {
                        log.warn("Could not retrieve Top N metrics.", e);
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Could not retrieve Top N metrics.",
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR));
                    }
                },
                executor);
    }

    private List<TopNMetricsResponseBody.CpuConsumerInfo> getTopCpuConsumers(
            MetricStore store, JobID jobId) {
        List<TopNMetricsResponseBody.CpuConsumerInfo> cpuConsumers = new ArrayList<>();

        Map<String, MetricStore.TaskManagerMetricStore> taskManagers = store.getTaskManagers();
        int index = 0;
        for (Map.Entry<String, MetricStore.TaskManagerMetricStore> entry :
                taskManagers.entrySet()) {
            String tmId = entry.getKey();
            MetricStore.TaskManagerMetricStore tmStore = entry.getValue();

            String cpuValue = tmStore.metrics.get(CPU_METRIC);
            if (cpuValue != null) {
                try {
                    double cpuLoad = Double.parseDouble(cpuValue);
                    cpuConsumers.add(
                            new TopNMetricsResponseBody.CpuConsumerInfo(
                                    index++, tmId, tmId, cpuLoad * 100.0, tmId));
                } catch (NumberFormatException e) {
                    // Skip invalid values
                }
            }
        }

        return cpuConsumers.stream()
                .sorted(
                        Comparator.comparing(
                                        TopNMetricsResponseBody.CpuConsumerInfo::getCpuPercentage)
                                .reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }

    private List<TopNMetricsResponseBody.GcTaskInfo> getTopGcIntensiveTasks(
            MetricStore store, JobID jobId) {
        List<TopNMetricsResponseBody.GcTaskInfo> gcIntensiveTasks = new ArrayList<>();

        Map<String, MetricStore.TaskManagerMetricStore> taskManagers = store.getTaskManagers();
        for (Map.Entry<String, MetricStore.TaskManagerMetricStore> entry :
                taskManagers.entrySet()) {
            String tmId = entry.getKey();
            MetricStore.TaskManagerMetricStore tmStore = entry.getValue();

            String gcTimeValue = tmStore.metrics.get(GC_TIME_METRIC);
            if (gcTimeValue != null) {
                try {
                    double gcTime = Double.parseDouble(gcTimeValue);
                    gcIntensiveTasks.add(
                            new TopNMetricsResponseBody.GcTaskInfo(tmId, tmId, gcTime, tmId));
                } catch (NumberFormatException e) {
                    // Skip invalid values
                }
            }
        }

        return gcIntensiveTasks.stream()
                .sorted(
                        Comparator.comparing(
                                        TopNMetricsResponseBody.GcTaskInfo::getGcTimePercentage)
                                .reversed())
                .limit(DEFAULT_TOP_N)
                .collect(Collectors.toList());
    }
}
