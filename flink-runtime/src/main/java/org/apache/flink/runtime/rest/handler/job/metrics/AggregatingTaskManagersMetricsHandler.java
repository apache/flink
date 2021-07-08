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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregateTaskManagerMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedTaskManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagersFilterQueryParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler that returns, aggregated across task managers, a list of all available metrics or
 * the values for a set of metrics.
 *
 * <p>Specific taskmanagers can be selected for aggregation by specifying a comma-separated list of
 * taskmanager IDs. {@code /metrics?get=X,Y&taskmanagers=A,B}
 */
public class AggregatingTaskManagersMetricsHandler
        extends AbstractAggregatingMetricsHandler<AggregateTaskManagerMetricsParameters> {

    public AggregatingTaskManagersMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                AggregatedTaskManagerMetricsHeaders.getInstance(),
                executor,
                fetcher);
    }

    @Nonnull
    @Override
    Collection<? extends MetricStore.ComponentMetricStore> getStores(
            MetricStore store,
            HandlerRequest<EmptyRequestBody, AggregateTaskManagerMetricsParameters> request) {
        List<ResourceID> taskmanagers =
                request.getQueryParameter(TaskManagersFilterQueryParameter.class);
        if (taskmanagers.isEmpty()) {
            return store.getTaskManagers().values();
        } else {
            Collection<MetricStore.TaskManagerMetricStore> taskmanagerStores =
                    new ArrayList<>(taskmanagers.size());
            for (ResourceID taskmanager : taskmanagers) {
                MetricStore.TaskManagerMetricStore taskManagerMetricStore =
                        store.getTaskManagerMetricStore(taskmanager.getResourceIdString());
                if (taskManagerMetricStore != null) {
                    taskmanagerStores.add(taskManagerMetricStore);
                }
            }
            return taskmanagerStores;
        }
    }
}
