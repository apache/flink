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
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexMetricsMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.Map;

/**
 * Handler that returns metrics given a {@link JobID} and {@link JobVertexID}.
 *
 * @see MetricStore#getTaskMetricStore(String, String)
 * @deprecated This class is subsumed by {@link SubtaskMetricsHandler} and is only kept for
 *     backwards-compatibility.
 */
public class JobVertexMetricsHandler
        extends AbstractMetricsHandler<JobVertexMetricsMessageParameters> {

    public JobVertexMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> headers,
            MetricFetcher metricFetcher) {

        super(
                leaderRetriever,
                timeout,
                headers,
                JobVertexMetricsHeaders.getInstance(),
                metricFetcher);
    }

    @Override
    protected MetricStore.ComponentMetricStore getComponentMetricStore(
            HandlerRequest<EmptyRequestBody, JobVertexMetricsMessageParameters> request,
            MetricStore metricStore) {

        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        final JobVertexID vertexId = request.getPathParameter(JobVertexIdPathParameter.class);

        return metricStore.getTaskMetricStore(jobId.toString(), vertexId.toString());
    }
}
