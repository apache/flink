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

package org.apache.flink.connector.testframe.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** The querier used to get job metrics by rest API. */
public class MetricQuerier {
    private static final Logger LOG = LoggerFactory.getLogger(MetricQuerier.class);
    private RestClient restClient;

    public MetricQuerier(Configuration configuration) throws ConfigurationException {
        restClient = new RestClient(configuration, Executors.newCachedThreadPool());
    }

    public static JobDetailsInfo getJobDetails(
            RestClient client, TestEnvironment.Endpoint endpoint, JobID jobId) throws Exception {
        String jmAddress = endpoint.getAddress();
        int jmPort = endpoint.getPort();

        final JobMessageParameters params = new JobMessageParameters();
        params.jobPathParameter.resolve(jobId);

        return client.sendRequest(
                        jmAddress,
                        jmPort,
                        JobDetailsHeaders.getInstance(),
                        params,
                        EmptyRequestBody.getInstance())
                .get(30, TimeUnit.SECONDS);
    }

    public AggregatedMetricsResponseBody getMetricList(
            TestEnvironment.Endpoint endpoint, JobID jobId, JobVertexID vertexId) throws Exception {
        AggregatedSubtaskMetricsParameters subtaskMetricsParameters =
                new AggregatedSubtaskMetricsParameters();
        Iterator<MessagePathParameter<?>> pathParams =
                subtaskMetricsParameters.getPathParameters().iterator();
        ((JobIDPathParameter) pathParams.next()).resolve(jobId);
        ((JobVertexIdPathParameter) pathParams.next()).resolve(vertexId);
        return restClient
                .sendRequest(
                        endpoint.getAddress(),
                        endpoint.getPort(),
                        AggregatedSubtaskMetricsHeaders.getInstance(),
                        subtaskMetricsParameters,
                        EmptyRequestBody.getInstance())
                .get(30, TimeUnit.SECONDS);
    }

    public AggregatedMetricsResponseBody getMetrics(
            TestEnvironment.Endpoint endpoint, JobID jobId, JobVertexID vertexId, String filters)
            throws Exception {
        AggregatedSubtaskMetricsParameters subtaskMetricsParameters =
                new AggregatedSubtaskMetricsParameters();
        Iterator<MessagePathParameter<?>> pathParams =
                subtaskMetricsParameters.getPathParameters().iterator();
        ((JobIDPathParameter) pathParams.next()).resolve(jobId);
        ((JobVertexIdPathParameter) pathParams.next()).resolve(vertexId);
        MetricsFilterParameter metricFilter =
                (MetricsFilterParameter)
                        subtaskMetricsParameters.getQueryParameters().iterator().next();
        metricFilter.resolveFromString(filters);

        return restClient
                .sendRequest(
                        endpoint.getAddress(),
                        endpoint.getPort(),
                        AggregatedSubtaskMetricsHeaders.getInstance(),
                        subtaskMetricsParameters,
                        EmptyRequestBody.getInstance())
                .get(30, TimeUnit.SECONDS);
    }

    public Double getAggregatedMetricsByRestAPI(
            TestEnvironment.Endpoint endpoint,
            JobID jobId,
            String sourceOrSinkName,
            String metricName,
            String filter)
            throws Exception {
        // get job details, including the vertex id
        JobDetailsInfo jobDetailsInfo = getJobDetails(restClient, endpoint, jobId);

        // get the vertex id for source/sink operator
        JobDetailsInfo.JobVertexDetailsInfo vertex =
                jobDetailsInfo.getJobVertexInfos().stream()
                        .filter(v -> v.getName().contains(sourceOrSinkName))
                        .findAny()
                        .orElse(null);
        assertThat(vertex).isNotNull();
        JobVertexID vertexId = vertex.getJobVertexID();

        // get the metric list
        AggregatedMetricsResponseBody metricsResponseBody =
                getMetricList(endpoint, jobId, vertexId);

        // get the metric query filters
        String queryParam =
                metricsResponseBody.getMetrics().stream()
                        .filter(
                                m ->
                                        filterByMetricName(
                                                m.getId(), sourceOrSinkName, metricName, filter))
                        .map(m -> m.getId())
                        .collect(Collectors.joining(","));

        if (StringUtils.isNullOrWhitespaceOnly(queryParam)) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot find metric[%s] for operator [%s].",
                            metricName, sourceOrSinkName));
        }

        AggregatedMetricsResponseBody metricsResponse =
                getMetrics(endpoint, jobId, vertexId, queryParam);

        Collection<AggregatedMetric> metrics = metricsResponse.getMetrics();
        if (metrics == null || metrics.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot find metric[%s] for operator [%s] with filter [%s].",
                            metricName, sourceOrSinkName, filter));
        }
        return metrics.iterator().next().getSum();
    }

    private boolean filterByMetricName(
            String metricName,
            String sourceOrSinkName,
            String targetMetricName,
            @Nullable String filter) {
        boolean filterByName =
                metricName.endsWith(targetMetricName) && metricName.contains(sourceOrSinkName);
        if (!StringUtils.isNullOrWhitespaceOnly(filter)) {
            return filterByName && metricName.contains(filter);
        }
        return filterByName;
    }
}
