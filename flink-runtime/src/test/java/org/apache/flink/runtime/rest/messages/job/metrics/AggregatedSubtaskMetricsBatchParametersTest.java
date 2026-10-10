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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for batch aggregated subtask metrics query parameters. */
class AggregatedSubtaskMetricsBatchParametersTest {

    @Test
    void testMetricNamesQueryParameters() throws Exception {
        final JobVertexID vertexId1 = new JobVertexID();
        final JobVertexID vertexId2 = new JobVertexID();
        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        AggregatedSubtaskMetricsNamesHeaders.getInstance()
                                .getUnresolvedMessageParameters(),
                        pathParameters(),
                        queryParameters(
                                "vertices", vertexId1 + "," + vertexId2, "regex", ".*busyTime.*"),
                        Collections.emptyList());

        assertThat(request.getQueryParameter(JobVerticesFilterQueryParameter.class))
                .containsExactly(vertexId1, vertexId2);
        assertThat(request.getQueryParameter(MetricsRegexFilterParameter.class))
                .containsExactly(".*busyTime.*");
    }

    @Test
    void testMetricValuesQueryParameters() throws Exception {
        final JobVertexID vertexId1 = new JobVertexID();
        final JobVertexID vertexId2 = new JobVertexID();
        final HandlerRequest<EmptyRequestBody> request =
                HandlerRequest.resolveParametersAndCreate(
                        EmptyRequestBody.getInstance(),
                        AggregatedSubtaskMetricsValuesHeaders.getInstance()
                                .getUnresolvedMessageParameters(),
                        pathParameters(),
                        queryParameters(
                                "vertices",
                                vertexId1 + "," + vertexId2,
                                "get",
                                "abc.busyTimeMsPerSecond,abc.numRecordsInPerSecond",
                                "agg",
                                "min,max"),
                        Collections.emptyList());

        assertThat(request.getQueryParameter(JobVerticesFilterQueryParameter.class))
                .containsExactly(vertexId1, vertexId2);
        assertThat(request.getQueryParameter(MetricsFilterParameter.class))
                .containsExactly("abc.busyTimeMsPerSecond", "abc.numRecordsInPerSecond");
        assertThat(request.getQueryParameter(MetricsAggregationParameter.class))
                .containsExactly(
                        MetricsAggregationParameter.AggregationMode.MIN,
                        MetricsAggregationParameter.AggregationMode.MAX);
    }

    private static Map<String, String> pathParameters() {
        return Collections.singletonMap(JobIDPathParameter.KEY, "00000000000000000000000000000000");
    }

    private static Map<String, List<String>> queryParameters(String... keyValues) {
        final Map<String, List<String>> queryParameters = new HashMap<>();
        for (int index = 0; index < keyValues.length; index += 2) {
            queryParameters.put(keyValues[index], Collections.singletonList(keyValues[index + 1]));
        }
        return queryParameters;
    }
}
