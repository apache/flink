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
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AggregatedSubtaskMetricsBatchResponseBody}. */
@ExtendWith(NoOpTestExtension.class)
class AggregatedSubtaskMetricsBatchResponseBodyTest
        extends RestResponseMarshallingTestBase<AggregatedSubtaskMetricsBatchResponseBody> {

    private static final JobVertexID VERTEX_ID = new JobVertexID();
    private static final String METRIC_ID = "abc.busyTimeMsPerSecond";

    @Override
    protected Class<AggregatedSubtaskMetricsBatchResponseBody> getTestResponseClass() {
        return AggregatedSubtaskMetricsBatchResponseBody.class;
    }

    @Override
    protected AggregatedSubtaskMetricsBatchResponseBody getTestResponseInstance() {
        return createResponse();
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            AggregatedSubtaskMetricsBatchResponseBody expected,
            AggregatedSubtaskMetricsBatchResponseBody actual) {
        assertThat(actual.getMetricsByVertex()).hasSize(1);

        final AggregatedSubtaskMetricsBatchResponseBody.VertexAggregatedMetrics vertexMetrics =
                actual.getMetricsByVertex().iterator().next();
        assertThat(vertexMetrics.getVertexId()).isEqualTo(VERTEX_ID);
        assertThat(vertexMetrics.getMetrics()).hasSize(1);

        final AggregatedMetric metric = vertexMetrics.getMetrics().iterator().next();
        assertThat(metric.getId()).isEqualTo(METRIC_ID);
        assertThat(metric.getMin()).isEqualTo(1.0);
        assertThat(metric.getMax()).isEqualTo(3.0);
        assertThat(metric.getAvg()).isEqualTo(2.0);
        assertThat(metric.getSum()).isEqualTo(4.0);
    }

    @Test
    void testSerializesAsTopLevelArray() throws Exception {
        final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
        final JsonNode rootNode =
                objectMapper.readTree(objectMapper.writeValueAsString(createResponse()));

        assertThat(rootNode.isArray()).isTrue();
        assertThat(rootNode).hasSize(1);
        assertThat(rootNode.get(0).has("vertexId")).isTrue();
        assertThat(rootNode.get(0).has("metrics")).isTrue();
    }

    private static AggregatedSubtaskMetricsBatchResponseBody createResponse() {
        return new AggregatedSubtaskMetricsBatchResponseBody(
                Collections.singletonList(
                        new AggregatedSubtaskMetricsBatchResponseBody.VertexAggregatedMetrics(
                                VERTEX_ID,
                                Collections.singletonList(
                                        new AggregatedMetric(
                                                METRIC_ID, 1.0, 3.0, 2.0, 4.0, null)))));
    }
}
