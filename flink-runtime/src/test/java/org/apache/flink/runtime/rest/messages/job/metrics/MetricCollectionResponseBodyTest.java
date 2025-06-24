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

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MetricCollectionResponseBody}. */
@ExtendWith(NoOpTestExtension.class)
class MetricCollectionResponseBodyTest
        extends RestResponseMarshallingTestBase<MetricCollectionResponseBody> {

    private static final String TEST_METRIC_NAME = "metric1";

    private static final String TEST_METRIC_VALUE = "1000";

    @Override
    protected Class<MetricCollectionResponseBody> getTestResponseClass() {
        return MetricCollectionResponseBody.class;
    }

    @Override
    protected MetricCollectionResponseBody getTestResponseInstance() {
        return new MetricCollectionResponseBody(
                Collections.singleton(new Metric(TEST_METRIC_NAME, TEST_METRIC_VALUE)));
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            MetricCollectionResponseBody expected, MetricCollectionResponseBody actual) {

        assertThat(actual.getMetrics()).hasSize(1);

        final Metric metric = actual.getMetrics().iterator().next();
        assertThat(metric.getId()).isEqualTo(TEST_METRIC_NAME);
        assertThat(metric.getValue()).isEqualTo(TEST_METRIC_VALUE);
    }

    @Test
    void testNullValueNotSerialized() throws Exception {
        final String json =
                RestMapperUtils.getStrictObjectMapper()
                        .writeValueAsString(
                                new MetricCollectionResponseBody(
                                        Collections.singleton(new Metric(TEST_METRIC_NAME))));

        assertThat(json).doesNotContain("\"value\"");
        assertThat(json).doesNotContain("\"metrics\"");
    }
}
