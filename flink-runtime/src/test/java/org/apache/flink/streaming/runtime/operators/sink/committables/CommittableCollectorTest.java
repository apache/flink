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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricsGroupTestUtils;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CommittableCollectorTest {
    private static final SinkCommitterMetricGroup METRIC_GROUP =
            spy(MetricsGroupTestUtils.mockCommitterMetricGroup());

    @BeforeEach
    void setUp() {
        Mockito.reset(METRIC_GROUP);
    }

    @Test
    void testGetCheckpointCommittablesUpTo() {
        final CommittableCollector<Integer> committableCollector =
                new CommittableCollector<>(METRIC_GROUP, true);
        CommittableSummary<Integer> first = new CommittableSummary<>(1, 1, 1L, 1, 0);
        committableCollector.addMessage(first);
        CommittableSummary<Integer> second = new CommittableSummary<>(1, 1, 2L, 1, 0);
        committableCollector.addMessage(second);
        committableCollector.addMessage(new CommittableSummary<>(1, 1, 3L, 1, 0));

        assertThat(committableCollector.getCheckpointCommittablesUpTo(2)).hasSize(2);
    }

    @Test
    void testCopyCommittableCollectorDoesNotTriggerMetricUpdates() {
        final CommittableCollector<Integer> committableCollector =
                new CommittableCollector<>(METRIC_GROUP, true);

        // before
        verify(METRIC_GROUP, times(1)).setCurrentPendingCommittablesGauge(Mockito.any());

        committableCollector.copy();

        // after
        verify(METRIC_GROUP, times(1)).setCurrentPendingCommittablesGauge(Mockito.any());
    }
}
