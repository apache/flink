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

package org.apache.flink.streaming.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link LatencyStats}. */
class LatencyStatsTest {

    private static final OperatorID OPERATOR_ID = new OperatorID();
    private static final OperatorID SOURCE_ID_1 = new OperatorID();
    private static final OperatorID SOURCE_ID_2 = new OperatorID();

    private static final int OPERATOR_SUBTASK_INDEX = 64;

    private static final String PARENT_GROUP_NAME = "parent";

    @Test
    void testLatencyStatsSingle() {
        testLatencyStats(
                LatencyStats.Granularity.SINGLE,
                registrations -> {
                    assertThat(registrations).hasSize(1);

                    {
                        final Tuple2<String, Histogram> registration = registrations.get(0);
                        assertName(registration.f0);
                        assertThat(registration.f1.getCount()).isEqualTo(5);
                    }
                });
    }

    @Test
    void testLatencyStatsOperator() {
        testLatencyStats(
                LatencyStats.Granularity.OPERATOR,
                registrations -> {
                    assertThat(registrations).hasSize(2);

                    {
                        final Tuple2<String, Histogram> registration = registrations.get(0);
                        assertName(registration.f0, SOURCE_ID_1);
                        assertThat(registration.f1.getCount()).isEqualTo(3);
                    }

                    {
                        final Tuple2<String, Histogram> registration = registrations.get(1);
                        assertName(registration.f0, SOURCE_ID_2);
                        assertThat(registration.f1.getCount()).isEqualTo(2);
                    }
                });
    }

    @Test
    void testLatencyStatsSubtask() {
        testLatencyStats(
                LatencyStats.Granularity.SUBTASK,
                registrations -> {
                    assertThat(registrations).hasSize(4);

                    {
                        final Tuple2<String, Histogram> registration = registrations.get(0);
                        assertName(registration.f0, SOURCE_ID_1, 0);
                        assertThat(registration.f1.getCount()).isEqualTo(2);
                    }

                    {
                        final Tuple2<String, Histogram> registration = registrations.get(1);
                        assertName(registration.f0, SOURCE_ID_1, 1);
                        assertThat(registration.f1.getCount()).isOne();
                    }

                    {
                        final Tuple2<String, Histogram> registration = registrations.get(2);
                        assertName(registration.f0, SOURCE_ID_2, 2);
                        assertThat(registration.f1.getCount()).isOne();
                    }

                    {
                        final Tuple2<String, Histogram> registration = registrations.get(3);
                        assertName(registration.f0, SOURCE_ID_2, 3);
                        assertThat(registration.f1.getCount()).isOne();
                    }
                });
    }

    private static void testLatencyStats(
            final LatencyStats.Granularity granularity,
            final Consumer<List<Tuple2<String, Histogram>>> verifier) {

        final AbstractMetricGroup<?> dummyGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();

        final List<Tuple2<String, Histogram>> latencyHistograms = new ArrayList<>(4);
        final TestingMetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer(
                                (metric, metricName, group) -> {
                                    if (metric instanceof Histogram) {
                                        latencyHistograms.add(
                                                Tuple2.of(
                                                        group.getMetricIdentifier(metricName),
                                                        (Histogram) metric));
                                    }
                                })
                        .build();
        final MetricGroup parentGroup =
                new GenericMetricGroup(registry, dummyGroup, PARENT_GROUP_NAME);

        final LatencyStats latencyStats =
                new LatencyStats(
                        parentGroup,
                        MetricOptions.LATENCY_HISTORY_SIZE.defaultValue(),
                        OPERATOR_SUBTASK_INDEX,
                        OPERATOR_ID,
                        granularity);

        latencyStats.reportLatency(new LatencyMarker(0L, SOURCE_ID_1, 0));
        latencyStats.reportLatency(new LatencyMarker(0L, SOURCE_ID_1, 0));
        latencyStats.reportLatency(new LatencyMarker(0L, SOURCE_ID_1, 1));
        latencyStats.reportLatency(new LatencyMarker(0L, SOURCE_ID_2, 2));
        latencyStats.reportLatency(new LatencyMarker(0L, SOURCE_ID_2, 3));

        verifier.accept(latencyHistograms);
    }

    /** Removes all parts from the metric identifier preceding the latency-related parts. */
    private static String sanitizeName(final String registrationName) {
        return registrationName.substring(
                registrationName.lastIndexOf(PARENT_GROUP_NAME) + PARENT_GROUP_NAME.length() + 1);
    }

    private static void assertName(final String registrationName) {
        final String sanitizedName = sanitizeName(registrationName);
        assertThat(sanitizedName)
                .isEqualTo(
                        "operator_id."
                                + OPERATOR_ID
                                + ".operator_subtask_index."
                                + OPERATOR_SUBTASK_INDEX
                                + ".latency");
    }

    private static void assertName(final String registrationName, final OperatorID sourceId) {
        final String sanitizedName = sanitizeName(registrationName);
        assertThat(sanitizedName)
                .isEqualTo(
                        "source_id."
                                + sourceId
                                + ".operator_id."
                                + OPERATOR_ID
                                + ".operator_subtask_index."
                                + OPERATOR_SUBTASK_INDEX
                                + ".latency");
    }

    private static void assertName(
            final String registrationName, final OperatorID sourceId, final int sourceIndex) {
        final String sanitizedName = sanitizeName(registrationName);
        assertThat(sanitizedName)
                .isEqualTo(
                        "source_id."
                                + sourceId
                                + ".source_subtask_index."
                                + sourceIndex
                                + ".operator_id."
                                + OPERATOR_ID
                                + ".operator_subtask_index."
                                + OPERATOR_SUBTASK_INDEX
                                + ".latency");
    }
}
