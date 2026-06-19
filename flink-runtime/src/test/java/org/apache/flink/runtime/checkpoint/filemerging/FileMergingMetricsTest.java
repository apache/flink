/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.filemerging;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileMergingMetricGroup}. */
class FileMergingMetricsTest {
    @Test
    void testMetricsRegistration() {
        final Collection<String> registeredGaugeNames = new ArrayList<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        if (gauge != null) {
                            registeredGaugeNames.add(name);
                        }
                        return gauge;
                    }
                };
        FileMergingSnapshotManager.SpaceStat spaceStat = new FileMergingSnapshotManager.SpaceStat();
        FileMergingMetricGroup fileMergingMetricGroup =
                new FileMergingMetricGroup(metricGroup, spaceStat);

        assertThat(registeredGaugeNames)
                .containsAll(
                        Arrays.asList(
                                FileMergingMetricGroup.LOGICAL_FILE_COUNT,
                                FileMergingMetricGroup.LOGICAL_FILE_SIZE,
                                FileMergingMetricGroup.PHYSICAL_FILE_COUNT,
                                FileMergingMetricGroup.PHYSICAL_FILE_SIZE));
        assertThat(registeredGaugeNames.size()).isEqualTo(4);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMetricsAreUpdated() {
        final Map<String, Gauge<?>> registeredGauges = new HashMap<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        registeredGauges.put(name, gauge);
                        return gauge;
                    }
                };
        FileMergingSnapshotManager.SpaceStat spaceStat = new FileMergingSnapshotManager.SpaceStat();
        FileMergingMetricGroup fileMergingMetricGroup =
                new FileMergingMetricGroup(metricGroup, spaceStat);
        Gauge<Long> logicalFileCountGauge =
                (Gauge<Long>) registeredGauges.get(FileMergingMetricGroup.LOGICAL_FILE_COUNT);
        Gauge<Long> logicalFileSizeGauge =
                (Gauge<Long>) registeredGauges.get(FileMergingMetricGroup.LOGICAL_FILE_SIZE);
        Gauge<Long> physicalFileCountGauge =
                (Gauge<Long>) registeredGauges.get(FileMergingMetricGroup.PHYSICAL_FILE_COUNT);
        Gauge<Long> physicalFileSizeGauge =
                (Gauge<Long>) registeredGauges.get(FileMergingMetricGroup.PHYSICAL_FILE_SIZE);

        assertThat(logicalFileCountGauge.getValue()).isEqualTo(0L);
        assertThat(logicalFileSizeGauge.getValue()).isEqualTo(0L);
        assertThat(physicalFileCountGauge.getValue()).isEqualTo(0L);
        assertThat(physicalFileSizeGauge.getValue()).isEqualTo(0L);

        // update space stat
        spaceStat.onLogicalFileCreate(100L);
        spaceStat.onPhysicalFileCreate();
        spaceStat.onPhysicalFileUpdate(100L);
        assertThat(logicalFileCountGauge.getValue()).isEqualTo(1L);
        assertThat(logicalFileSizeGauge.getValue()).isEqualTo(100L);
        assertThat(physicalFileCountGauge.getValue()).isEqualTo(1L);
        assertThat(physicalFileSizeGauge.getValue()).isEqualTo(100L);
    }
}
