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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.util.TestReporter;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the registration of groups and metrics on a {@link MetricGroup}. */
class MetricGroupRegistrationTest {
    /** Verifies that group methods instantiate the correct metric with the given name. */
    @Test
    void testMetricInstantiation() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(
                                ReporterSetup.forReporter("test", new TestReporter1())));

        MetricGroup root =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        Counter counter = root.counter("counter");
        assertThat(TestReporter1.lastPassedMetric).isEqualTo(counter);
        assertThat(TestReporter1.lastPassedName).isEqualTo("counter");

        Gauge<Object> gauge =
                root.gauge(
                        "gauge",
                        new Gauge<Object>() {
                            @Override
                            public Object getValue() {
                                return null;
                            }
                        });

        assertThat(TestReporter1.lastPassedMetric).isEqualTo(gauge);
        assertThat(TestReporter1.lastPassedName).isEqualTo("gauge");

        Histogram histogram =
                root.histogram(
                        "histogram",
                        new Histogram() {
                            @Override
                            public void update(long value) {}

                            @Override
                            public long getCount() {
                                return 0;
                            }

                            @Override
                            public HistogramStatistics getStatistics() {
                                return null;
                            }
                        });

        assertThat(TestReporter1.lastPassedMetric).isEqualTo(histogram);
        assertThat(TestReporter1.lastPassedName).isEqualTo("histogram");
        registry.closeAsync().get();
    }

    /** Reporter that exposes the last name and metric instance it was notified of. */
    public static class TestReporter1 extends TestReporter {

        public static Metric lastPassedMetric;
        public static String lastPassedName;

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            lastPassedMetric = metric;
            lastPassedName = metricName;
        }
    }

    /**
     * Verifies that when attempting to create a group with the name of an existing one the existing
     * one will be returned instead.
     */
    @Test
    void testDuplicateGroupName() throws Exception {
        Configuration config = new Configuration();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(config));

        MetricGroup root =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        MetricGroup group1 = root.addGroup("group");
        MetricGroup group2 = root.addGroup("group");
        MetricGroup group3 = root.addGroup("group");
        assertThat(group1 == group2 && group2 == group3).isTrue();

        registry.closeAsync().get();
    }
}
