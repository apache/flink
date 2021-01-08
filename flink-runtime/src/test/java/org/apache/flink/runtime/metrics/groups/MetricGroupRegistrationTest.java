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
import org.apache.flink.metrics.*;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the registration of groups and metrics on a {@link MetricGroup}. */
public class MetricGroupRegistrationTest extends TestLogger {
    /** Verifies that group methods instantiate the correct metric with the given name. */
    @Test
    public void testMetricInstantiation() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryConfiguration.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(
                                ReporterSetup.forReporter("test", new TestReporter1())));

        MetricGroup root = new TaskManagerMetricGroup(registry, "host", "id");

        Counter counter = root.counter("counter");
        assertEquals(counter, TestReporter1.lastPassedMetric);
        assertEquals("counter", TestReporter1.lastPassedName);

        Gauge<Object> gauge =
                root.gauge(
                        "gauge",
                        new Gauge<Object>() {
                            @Override
                            public Object getValue() {
                                return null;
                            }
                        });

        Assertions.assertEquals(gauge, TestReporter1.lastPassedMetric);
        assertEquals("gauge", TestReporter1.lastPassedName);

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

        Assertions.assertEquals(histogram, TestReporter1.lastPassedMetric);
        assertEquals("histogram", TestReporter1.lastPassedName);
        registry.shutdown().get();
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
    public void testDuplicateGroupName() throws Exception {
        Configuration config = new Configuration();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));

        MetricGroup root = new TaskManagerMetricGroup(registry, "host", "id");

        MetricGroup group1 = root.addGroup("group");
        MetricGroup group2 = root.addGroup("group");
        MetricGroup group3 = root.addGroup("group");
        Assertions.assertTrue(group1 == group2 && group2 == group3);

        registry.shutdown().get();
    }
}
