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

package org.apache.flink.metrics.influxdb;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.util.TestMetricGroup;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/** Test for {@link MeasurementInfoProvider}. */
public class MeasurementInfoProviderTest extends TestLogger {
    private final MeasurementInfoProvider provider = new MeasurementInfoProvider();

    @Test
    public void simpleTestGetMetricInfo() {
        String logicalScope = "myService.Status.JVM.ClassLoader";
        Map<String, String> variables = new HashMap<>();
        variables.put("<A>", "a");
        variables.put("<B>", "b");
        variables.put("<C>", "c");
        String metricName = "ClassesLoaded";

        final MetricGroup metricGroup =
                TestMetricGroup.newBuilder()
                        .setVariables(variables)
                        .setLogicalScopeFunction((characterFilter, character) -> logicalScope)
                        .build();

        MeasurementInfo info = provider.getMetricInfo(metricName, metricGroup);
        assertNotNull(info);
        assertEquals(
                String.join("" + MeasurementInfoProvider.SCOPE_SEPARATOR, logicalScope, metricName),
                info.getName());
        assertThat(info.getTags(), hasEntry("A", "a"));
        assertThat(info.getTags(), hasEntry("B", "b"));
        assertThat(info.getTags(), hasEntry("C", "c"));
        assertEquals(3, info.getTags().size());
    }

    @Test
    public void testNormalizingTags() {
        Map<String, String> variables = new HashMap<>();
        variables.put("<A\n>", "a\n");

        final MetricGroup metricGroup =
                TestMetricGroup.newBuilder().setVariables(variables).build();

        MeasurementInfo info = provider.getMetricInfo("m1", metricGroup);
        assertThat(info.getTags(), hasEntry("A", "a"));
    }
}
