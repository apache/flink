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

package org.apache.flink.runtime.metrics.dump;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link MetricQueryService}. */
public class MetricQueryServiceTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(1);

    private static TestingRpcService rpcService;

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @After
    public void teardown() {
        rpcService.clearGateways();
    }

    @AfterClass
    public static void teardownClass() {
        if (rpcService != null) {
            rpcService.stopService();
            rpcService = null;
        }
    }

    @Test
    public void testCreateDump() throws Exception {
        MetricQueryService queryService =
                MetricQueryService.createMetricQueryService(
                        rpcService, ResourceID.generate(), Long.MAX_VALUE);
        queryService.start();

        final Counter c = new SimpleCounter();
        final Gauge<String> g = () -> "Hello";
        final Histogram h = new TestHistogram();
        final Meter m = new TestMeter();

        final TaskManagerMetricGroup tm =
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

        queryService.addMetric("counter", c, tm);
        queryService.addMetric("gauge", g, tm);
        queryService.addMetric("histogram", h, tm);
        queryService.addMetric("meter", m, tm);

        MetricDumpSerialization.MetricSerializationResult dump =
                queryService.queryMetrics(TIMEOUT).get();

        assertTrue(dump.serializedCounters.length > 0);
        assertTrue(dump.serializedGauges.length > 0);
        assertTrue(dump.serializedHistograms.length > 0);
        assertTrue(dump.serializedMeters.length > 0);

        queryService.removeMetric(c);
        queryService.removeMetric(g);
        queryService.removeMetric(h);
        queryService.removeMetric(m);

        MetricDumpSerialization.MetricSerializationResult emptyDump =
                queryService.queryMetrics(TIMEOUT).get();

        assertEquals(0, emptyDump.serializedCounters.length);
        assertEquals(0, emptyDump.serializedGauges.length);
        assertEquals(0, emptyDump.serializedHistograms.length);
        assertEquals(0, emptyDump.serializedMeters.length);
    }

    @Test
    public void testHandleOversizedMetricMessage() throws Exception {
        final long sizeLimit = 200L;
        MetricQueryService queryService =
                MetricQueryService.createMetricQueryService(
                        rpcService, ResourceID.generate(), sizeLimit);
        queryService.start();

        final TaskManagerMetricGroup tm =
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

        final String gaugeValue = "Hello";
        final long requiredGaugesToExceedLimit = sizeLimit / gaugeValue.length() + 1;
        List<Tuple2<String, Gauge<String>>> gauges =
                LongStream.range(0, requiredGaugesToExceedLimit)
                        .mapToObj(x -> Tuple2.of("gauge" + x, (Gauge<String>) () -> "Hello" + x))
                        .collect(Collectors.toList());
        gauges.forEach(gauge -> queryService.addMetric(gauge.f0, gauge.f1, tm));

        queryService.addMetric("counter", new SimpleCounter(), tm);
        queryService.addMetric("histogram", new TestHistogram(), tm);
        queryService.addMetric("meter", new TestMeter(), tm);

        MetricDumpSerialization.MetricSerializationResult dump =
                queryService.queryMetrics(TIMEOUT).get();

        assertTrue(dump.serializedCounters.length > 0);
        assertEquals(1, dump.numCounters);
        assertTrue(dump.serializedMeters.length > 0);
        assertEquals(1, dump.numMeters);

        // gauges exceeded the size limit and will be excluded
        assertEquals(0, dump.serializedGauges.length);
        assertEquals(0, dump.numGauges);

        assertTrue(dump.serializedHistograms.length > 0);
        assertEquals(1, dump.numHistograms);

        // unregister all but one gauge to ensure gauges are reported again if the remaining fit
        for (int x = 1; x < gauges.size(); x++) {
            queryService.removeMetric(gauges.get(x).f1);
        }

        MetricDumpSerialization.MetricSerializationResult recoveredDump =
                queryService.queryMetrics(TIMEOUT).get();

        assertTrue(recoveredDump.serializedCounters.length > 0);
        assertEquals(1, recoveredDump.numCounters);
        assertTrue(recoveredDump.serializedMeters.length > 0);
        assertEquals(1, recoveredDump.numMeters);
        assertTrue(recoveredDump.serializedGauges.length > 0);
        assertEquals(1, recoveredDump.numGauges);
        assertTrue(recoveredDump.serializedHistograms.length > 0);
        assertEquals(1, recoveredDump.numHistograms);
    }
}
