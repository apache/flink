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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link MetricQueryService}. */
class MetricQueryServiceTest {

    private static final Time TIMEOUT = Time.seconds(1);

    private static TestingRpcService rpcService;

    @BeforeAll
    static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @AfterEach
    void teardown() {
        rpcService.clearGateways();
    }

    @AfterAll
    static void teardownClass() {
        if (rpcService != null) {
            rpcService.closeAsync();
            rpcService = null;
        }
    }

    @Test
    void testCreateDump() throws Exception {
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

        assertThat(dump.serializedCounters).isNotEmpty();
        assertThat(dump.serializedGauges).isNotEmpty();
        assertThat(dump.serializedHistograms).isNotEmpty();
        assertThat(dump.serializedMeters).isNotEmpty();

        queryService.removeMetric(c);
        queryService.removeMetric(g);
        queryService.removeMetric(h);
        queryService.removeMetric(m);

        MetricDumpSerialization.MetricSerializationResult emptyDump =
                queryService.queryMetrics(TIMEOUT).get();

        assertThat(emptyDump.serializedCounters).isEmpty();
        assertThat(emptyDump.serializedGauges).isEmpty();
        assertThat(emptyDump.serializedHistograms).isEmpty();
        assertThat(emptyDump.serializedMeters).isEmpty();
    }

    @Test
    void testHandleOversizedMetricMessage() throws Exception {
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

        assertThat(dump.serializedCounters).isNotEmpty();
        assertThat(dump.numCounters).isOne();
        assertThat(dump.serializedMeters).isNotEmpty();
        assertThat(dump.numMeters).isOne();

        // gauges exceeded the size limit and will be excluded
        assertThat(dump.serializedGauges).isEmpty();
        assertThat(dump.numGauges).isZero();

        assertThat(dump.serializedHistograms).isNotEmpty();
        assertThat(dump.numHistograms).isOne();

        // unregister all but one gauge to ensure gauges are reported again if the remaining fit
        for (int x = 1; x < gauges.size(); x++) {
            queryService.removeMetric(gauges.get(x).f1);
        }

        MetricDumpSerialization.MetricSerializationResult recoveredDump =
                queryService.queryMetrics(TIMEOUT).get();

        assertThat(recoveredDump.serializedCounters).isNotEmpty();
        assertThat(recoveredDump.numCounters).isOne();
        assertThat(recoveredDump.serializedMeters).isNotEmpty();
        assertThat(recoveredDump.numMeters).isOne();
        assertThat(recoveredDump.serializedGauges).isNotEmpty();
        assertThat(recoveredDump.numGauges).isOne();
        assertThat(recoveredDump.serializedHistograms).isNotEmpty();
        assertThat(recoveredDump.numHistograms).isOne();
    }
}
