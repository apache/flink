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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_COUNTER;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_GAUGE;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_HISTOGRAM;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_METER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link MetricDumpSerialization}. */
class MetricDumpSerializerTest {
    @Test
    void testNullGaugeHandling() throws IOException {
        MetricDumpSerialization.MetricDumpSerializer serializer =
                new MetricDumpSerialization.MetricDumpSerializer();
        MetricDumpSerialization.MetricDumpDeserializer deserializer =
                new MetricDumpSerialization.MetricDumpDeserializer();

        Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();

        gauges.put(
                new Gauge<Object>() {
                    @Override
                    public Object getValue() {
                        return null;
                    }
                },
                new Tuple2<QueryScopeInfo, String>(
                        new QueryScopeInfo.JobManagerQueryScopeInfo("A"), "g"));

        MetricDumpSerialization.MetricSerializationResult output =
                serializer.serialize(
                        Collections.<Counter, Tuple2<QueryScopeInfo, String>>emptyMap(),
                        gauges,
                        Collections.<Histogram, Tuple2<QueryScopeInfo, String>>emptyMap(),
                        Collections.<Meter, Tuple2<QueryScopeInfo, String>>emptyMap());

        // no metrics should be serialized
        assertThat(output.serializedCounters).isEmpty();
        assertThat(output.serializedGauges).isEmpty();
        assertThat(output.serializedHistograms).isEmpty();
        assertThat(output.serializedMeters).isEmpty();
        List<MetricDump> deserialized = deserializer.deserialize(output);
        assertThat(deserialized).isEmpty();
    }

    @Test
    void testJavaSerialization() throws IOException {
        MetricDumpSerialization.MetricDumpSerializer serializer =
                new MetricDumpSerialization.MetricDumpSerializer();

        final ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        final ObjectOutputStream oos = new ObjectOutputStream(bos);

        oos.writeObject(
                serializer.serialize(
                        new HashMap<Counter, Tuple2<QueryScopeInfo, String>>(),
                        new HashMap<Gauge<?>, Tuple2<QueryScopeInfo, String>>(),
                        new HashMap<Histogram, Tuple2<QueryScopeInfo, String>>(),
                        new HashMap<Meter, Tuple2<QueryScopeInfo, String>>()));
    }

    @Test
    void testSerialization() {
        MetricDumpSerialization.MetricDumpSerializer serializer =
                new MetricDumpSerialization.MetricDumpSerializer();
        MetricDumpSerialization.MetricDumpDeserializer deserializer =
                new MetricDumpSerialization.MetricDumpDeserializer();

        Map<Counter, Tuple2<QueryScopeInfo, String>> counters = new HashMap<>();
        Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();
        Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms = new HashMap<>();
        Map<Meter, Tuple2<QueryScopeInfo, String>> meters = new HashMap<>();

        SimpleCounter c1 = new SimpleCounter();
        SimpleCounter c2 = new SimpleCounter();
        SimpleCounter c3 = new SimpleCounter();

        c1.inc(1);
        c2.inc(2);
        c3.inc(3);

        Gauge<Integer> g1 =
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return 4;
                    }
                };

        Histogram h1 = new TestHistogram();

        Meter m1 =
                new Meter() {
                    @Override
                    public void markEvent() {}

                    @Override
                    public void markEvent(long n) {}

                    @Override
                    public double getRate() {
                        return 5;
                    }

                    @Override
                    public long getCount() {
                        return 10;
                    }
                };

        counters.put(c1, new Tuple2<>(new QueryScopeInfo.JobManagerQueryScopeInfo("A"), "c1"));
        counters.put(
                c2, new Tuple2<>(new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid", "B"), "c2"));
        counters.put(
                c3,
                new Tuple2<>(
                        new QueryScopeInfo.JobManagerOperatorQueryScopeInfo(
                                "job-id", "tmid", "opName", "C"),
                        "c3"));
        meters.put(m1, new Tuple2<>(new QueryScopeInfo.JobQueryScopeInfo("jid", "C"), "c3"));
        gauges.put(
                g1,
                new Tuple2<>(new QueryScopeInfo.TaskQueryScopeInfo("jid", "vid", 2, 0, "D"), "g1"));
        histograms.put(
                h1,
                new Tuple2<>(
                        new QueryScopeInfo.OperatorQueryScopeInfo(
                                "jid", "vid", 2, 0, "opname", "E"),
                        "h1"));

        MetricDumpSerialization.MetricSerializationResult serialized =
                serializer.serialize(counters, gauges, histograms, meters);
        List<MetricDump> deserialized = deserializer.deserialize(serialized);

        // ===== Counters
        // ==============================================================================================
        assertThat(deserialized).hasSize(6);

        for (MetricDump metric : deserialized) {
            switch (metric.getCategory()) {
                case METRIC_CATEGORY_COUNTER:
                    MetricDump.CounterDump counterDump = (MetricDump.CounterDump) metric;
                    switch ((byte) counterDump.count) {
                        case 1:
                            assertThat(counterDump.scopeInfo)
                                    .isInstanceOf(QueryScopeInfo.JobManagerQueryScopeInfo.class);
                            assertThat(counterDump.scopeInfo.scope).isEqualTo("A");
                            assertThat(counterDump.name).isEqualTo("c1");
                            counters.remove(c1);
                            break;
                        case 2:
                            assertThat(counterDump.scopeInfo)
                                    .isInstanceOf(QueryScopeInfo.TaskManagerQueryScopeInfo.class);
                            assertThat(counterDump.scopeInfo.scope).isEqualTo("B");
                            assertThat(counterDump.name).isEqualTo("c2");
                            assertThat("tmid")
                                    .isEqualTo(
                                            ((QueryScopeInfo.TaskManagerQueryScopeInfo)
                                                            counterDump.scopeInfo)
                                                    .taskManagerID);
                            counters.remove(c2);
                            break;
                        case 3:
                            assertThat(counterDump.scopeInfo)
                                    .isInstanceOf(
                                            QueryScopeInfo.JobManagerOperatorQueryScopeInfo.class);
                            assertThat(counterDump.scopeInfo.scope).isEqualTo("C");
                            assertThat(counterDump.name).isEqualTo("c3");
                            QueryScopeInfo.JobManagerOperatorQueryScopeInfo
                                    jmOperatorQueryScopeInfo =
                                            ((QueryScopeInfo.JobManagerOperatorQueryScopeInfo)
                                                    counterDump.scopeInfo);
                            assertThat(jmOperatorQueryScopeInfo.jobID).isEqualTo("job-id");
                            assertThat(jmOperatorQueryScopeInfo.vertexID).isEqualTo("tmid");
                            assertThat(jmOperatorQueryScopeInfo.operatorName).isEqualTo("opName");
                            counters.remove(c3);
                            break;
                        default:
                            fail("Unexpected counter count.");
                    }
                    break;
                case METRIC_CATEGORY_GAUGE:
                    MetricDump.GaugeDump gaugeDump = (MetricDump.GaugeDump) metric;
                    assertThat(gaugeDump.value).isEqualTo("4");
                    assertThat(gaugeDump.name).isEqualTo("g1");

                    assertThat(gaugeDump.scopeInfo)
                            .isInstanceOf(QueryScopeInfo.TaskQueryScopeInfo.class);
                    QueryScopeInfo.TaskQueryScopeInfo taskInfo =
                            (QueryScopeInfo.TaskQueryScopeInfo) gaugeDump.scopeInfo;
                    assertThat(taskInfo.scope).isEqualTo("D");
                    assertThat(taskInfo.jobID).isEqualTo("jid");
                    assertThat(taskInfo.vertexID).isEqualTo("vid");
                    assertThat(taskInfo.subtaskIndex).isEqualTo(2);
                    gauges.remove(g1);
                    break;
                case METRIC_CATEGORY_HISTOGRAM:
                    MetricDump.HistogramDump histogramDump = (MetricDump.HistogramDump) metric;
                    assertThat(histogramDump.name).isEqualTo("h1");
                    assertThat(histogramDump.median).isCloseTo(0.5, Offset.offset(0.1));
                    assertThat(histogramDump.p75).isCloseTo(0.75, Offset.offset(0.1));
                    assertThat(histogramDump.p90).isCloseTo(0.9, Offset.offset(0.1));
                    assertThat(histogramDump.p95).isCloseTo(0.95, Offset.offset(0.1));
                    assertThat(histogramDump.p98).isCloseTo(0.98, Offset.offset(0.1));
                    assertThat(histogramDump.p99).isCloseTo(0.99, Offset.offset(0.1));
                    assertThat(histogramDump.p999).isCloseTo(0.999, Offset.offset(0.1));
                    assertThat(histogramDump.mean).isCloseTo(4, Offset.offset(0.1));
                    assertThat(histogramDump.stddev).isCloseTo(5, Offset.offset(0.1));
                    assertThat(histogramDump.max).isEqualTo(6);
                    assertThat(histogramDump.min).isEqualTo(7);

                    assertThat(histogramDump.scopeInfo)
                            .isInstanceOf(QueryScopeInfo.OperatorQueryScopeInfo.class);
                    QueryScopeInfo.OperatorQueryScopeInfo opInfo =
                            (QueryScopeInfo.OperatorQueryScopeInfo) histogramDump.scopeInfo;
                    assertThat(opInfo.scope).isEqualTo("E");
                    assertThat(opInfo.jobID).isEqualTo("jid");
                    assertThat(opInfo.vertexID).isEqualTo("vid");
                    assertThat(opInfo.subtaskIndex).isEqualTo(2);
                    assertThat(opInfo.operatorName).isEqualTo("opname");
                    histograms.remove(h1);
                    break;
                case METRIC_CATEGORY_METER:
                    MetricDump.MeterDump meterDump = (MetricDump.MeterDump) metric;
                    assertThat(meterDump.rate).isCloseTo(5.0, Offset.offset(0.1));

                    assertThat(meterDump.scopeInfo)
                            .isInstanceOf(QueryScopeInfo.JobQueryScopeInfo.class);
                    assertThat(meterDump.scopeInfo.scope).isEqualTo("C");
                    assertThat(meterDump.name).isEqualTo("c3");
                    assertThat(((QueryScopeInfo.JobQueryScopeInfo) meterDump.scopeInfo).jobID)
                            .isEqualTo("jid");
                    break;
                default:
                    fail("Unexpected metric type: " + metric.getCategory());
            }
        }
        assertThat(counters).isEmpty();
        assertThat(gauges).isEmpty();
        assertThat(histograms).isEmpty();
    }
}
