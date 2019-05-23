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

import org.junit.Assert;
import org.junit.Test;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link MetricDumpSerialization}.
 */
public class MetricDumpSerializerTest {
	@Test
	public void testNullGaugeHandling() throws IOException {
		MetricDumpSerialization.MetricDumpSerializer serializer = new MetricDumpSerialization.MetricDumpSerializer();
		MetricDumpSerialization.MetricDumpDeserializer deserializer = new MetricDumpSerialization.MetricDumpDeserializer();

		Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();

		gauges.put(new Gauge<Object>() {
			@Override
			public Object getValue() {
				return null;
			}
		}, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.JobManagerQueryScopeInfo("A"), "g"));

		MetricDumpSerialization.MetricSerializationResult output = serializer.serialize(
			Collections.<Counter, Tuple2<QueryScopeInfo, String>>emptyMap(),
			gauges,
			Collections.<Histogram, Tuple2<QueryScopeInfo, String>>emptyMap(),
			Collections.<Meter, Tuple2<QueryScopeInfo, String>>emptyMap());

		// no metrics should be serialized
		Assert.assertEquals(0, output.serializedCounters.length);
		Assert.assertEquals(0, output.serializedGauges.length);
		Assert.assertEquals(0, output.serializedHistograms.length);
		Assert.assertEquals(0, output.serializedMeters.length);

		List<MetricDump> deserialized = deserializer.deserialize(output);
		Assert.assertEquals(0, deserialized.size());
	}

	@Test
	public void testJavaSerialization() throws IOException {
		MetricDumpSerialization.MetricDumpSerializer serializer = new MetricDumpSerialization.MetricDumpSerializer();

		final ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
		final ObjectOutputStream oos = new ObjectOutputStream(bos);

		oos.writeObject(serializer.serialize(
			new HashMap<Counter, Tuple2<QueryScopeInfo, String>>(),
			new HashMap<Gauge<?>, Tuple2<QueryScopeInfo, String>>(),
			new HashMap<Histogram, Tuple2<QueryScopeInfo, String>>(),
			new HashMap<Meter, Tuple2<QueryScopeInfo, String>>()));
	}

	@Test
	public void testSerialization() throws IOException {
		MetricDumpSerialization.MetricDumpSerializer serializer = new MetricDumpSerialization.MetricDumpSerializer();
		MetricDumpSerialization.MetricDumpDeserializer deserializer = new MetricDumpSerialization.MetricDumpDeserializer();

		Map<Counter, Tuple2<QueryScopeInfo, String>> counters = new HashMap<>();
		Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();
		Map<Histogram, Tuple2<QueryScopeInfo, String>> histograms = new HashMap<>();
		Map<Meter, Tuple2<QueryScopeInfo, String>> meters = new HashMap<>();

		SimpleCounter c1 = new SimpleCounter();
		SimpleCounter c2 = new SimpleCounter();

		c1.inc(1);
		c2.inc(2);

		Gauge<Integer> g1 = new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 4;
			}
		};

		Histogram h1 = new TestHistogram();

		Meter m1 = new Meter() {
			@Override
			public void markEvent() {
			}

			@Override
			public void markEvent(long n) {
			}

			@Override
			public double getRate() {
				return 5;
			}

			@Override
			public long getCount() {
				return 10;
			}
		};

		counters.put(c1, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.JobManagerQueryScopeInfo("A"), "c1"));
		counters.put(c2, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.TaskManagerQueryScopeInfo("tmid", "B"), "c2"));
		meters.put(m1, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.JobQueryScopeInfo("jid", "C"), "c3"));
		gauges.put(g1, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.TaskQueryScopeInfo("jid", "vid", 2, "D"), "g1"));
		histograms.put(h1, new Tuple2<QueryScopeInfo, String>(new QueryScopeInfo.OperatorQueryScopeInfo("jid", "vid", 2, "opname", "E"), "h1"));

		MetricDumpSerialization.MetricSerializationResult serialized = serializer.serialize(
			counters, gauges, histograms, meters);
		List<MetricDump> deserialized = deserializer.deserialize(serialized);

		// ===== Counters ==============================================================================================
		assertEquals(5, deserialized.size());

		for (MetricDump metric : deserialized) {
			switch (metric.getCategory()) {
				case METRIC_CATEGORY_COUNTER:
					MetricDump.CounterDump counterDump = (MetricDump.CounterDump) metric;
					switch ((byte) counterDump.count) {
						case 1:
							assertTrue(counterDump.scopeInfo instanceof QueryScopeInfo.JobManagerQueryScopeInfo);
							assertEquals("A", counterDump.scopeInfo.scope);
							assertEquals("c1", counterDump.name);
							counters.remove(c1);
							break;
						case 2:
							assertTrue(counterDump.scopeInfo instanceof QueryScopeInfo.TaskManagerQueryScopeInfo);
							assertEquals("B", counterDump.scopeInfo.scope);
							assertEquals("c2", counterDump.name);
							assertEquals("tmid", ((QueryScopeInfo.TaskManagerQueryScopeInfo) counterDump.scopeInfo).taskManagerID);
							counters.remove(c2);
							break;
						default:
							fail();
					}
					break;
				case METRIC_CATEGORY_GAUGE:
					MetricDump.GaugeDump gaugeDump = (MetricDump.GaugeDump) metric;
					assertEquals("4", gaugeDump.value);
					assertEquals("g1", gaugeDump.name);

					assertTrue(gaugeDump.scopeInfo instanceof QueryScopeInfo.TaskQueryScopeInfo);
					QueryScopeInfo.TaskQueryScopeInfo taskInfo = (QueryScopeInfo.TaskQueryScopeInfo) gaugeDump.scopeInfo;
					assertEquals("D", taskInfo.scope);
					assertEquals("jid", taskInfo.jobID);
					assertEquals("vid", taskInfo.vertexID);
					assertEquals(2, taskInfo.subtaskIndex);
					gauges.remove(g1);
					break;
				case METRIC_CATEGORY_HISTOGRAM:
					MetricDump.HistogramDump histogramDump = (MetricDump.HistogramDump) metric;
					assertEquals("h1", histogramDump.name);
					assertEquals(0.5, histogramDump.median, 0.1);
					assertEquals(0.75, histogramDump.p75, 0.1);
					assertEquals(0.90, histogramDump.p90, 0.1);
					assertEquals(0.95, histogramDump.p95, 0.1);
					assertEquals(0.98, histogramDump.p98, 0.1);
					assertEquals(0.99, histogramDump.p99, 0.1);
					assertEquals(0.999, histogramDump.p999, 0.1);
					assertEquals(4, histogramDump.mean, 0.1);
					assertEquals(5, histogramDump.stddev, 0.1);
					assertEquals(6, histogramDump.max);
					assertEquals(7, histogramDump.min);

					assertTrue(histogramDump.scopeInfo instanceof QueryScopeInfo.OperatorQueryScopeInfo);
					QueryScopeInfo.OperatorQueryScopeInfo opInfo = (QueryScopeInfo.OperatorQueryScopeInfo) histogramDump.scopeInfo;
					assertEquals("E", opInfo.scope);
					assertEquals("jid", opInfo.jobID);
					assertEquals("vid", opInfo.vertexID);
					assertEquals(2, opInfo.subtaskIndex);
					assertEquals("opname", opInfo.operatorName);
					histograms.remove(h1);
					break;
				case METRIC_CATEGORY_METER:
					MetricDump.MeterDump meterDump = (MetricDump.MeterDump) metric;
					assertEquals(5.0, meterDump.rate, 0.1);

					assertTrue(meterDump.scopeInfo instanceof QueryScopeInfo.JobQueryScopeInfo);
					assertEquals("C", meterDump.scopeInfo.scope);
					assertEquals("c3", meterDump.name);
					assertEquals("jid", ((QueryScopeInfo.JobQueryScopeInfo) meterDump.scopeInfo).jobID);
					break;
				default:
					fail();
			}
		}
		assertTrue(counters.isEmpty());
		assertTrue(gauges.isEmpty());
		assertTrue(histograms.isEmpty());
	}
}
