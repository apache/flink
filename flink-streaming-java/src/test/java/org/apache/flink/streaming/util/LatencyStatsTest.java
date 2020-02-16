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
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Tests for the {@link LatencyStats}.
 */
public class LatencyStatsTest extends TestLogger {

	private static final OperatorID OPERATOR_ID = new OperatorID();
	private static final OperatorID SOURCE_ID_1 = new OperatorID();
	private static final OperatorID SOURCE_ID_2 = new OperatorID();

	private static final int OPERATOR_SUBTASK_INDEX = 64;

	private static final String PARENT_GROUP_NAME = "parent";

	@Test
	public void testLatencyStatsSingle() {
		testLatencyStats(LatencyStats.Granularity.SINGLE, registrations -> {
			Assert.assertEquals(1, registrations.size());

			{
				final Tuple2<String, Histogram> registration = registrations.get(0);
				assertName(registration.f0);
				Assert.assertEquals(5, registration.f1.getCount());
			}
		});
	}

	@Test
	public void testLatencyStatsOperator() {
		testLatencyStats(LatencyStats.Granularity.OPERATOR, registrations -> {
			Assert.assertEquals(2, registrations.size());

			{
				final Tuple2<String, Histogram> registration = registrations.get(0);
				assertName(registration.f0, SOURCE_ID_1);
				Assert.assertEquals(3, registration.f1.getCount());
			}

			{
				final Tuple2<String, Histogram> registration = registrations.get(1);
				assertName(registration.f0, SOURCE_ID_2);
				Assert.assertEquals(2, registration.f1.getCount());
			}
		});
	}

	@Test
	public void testLatencyStatsSubtask() {
		testLatencyStats(LatencyStats.Granularity.SUBTASK, registrations -> {
			Assert.assertEquals(4, registrations.size());

			{
				final Tuple2<String, Histogram> registration = registrations.get(0);
				assertName(registration.f0, SOURCE_ID_1, 0);
				Assert.assertEquals(2, registration.f1.getCount());
			}

			{
				final Tuple2<String, Histogram> registration = registrations.get(1);
				assertName(registration.f0, SOURCE_ID_1, 1);
				Assert.assertEquals(1, registration.f1.getCount());
			}

			{
				final Tuple2<String, Histogram> registration = registrations.get(2);
				assertName(registration.f0, SOURCE_ID_2, 2);
				Assert.assertEquals(1, registration.f1.getCount());
			}

			{
				final Tuple2<String, Histogram> registration = registrations.get(3);
				assertName(registration.f0, SOURCE_ID_2, 3);
				Assert.assertEquals(1, registration.f1.getCount());
			}
		});
	}

	private static void testLatencyStats(
		final LatencyStats.Granularity granularity,
		final Consumer<List<Tuple2<String, Histogram>>> verifier) {

		final AbstractMetricGroup<?> dummyGroup = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
		final TestMetricRegistry registry = new TestMetricRegistry();
		final MetricGroup parentGroup = new GenericMetricGroup(registry, dummyGroup, PARENT_GROUP_NAME);

		final LatencyStats latencyStats = new LatencyStats(
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

		verifier.accept(registry.latencyHistograms);
	}

	/**
	 * Removes all parts from the metric identifier preceding the latency-related parts.
	 */
	private static String sanitizeName(final String registrationName) {
		return registrationName.substring(registrationName.lastIndexOf(PARENT_GROUP_NAME) + PARENT_GROUP_NAME.length() + 1);
	}

	private static void assertName(final String registrationName) {
		final String sanitizedName = sanitizeName(registrationName);
		Assert.assertEquals("operator_id." + OPERATOR_ID +
			".operator_subtask_index." + OPERATOR_SUBTASK_INDEX +
			".latency", sanitizedName);
	}

	private static void assertName(final String registrationName, final OperatorID sourceId) {
		final String sanitizedName = sanitizeName(registrationName);
		Assert.assertEquals("source_id." + sourceId +
			".operator_id." + OPERATOR_ID +
			".operator_subtask_index." + OPERATOR_SUBTASK_INDEX +
			".latency", sanitizedName);
	}

	private static void assertName(final String registrationName, final OperatorID sourceId, final int sourceIndex) {
		final String sanitizedName = sanitizeName(registrationName);
		Assert.assertEquals("source_id." + sourceId +
			".source_subtask_index." + sourceIndex +
			".operator_id." + OPERATOR_ID +
			".operator_subtask_index." + OPERATOR_SUBTASK_INDEX +
			".latency", sanitizedName);
	}

	private static class TestMetricRegistry implements MetricRegistry {

		private final List<Tuple2<String, Histogram>> latencyHistograms = new ArrayList<>(4);

		@Override
		public void register(Metric metric, String metricName, AbstractMetricGroup group) {
			if (metric instanceof Histogram) {
				latencyHistograms.add(Tuple2.of(group.getMetricIdentifier(metricName), (Histogram) metric));
			}
		}

		@Override
		public char getDelimiter() {
			return '.';
		}

		@Override
		public int getNumberReporters() {
			return 0;
		}

		@Override
		public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {

		}

		@Override
		public ScopeFormats getScopeFormats() {
			return null;
		}
	}
}
