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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import org.junit.Test;

import java.io.Serializable;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test for the {@link WatermarkStrategies} class.
 */
public class WatermarkStrategiesTest {

	@Test
	public void testDefaultTimeStampAssigner() {
		WatermarkStrategy<Object> wmStrategy = WatermarkStrategies
				.forMonotonousTimestamps()
				.build();
		// ensure that the closure can be cleaned through the WatermarkStategies
		ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		assertThat(wmStrategy.createTimestampAssigner(assignerContext()), instanceOf(RecordTimestampAssigner.class));
	}

	@Test
	public void testLambdaTimestampAssigner() {
		WatermarkStrategy<Object> wmStrategy = WatermarkStrategies
				.forMonotonousTimestamps()
				.withTimestampAssigner((event, timestamp) -> 42L)
				.build();
		// ensure that the closure can be cleaned through the WatermarkStategies
		ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		TimestampAssigner<Object> timestampAssigner = wmStrategy.createTimestampAssigner(assignerContext());

		assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
	}

	@Test
	public void testLambdaTimestampAssignerSupplier() {
		WatermarkStrategy<Object> wmStrategy = WatermarkStrategies
				.forMonotonousTimestamps()
				.withTimestampAssigner(TimestampAssignerSupplier.of((event, timestamp) -> 42L))
				.build();
		// ensure that the closure can be cleaned through the WatermarkStategies
		ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		TimestampAssigner<Object> timestampAssigner = wmStrategy.createTimestampAssigner(assignerContext());

		assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
	}

	@Test
	public void testAnonymousInnerTimestampAssigner() {
		WatermarkStrategy<Object> wmStrategy = WatermarkStrategies
				.forMonotonousTimestamps()
				.withTimestampAssigner(new SerializableTimestampAssigner<Object>() {
					@Override
					public long extractTimestamp(Object element, long recordTimestamp) {
						return 42;
					}
				})
				.build();
		// ensure that the closure can be cleaned through the WatermarkStategies
		ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		TimestampAssigner<Object> timestampAssigner = wmStrategy.createTimestampAssigner(assignerContext());

		assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
	}

	@Test
	public void testClassTimestampAssigner() {
		WatermarkStrategy<Object> wmStrategy = WatermarkStrategies
				.forMonotonousTimestamps()
				.withTimestampAssigner((ctx) -> new TestTimestampAssigner())
				.build();
		// ensure that the closure can be cleaned through the WatermarkStategies
		ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		TimestampAssigner<Object> timestampAssigner = wmStrategy.createTimestampAssigner(assignerContext());

		assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
	}

	@Test
	public void testClassTimestampAssignerUsingSupplier() {
		WatermarkStrategy<Object> wmStrategy = WatermarkStrategies
				.forMonotonousTimestamps()
				.withTimestampAssigner((context) -> new TestTimestampAssigner())
				.build();
		// ensure that the closure can be cleaned through the WatermarkStategies
		ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

		TimestampAssigner<Object> timestampAssigner = wmStrategy.createTimestampAssigner(assignerContext());

		assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
	}

	static class TestTimestampAssigner implements TimestampAssigner<Object>, Serializable {
		@Override
		public long extractTimestamp(Object element, long recordTimestamp) {
			return 42L;
		}
	}

	static TimestampAssignerSupplier.Context assignerContext() {
		return DummyMetricGroup::new;
	}

	/**
	 * A dummy {@link MetricGroup} to be used when a group is required as an argument but not actually used.
	 */
	public static class DummyMetricGroup implements MetricGroup {
		@Override
		public Counter counter(int name) {
			return null;
		}

		@Override
		public Counter counter(String name) {
			return null;
		}

		@Override
		public <C extends Counter> C counter(int name, C counter) {
			return null;
		}

		@Override
		public <C extends Counter> C counter(String name, C counter) {
			return null;
		}

		@Override
		public <T, G extends Gauge<T>> G gauge(int name, G gauge) {
			return null;
		}

		@Override
		public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
			return null;
		}

		@Override
		public <H extends Histogram> H histogram(String name, H histogram) {
			return null;
		}

		@Override
		public <H extends Histogram> H histogram(int name, H histogram) {
			return null;
		}

		@Override
		public <M extends Meter> M meter(String name, M meter) {
			return null;
		}

		@Override
		public <M extends Meter> M meter(int name, M meter) {
			return null;
		}

		@Override
		public MetricGroup addGroup(int name) {
			return null;
		}

		@Override
		public MetricGroup addGroup(String name) {
			return null;
		}

		@Override
		public MetricGroup addGroup(String key, String value) {
			return null;
		}

		@Override
		public String[] getScopeComponents() {
			return new String[0];
		}

		@Override
		public Map<String, String> getAllVariables() {
			return null;
		}

		@Override
		public String getMetricIdentifier(String metricName) {
			return null;
		}

		@Override
		public String getMetricIdentifier(
				String metricName, CharacterFilter filter) {
			return null;
		}
	}
}
