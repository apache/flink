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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link MetricUtils} class.
 */
public class MetricUtilsTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(MetricUtilsTest.class);

	/**
	 * Tests that the {@link MetricUtils#startMetricsActorSystem(Configuration, String, Logger)} respects
	 * the given {@link MetricOptions#QUERY_SERVICE_THREAD_PRIORITY}.
	 */
	@Test
	public void testStartMetricActorSystemRespectsThreadPriority() throws Exception {
		final Configuration configuration = new Configuration();
		final int expectedThreadPriority = 3;
		configuration.setInteger(MetricOptions.QUERY_SERVICE_THREAD_PRIORITY, expectedThreadPriority);
		final ActorSystem actorSystem = MetricUtils.startMetricsActorSystem(configuration, "localhost", LOG);

		try {
			final int threadPriority = actorSystem.settings().config().getInt("akka.actor.default-dispatcher.thread-priority");

			assertThat(threadPriority, is(expectedThreadPriority));
		} finally {
			AkkaUtils.terminateActorSystem(actorSystem).get();
		}
	}

	@Test
	public void testNonHeapMetricsCompleteness() {
		final InterceptingOperatorMetricGroup nonHeapMetrics = new InterceptingOperatorMetricGroup();

		MetricUtils.instantiateNonHeapMemoryMetrics(nonHeapMetrics);

		Assert.assertNotNull(nonHeapMetrics.get(MetricNames.MEMORY_USED));
		Assert.assertNotNull(nonHeapMetrics.get(MetricNames.MEMORY_COMMITTED));
		Assert.assertNotNull(nonHeapMetrics.get(MetricNames.MEMORY_MAX));
	}

	@Test
	public void testHeapMetricsCompleteness() {
		final InterceptingOperatorMetricGroup heapMetrics = new InterceptingOperatorMetricGroup();

		MetricUtils.instantiateHeapMemoryMetrics(heapMetrics);

		Assert.assertNotNull(heapMetrics.get(MetricNames.MEMORY_USED));
		Assert.assertNotNull(heapMetrics.get(MetricNames.MEMORY_COMMITTED));
		Assert.assertNotNull(heapMetrics.get(MetricNames.MEMORY_MAX));
	}

	/**
	 * Tests that heap/non-heap metrics do not rely on a static MemoryUsage instance.
	 *
	 * <p>We can only check this easily for the currently used heap memory, so we use it this as a proxy for testing
	 * the functionality in general.
	 */
	@Test
	public void testHeapMetrics() throws Exception {
		final InterceptingOperatorMetricGroup heapMetrics = new InterceptingOperatorMetricGroup();

		MetricUtils.instantiateHeapMemoryMetrics(heapMetrics);

		@SuppressWarnings("unchecked")
		final Gauge<Long> used = (Gauge<Long>) heapMetrics.get(MetricNames.MEMORY_USED);

		final long usedHeapInitially = used.getValue();

		// check memory usage difference multiple times since other tests may affect memory usage as well
		for (int x = 0; x < 10; x++) {
			final byte[] array = new byte[1024 * 1024 * 8];
			final long usedHeapAfterAllocation = used.getValue();

			if (usedHeapInitially != usedHeapAfterAllocation) {
				return;
			}
			Thread.sleep(50);
		}
		Assert.fail("Heap usage metric never changed it's value.");
	}
}
