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
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
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
}
