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
package org.apache.flink.runtime.webmonitor.metrics;

import akka.actor.ActorSystem;
import org.apache.flink.runtime.webmonitor.JobManagerRetriever;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import scala.concurrent.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.mock;

public class JobManagerMetricsHandlerTest extends TestLogger {
	@Test
	public void getMapFor() {
		MetricFetcher fetcher = new MetricFetcher(mock(ActorSystem.class), mock(JobManagerRetriever.class), mock(ExecutionContext.class));
		MetricStore store = MetricStoreTest.setupStore(fetcher.getMetricStore());

		JobManagerMetricsHandler handler = new JobManagerMetricsHandler(fetcher);

		Map<String, String> pathParams = new HashMap<>();

		Map<String, String> metrics = handler.getMapFor(pathParams, store);

		assertEquals("0", metrics.get("abc.metric1"));
	}

	@Test
	public void getMapForNull() {
		MetricFetcher fetcher = new MetricFetcher(mock(ActorSystem.class), mock(JobManagerRetriever.class), mock(ExecutionContext.class));
		MetricStore store = fetcher.getMetricStore();

		JobManagerMetricsHandler handler = new JobManagerMetricsHandler(fetcher);

		Map<String, String> pathParams = new HashMap<>();

		Map<String, String> metrics = handler.getMapFor(pathParams, store);

		assertNotNull(metrics);
	}
}
