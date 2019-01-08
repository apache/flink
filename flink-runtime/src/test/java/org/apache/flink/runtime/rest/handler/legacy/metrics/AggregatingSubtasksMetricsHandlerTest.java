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

package org.apache.flink.runtime.rest.handler.legacy.metrics;

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.rest.handler.legacy.metrics.JobMetricsHandler.PARAMETER_JOB_ID;
import static org.apache.flink.runtime.rest.handler.legacy.metrics.JobVertexMetricsHandler.PARAMETER_VERTEX_ID;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Tests for the {@link AggregatingSubtasksMetricsHandler}.
 */
public class AggregatingSubtasksMetricsHandlerTest extends TestLogger {
	@Test
	public void testGetPaths() {
		AggregatingSubtasksMetricsHandler handler = new AggregatingSubtasksMetricsHandler(Executors.directExecutor(), mock(MetricFetcher.class));
		String[] paths = handler.getPaths();
		assertEquals(1, paths.length);
		assertEquals("/jobs/:jobid/vertices/:vertexid/subtasks/metrics", paths[0]);
	}

	@Test
	public void getStores() throws Exception {
		MetricFetcher fetcher = new MetricFetcher(
			mock(GatewayRetriever.class),
			mock(MetricQueryServiceRetriever.class),
			Executors.directExecutor(),
			TestingUtils.TIMEOUT());
		MetricStore store = MetricStoreTest.setupStore(fetcher.getMetricStore());

		AggregatingSubtasksMetricsHandler handler = new AggregatingSubtasksMetricsHandler(Executors.directExecutor(), fetcher);

		Map<String, String> pathParams = new HashMap<>();
		pathParams.put(PARAMETER_JOB_ID, "jobid");
		pathParams.put(PARAMETER_VERTEX_ID, "taskid");
		Map<String, String> queryParams = new HashMap<>();

		Collection<? extends MetricStore.ComponentMetricStore> stores = handler.getStores(store, pathParams, queryParams);

		assertEquals(2, stores.size());
	}
}
