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

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.retriever.JobManagerRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.webmonitor.metrics.JobMetricsHandler.PARAMETER_JOB_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Tests for the JobMetricsHandler.
 */
public class JobMetricsHandlerTest extends TestLogger {
	@Test
	public void testGetPaths() {
		JobMetricsHandler handler = new JobMetricsHandler(mock(MetricFetcher.class));
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/metrics", paths[0]);
	}

	@Test
	public void getMapFor() throws Exception {
		MetricFetcher fetcher = new MetricFetcher(
			mock(JobManagerRetriever.class),
			mock(MetricQueryServiceRetriever.class),
			Executors.directExecutor(),
			TestingUtils.TIMEOUT());
		MetricStore store = MetricStoreTest.setupStore(fetcher.getMetricStore());

		JobMetricsHandler handler = new JobMetricsHandler(fetcher);

		Map<String, String> pathParams = new HashMap<>();
		pathParams.put(PARAMETER_JOB_ID, "jobid");

		Map<String, String> metrics = handler.getMapFor(pathParams, store);

		assertEquals("2", metrics.get("abc.metric3"));
	}

	@Test
	public void getMapForNull() {
		MetricFetcher fetcher = new MetricFetcher(
			mock(JobManagerRetriever.class),
			mock(MetricQueryServiceRetriever.class),
			Executors.directExecutor(),
			TestingUtils.TIMEOUT());
		MetricStore store = fetcher.getMetricStore();

		JobMetricsHandler handler = new JobMetricsHandler(fetcher);

		Map<String, String> pathParams = new HashMap<>();

		Map<String, String> metrics = handler.getMapFor(pathParams, store);

		assertNull(metrics);
	}
}
