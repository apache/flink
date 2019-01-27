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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Tests for the AbstractMetricsHandler.
 */
public class AbstractMetricsHandlerTest extends TestLogger {
	/**
	 * Verifies that the handlers correctly handle expected REST calls.
	 */
	@Test
	public void testHandleRequest() throws Exception {
		MetricFetcher fetcher = new MetricFetcher(
			mock(GatewayRetriever.class),
			mock(MetricQueryServiceRetriever.class),
			Executors.directExecutor(),
			TestingUtils.TIMEOUT());
		MetricStoreTest.setupStore(fetcher.getMetricStore());

		JobVertexMetricsHandler handler = new JobVertexMetricsHandler(Executors.directExecutor(), fetcher);

		Map<String, String> pathParams = new HashMap<>();
		Map<String, String> queryParams = new HashMap<>();

		pathParams.put("jobid", "jobid");
		pathParams.put("vertexid", "taskid");

		// get list of available metrics
		String availableList = handler.handleJsonRequest(pathParams, queryParams, null).get();

		assertEquals("[" +
				"{\"id\":\"8.opname.abc.metric6\"}," +
				"{\"id\":\"8.opname.abc.metric7\"}," +
				"{\"id\":\"1.opname.abc.metric6\"}," +
				"{\"id\":\"1.opname.abc.metric7\"}," +
				"{\"id\":\"8.abc.metric5\"}" +
				"]",
			availableList);

		// get value for a single metric
		queryParams.put("get", "8.opname.abc.metric6");

		String metricValue = handler.handleJsonRequest(pathParams, queryParams, null).get();

		assertEquals("[" +
				"{\"id\":\"8.opname.abc.metric6\",\"value\":\"5\"}" +
				"]"
			, metricValue
		);

		// get values for multiple metrics
		queryParams.put("get", "8.opname.abc.metric6,8.abc.metric5");

		String metricValues = handler.handleJsonRequest(pathParams, queryParams, null).get();

		assertEquals("[" +
				"{\"id\":\"8.opname.abc.metric6\",\"value\":\"5\"}," +
				"{\"id\":\"8.abc.metric5\",\"value\":\"4\"}" +
				"]",
			metricValues
		);
	}

	/**
	 * Verifies that a malformed request for available metrics does not throw an exception.
	 */
	@Test
	public void testInvalidListDoesNotFail() {
		MetricFetcher fetcher = new MetricFetcher(
			mock(GatewayRetriever.class),
			mock(MetricQueryServiceRetriever.class),
			Executors.directExecutor(),
			TestingUtils.TIMEOUT());
		MetricStoreTest.setupStore(fetcher.getMetricStore());

		JobVertexMetricsHandler handler = new JobVertexMetricsHandler(Executors.directExecutor(), fetcher);

		Map<String, String> pathParams = new HashMap<>();
		Map<String, String> queryParams = new HashMap<>();

		pathParams.put("jobid", "jobid");
		pathParams.put("vertexid", "taskid");

		//-----invalid variable
		pathParams.put("jobid", "nonexistent");

		try {
			assertEquals("", handler.handleJsonRequest(pathParams, queryParams, null).get());
		} catch (Exception e) {
			fail();
		}
	}

	/**
	 * Verifies that a malformed request for a metric value does not throw an exception.
	 */
	@Test
	public void testInvalidGetDoesNotFail() {
		MetricFetcher fetcher = new MetricFetcher(
			mock(GatewayRetriever.class),
			mock(MetricQueryServiceRetriever.class),
			Executors.directExecutor(),
			TestingUtils.TIMEOUT());
		MetricStoreTest.setupStore(fetcher.getMetricStore());

		JobVertexMetricsHandler handler = new JobVertexMetricsHandler(Executors.directExecutor(), fetcher);

		Map<String, String> pathParams = new HashMap<>();
		Map<String, String> queryParams = new HashMap<>();

		pathParams.put("jobid", "jobid");
		pathParams.put("vertexid", "taskid");

		//-----empty string
		queryParams.put("get", "");

		try {
			assertEquals("", handler.handleJsonRequest(pathParams, queryParams, null).get());
		} catch (Exception e) {
			fail(e.getMessage());
		}

		//-----invalid variable
		pathParams.put("jobid", "nonexistent");
		queryParams.put("get", "subindex.opname.abc.metric5");

		try {
			assertEquals("", handler.handleJsonRequest(pathParams, queryParams, null).get());
		} catch (Exception e) {
			fail(e.getMessage());
		}

		//-----invalid metric
		pathParams.put("jobid", "nonexistant");
		queryParams.put("get", "subindex.opname.abc.nonexistant");

		try {
			assertEquals("", handler.handleJsonRequest(pathParams, queryParams, null).get());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
