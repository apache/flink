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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Tests for the {@link AbstractAggregatingMetricsHandler}.
 */
public class AbstractAggregatingMetricsHandlerTest {
	private static final ObjectMapper mapper = new ObjectMapper();

	private static AggregatingTaskManagersMetricsHandler handler;

	@BeforeClass
	public static void setupHandler() {
		MetricFetcher fetcher = new MetricFetcher(
			mock(GatewayRetriever.class),
			mock(MetricQueryServiceRetriever.class),
			Executors.directExecutor(),
			TestingUtils.TIMEOUT());
		MetricStoreTest.setupStore(fetcher.getMetricStore());

		handler = new AggregatingTaskManagersMetricsHandler(Executors.directExecutor(), fetcher);
	}

	@Test
	public void testListMetrics() throws Exception {
		String response = handler.handleJsonRequest(Collections.emptyMap(), Collections.emptyMap(), null)
			.get();

		Assert.assertEquals("[{\"id\":\"abc.metric22\"},{\"id\":\"abc.metric2b\"},{\"id\":\"abc.metric2\"}]", response);
	}

	@Test
	public void testMinAggregation() throws Exception {
		Map<String, String> queryParams = new HashMap<>();
		queryParams.put("get", "abc.metric2");
		queryParams.put("agg", "min");
		String response = handler.handleJsonRequest(Collections.emptyMap(), queryParams, null)
			.get();

		JsonNode responseObject = mapper.readTree(response);
		Assert.assertEquals(1, responseObject.size());
		JsonNode metricInfo = responseObject.get(0);
		Assert.assertEquals("abc.metric2", metricInfo.get("id").asText());
		Assert.assertEquals(1, metricInfo.get("min").asDouble(), 0.01);
	}

	@Test
	public void testMaxAggregation() throws Exception {
		Map<String, String> queryParams = new HashMap<>();
		queryParams.put("get", "abc.metric2");
		queryParams.put("agg", "max");
		String response = handler.handleJsonRequest(Collections.emptyMap(), queryParams, null)
			.get();

		JsonNode responseObject = mapper.readTree(response);
		Assert.assertEquals(1, responseObject.size());
		JsonNode metricInfo = responseObject.get(0);
		Assert.assertEquals("abc.metric2", metricInfo.get("id").asText());
		Assert.assertEquals(10, metricInfo.get("max").asDouble(), 0.01);
	}

	@Test
	public void testAvgAggregation() throws Exception {
		Map<String, String> queryParams = new HashMap<>();
		queryParams.put("get", "abc.metric2");
		queryParams.put("agg", "avg");
		String response = handler.handleJsonRequest(Collections.emptyMap(), queryParams, null)
			.get();

		JsonNode responseObject = mapper.readTree(response);
		Assert.assertEquals(1, responseObject.size());
		JsonNode metricInfo = responseObject.get(0);
		Assert.assertEquals("abc.metric2", metricInfo.get("id").asText());
		Assert.assertEquals(5.5, metricInfo.get("avg").asDouble(), 0.01);
	}

	@Test
	public void testSumAggregation() throws Exception {
		Map<String, String> queryParams = new HashMap<>();
		queryParams.put("get", "abc.metric2");
		queryParams.put("agg", "sum");
		String response = handler.handleJsonRequest(Collections.emptyMap(), queryParams, null)
			.get();

		JsonNode responseObject = mapper.readTree(response);
		Assert.assertEquals(1, responseObject.size());
		JsonNode metricInfo = responseObject.get(0);
		Assert.assertEquals("abc.metric2", metricInfo.get("id").asText());
		Assert.assertEquals(11, metricInfo.get("sum").asDouble(), 0.01);
	}

	@Test
	public void testMultipleAggregations() throws Exception {
		Map<String, String> queryParams = new HashMap<>();
		queryParams.put("get", "abc.metric2");
		queryParams.put("agg", "sum,max,min,avg");
		String response = handler.handleJsonRequest(Collections.emptyMap(), queryParams, null)
			.get();

		JsonNode array = mapper.readTree(response);
		Assert.assertEquals(1, array.size());

		FullMetricInfo mappedResponse = mapper.treeToValue(array.get(0), FullMetricInfo.class);
		Assert.assertEquals("abc.metric2", mappedResponse.id);
		Assert.assertEquals(11, mappedResponse.sum, 0.01);
		Assert.assertEquals(1, mappedResponse.min, 0.01);
		Assert.assertEquals(10, mappedResponse.max, 0.01);
		Assert.assertEquals(5.5, mappedResponse.avg, 0.01);
	}

	@Test
	public void testDefaultAggregations() throws Exception {
		Map<String, String> queryParams = new HashMap<>();
		queryParams.put("get", "abc.metric2");
		String response = handler.handleJsonRequest(Collections.emptyMap(), queryParams, null)
			.get();

		JsonNode array = mapper.readTree(response);
		Assert.assertEquals(1, array.size());

		FullMetricInfo mappedResponse = mapper.treeToValue(array.get(0), FullMetricInfo.class);
		Assert.assertEquals("abc.metric2", mappedResponse.id);
		Assert.assertEquals(11, mappedResponse.sum, 0.01);
		Assert.assertEquals(1, mappedResponse.min, 0.01);
		Assert.assertEquals(10, mappedResponse.max, 0.01);
		Assert.assertEquals(5.5, mappedResponse.avg, 0.01);
	}

	private static class FullMetricInfo {
		private final String id;
		private final double min;
		private final double max;
		private final double avg;
		private final double sum;

		@JsonCreator
		private FullMetricInfo(
			@JsonProperty("id") String id,
			@JsonProperty("min") double min,
			@JsonProperty("max") double max,
			@JsonProperty("avg") double avg,
			@JsonProperty("sum") double sum) {
			this.id = id;
			this.min = min;
			this.max = max;
			this.avg = avg;
			this.sum = sum;
		}
	}
}
