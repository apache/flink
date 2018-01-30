/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.SourceSinkDataTestKit;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;

import org.apache.http.HttpHost;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * Environment preparation and suite of tests for version-specific {@link ElasticsearchSinkBase}
 * implementations.
 */
public abstract class ElasticsearchRestSinkTestBase extends StreamingMultipleProgramsTestBase {

	protected static final boolean RUN_TESTS = false;
	protected static final String CLUSTER_NAME = "test-cluster";
	protected static final String ES_TEST_HOST = "localhost";
	protected static final String ES_TEST_INDEX = "es-flink-test-index";
	protected static final int ES_TEST_PORT = 9200;
	protected static RestHighLevelClient esClient;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@BeforeClass
	public static void prepare() throws Exception {
		if (!RUN_TESTS) {
			return;
		}
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting Elasticsearch REST client");
		LOG.info("-------------------------------------------------------------------------");

		// dynamically load version-specific implementation of the Elasticsearch embedded node
		// environment
		esClient = new RestHighLevelClient(
				RestClient.builder(new HttpHost(ES_TEST_HOST, ES_TEST_PORT, "http")));
	}

	@AfterClass
	public static void shutdown() throws Exception {
		if (!RUN_TESTS) {
			return;
		}
		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Shutting down Elasticsearch REST client");
		LOG.info("-------------------------------------------------------------------------");

		esClient.close();

	}

	/**
	 * Tests that the Elasticsearch sink works properly using a {@link RestHighLevelClient}.
	 */
	public void runClientTest() throws Exception {
		if (!RUN_TESTS) {
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env
				.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

		Map<String, String> userConfig = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), CLUSTER_NAME);

		List<HttpHost> clusterHosts = new ArrayList<>();
		clusterHosts.add(new HttpHost(ES_TEST_HOST, ES_TEST_PORT, "http"));
		source.addSink(createElasticsearchSink(userConfig, clusterHosts,
				new SourceSinkDataTestKit.TestElasticsearchSinkFunction(ES_TEST_INDEX)));

		env.execute("Elasticsearch REST client Test");

		// verify the results
		SourceSinkDataTestKit.verifyProducedSinkData(esClient, ES_TEST_INDEX);

		esClient.close();
	}

	/**
	 * Tests that the Elasticsearch sink fails eagerly if the provided list of hosts is
	 * {@code null}.
	 */
	public void runNullClientTest() throws Exception {
		if (!RUN_TESTS) {
			return;
		}
		Map<String, String> userConfig = new HashMap<>();
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), CLUSTER_NAME);

		try {
			createElasticsearchSink(userConfig, null,
					new SourceSinkDataTestKit.TestElasticsearchSinkFunction(ES_TEST_INDEX));
		} catch (IllegalArgumentException expectedException) {
			// test passes
			return;
		}

		fail();
	}

	/**
	 * Tests that the Elasticsearch sink fails eagerly if the provided list of nodes is
	 * empty.
	 */
	public void runEmptyClientTest() throws Exception {
		if (!RUN_TESTS) {
			return;
		}
		Map<String, String> userConfig = new HashMap<>();
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), CLUSTER_NAME);

		try {
			createElasticsearchSink(userConfig, Collections.<HttpHost>emptyList(),
					new SourceSinkDataTestKit.TestElasticsearchSinkFunction(ES_TEST_INDEX));
		} catch (IllegalArgumentException expectedException) {
			// test passes
			return;
		}

		fail();
	}

	/** Creates a version-specific Elasticsearch sink, using arbitrary hosts. */
	protected abstract <T> ElasticsearchSinkBase<T> createElasticsearchSink(
			Map<String, String> userConfig, List<HttpHost> clusterHosts,
			ElasticsearchSinkFunction<T> elasticsearchSinkFunction);
}
