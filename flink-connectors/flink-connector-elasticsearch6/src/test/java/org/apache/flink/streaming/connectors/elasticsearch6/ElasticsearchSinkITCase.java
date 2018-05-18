/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkTestBase;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.SourceSinkDataTestKit;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * IT cases for the {@link ElasticsearchSink}.
 */
public class ElasticsearchSinkITCase extends ElasticsearchSinkTestBase {

	/**
	 * Tests that the Elasticsearch sink works properly using a {@link RestHighLevelClient}.
	 */
	public void runTransportClientTest() throws Exception {
		final String index = "transport-client-test-index";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

		Map<String, String> userConfig = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		source.addSink(createElasticsearchSinkForEmbeddedNode(userConfig,
			new SourceSinkDataTestKit.TestElasticsearchSinkFunction(index)));

		env.execute("Elasticsearch RestHighLevelClient Test");

		// verify the results
		Client client = embeddedNodeEnv.getClient();
		SourceSinkDataTestKit.verifyProducedSinkData(client, index);

		client.close();
	}

	/**
	 * Tests that the Elasticsearch sink fails eagerly if the provided list of transport addresses is {@code null}.
	 */
	public void runNullTransportClientTest() throws Exception {
		try {
			Map<String, String> userConfig = new HashMap<>();
			userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
			createElasticsearchSink6(userConfig, null, new SourceSinkDataTestKit.TestElasticsearchSinkFunction("test"));
		} catch (IllegalArgumentException expectedException) {
			// test passes
			return;
		}

		fail();
	}

	/**
	 * Tests that the Elasticsearch sink fails eagerly if the provided list of transport addresses is empty.
	 */
	public void runEmptyTransportClientTest() throws Exception {
		try {
			Map<String, String> userConfig = new HashMap<>();
			userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
			createElasticsearchSink6(userConfig,
				Collections.<HttpHost>emptyList(),
				new SourceSinkDataTestKit.TestElasticsearchSinkFunction("test"));
		} catch (IllegalArgumentException expectedException) {
			// test passes
			return;
		}

		fail();
	}

	/**
	 * Tests whether the Elasticsearch sink fails when there is no cluster to connect to.
	 */
	public void runTransportClientFailsTest() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

		Map<String, String> userConfig = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		source.addSink(createElasticsearchSinkForEmbeddedNode(userConfig,
			new SourceSinkDataTestKit.TestElasticsearchSinkFunction("test")));

		try {
			env.execute("Elasticsearch Transport Client Test");
		} catch (JobExecutionException expectedException) {
			assertTrue(expectedException.getCause().getMessage().contains("not connected to any Elasticsearch nodes"));
			return;
		}

		fail();
	}

	@Override
	protected <T> ElasticsearchSinkBase<T> createElasticsearchSink(Map<String, String> userConfig, List<InetSocketAddress> transportAddresses, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
		return null;
	}

	@Override
	protected <T> ElasticsearchSinkBase<T> createElasticsearchSinkForEmbeddedNode(Map<String, String> userConfig, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) throws Exception {
		ArrayList<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
		return new ElasticsearchSink<>(userConfig, httpHosts, elasticsearchSinkFunction);
	}

	private <T> ElasticsearchSinkBase<T> createElasticsearchSink6(
		Map<String, String> userConfig,
		List<HttpHost> httpHosts,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
		return new ElasticsearchSink<>(userConfig, httpHosts, elasticsearchSinkFunction);
	}
}
