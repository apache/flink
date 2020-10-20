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
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkTestBase;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * IT cases for the {@link ElasticsearchSink}.
 */
public class ElasticsearchSinkITCase extends ElasticsearchSinkTestBase<RestHighLevelClient, HttpHost> {

	@Before
	public void ensureClusterIsUp() throws IOException {
		RestClientBuilder builder = RestClient.builder(HttpHost.create("http://127.0.0.1:9200"));
		RestHighLevelClient client = new RestHighLevelClient(builder);
		if (!client.ping()) {
			throw new RuntimeException("Cannot ping cluster!");
		}
	}

	@Test
	public void testElasticsearchSink() throws Exception {
		runElasticsearchSinkTest();
	}

	@Test
	public void testElasticsearchSinkWithSmile() throws Exception {
		runElasticsearchSinkSmileTest();
	}

	@Test
	public void testNullAddresses() throws Exception {
		runNullAddressesTest();
	}

	@Test
	public void testEmptyAddresses() throws Exception {
		runEmptyAddressesTest();
	}

	@Test
	public void testInvalidElasticsearchCluster() throws Exception{
		runInvalidElasticsearchClusterTest();
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient> createElasticsearchSink(
			int bulkFlushMaxActions,
			String clusterName,
			List<HttpHost> httpHosts,
			ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) {

		ElasticsearchSink.Builder<Tuple2<Integer, String>> builder = new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
		builder.setBulkFlushMaxActions(bulkFlushMaxActions);

		return builder.build();
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient> createElasticsearchSinkForEmbeddedNode(
			int bulkFlushMaxActions,
			String clusterName,
			ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) throws Exception {

		return createElasticsearchSinkForNode(
				bulkFlushMaxActions, clusterName, elasticsearchSinkFunction, "127.0.0.1");
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient> createElasticsearchSinkForNode(
			int bulkFlushMaxActions,
			String clusterName,
			ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction,
			String ipAddress) throws Exception {

		ArrayList<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost(ipAddress, 9200, "http"));

		ElasticsearchSink.Builder<Tuple2<Integer, String>> builder = new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
		builder.setBulkFlushMaxActions(bulkFlushMaxActions);

		return builder.build();
	}
}
