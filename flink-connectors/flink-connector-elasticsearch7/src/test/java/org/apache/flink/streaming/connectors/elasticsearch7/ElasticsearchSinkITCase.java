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

package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkTestBase;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

/**
 * IT cases for the {@link ElasticsearchSink}.
 */
public class ElasticsearchSinkITCase extends ElasticsearchSinkTestBase<RestHighLevelClient, HttpHost> {

	@ClassRule
	public static ElasticsearchContainer elasticsearchContainer =
			new ElasticsearchContainer(
					DockerImageName
							.parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
							.withTag("7.5.1"));

	@Override
	protected String getClusterName() {
		return "docker-cluster";
	}

	@Override
	@SuppressWarnings("deprecation")
	protected final Client getClient() {
		TransportAddress transportAddress = new TransportAddress(elasticsearchContainer.getTcpHost());
		String expectedClusterName = getClusterName();
		Settings settings = Settings.builder().put("cluster.name", expectedClusterName).build();
		return new PreBuiltTransportClient(settings)
				.addTransportAddress(transportAddress);
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
	public void testNullAddresses() {
		runNullAddressesTest();
	}

	@Test
	public void testEmptyAddresses() {
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
			ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) {

		return createElasticsearchSinkForNode(
				bulkFlushMaxActions, clusterName, elasticsearchSinkFunction, elasticsearchContainer.getHttpHostAddress());
	}

	@Override
	protected ElasticsearchSinkBase<Tuple2<Integer, String>, RestHighLevelClient> createElasticsearchSinkForNode(
			int bulkFlushMaxActions,
			String clusterName,
			ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction,
			String hostAddress) {

		ArrayList<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(HttpHost.create(hostAddress));

		ElasticsearchSink.Builder<Tuple2<Integer, String>> builder = new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
		builder.setBulkFlushMaxActions(bulkFlushMaxActions);

		return builder.build();
	}
}
