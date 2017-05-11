/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.batch.connectors.elasticsearch;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.commectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.batch.connectors.elasticsearch.testutils.SourceSinkDataTestKit;
import org.apache.flink.connectors.elasticsearch.commons.ElasticsearchSinkFunction;
import org.apache.flink.connectors.elasticsearch.commons.util.ElasticsearchUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchSinkITCase extends ElasticsearchSinkTestBase {

	@Test
	public void testTransportClient() throws Exception {
		runTransportClientTest();
	}

	@Test
	public void testNullTransportClient() throws Exception {
		runNullTransportClientTest();
	}

	@Test
	public void testEmptyTransportClient() throws Exception {
		runEmptyTransportClientTest();
	}

	@Test
	public void testTransportClientFails() throws Exception{
		runTransportClientFailsTest();
	}

	// -- Tests specific to Elasticsearch 1.x --

	/**
	 * Tests that the Elasticsearch sink works properly using an embedded node to connect to Elasticsearch.
	 */
	@Test
	public void testEmbeddedNode() throws Exception {
		final String index = "embedded-node-test-index";

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Tuple2<Integer, String>> source = env.fromCollection(SourceSinkDataTestKit.generateDatas());

		Map<String, String> userConfig = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put("cluster.name", CLUSTER_NAME);
		userConfig.put("node.local", "true");

		source.output(new ElasticsearchSink<>(
			userConfig,
			new SourceSinkDataTestKit.TestElasticsearchSinkFunction(index))
		);

		env.execute("Elasticsearch Embedded Node Test");

		// verify the results
		Client client = embeddedNodeEnv.getClient();
		SourceSinkDataTestKit.verifyProducedSinkData(client, index);

		client.close();
	}


	@Override
	protected <T> ElasticsearchSinkBase<T> createElasticsearchSink(Map<String, String> userConfig,
																List<InetSocketAddress> transportAddresses,
																ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
		return new ElasticsearchSink<>(userConfig, ElasticsearchUtils.convertInetSocketAddresses(transportAddresses), elasticsearchSinkFunction);
	}

	@Override
	protected <T> ElasticsearchSinkBase<T> createElasticsearchSinkForEmbeddedNode(
		Map<String, String> userConfig, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) throws Exception {

		// Elasticsearch 1.x requires this setting when using
		// LocalTransportAddress to connect to a local embedded node
		userConfig.put("node.local", "true");

		List<TransportAddress> transports = Lists.newArrayList();
		transports.add(new LocalTransportAddress("1"));

		return new ElasticsearchSink<>(
			userConfig,
			transports,
			elasticsearchSinkFunction);
	}
}
