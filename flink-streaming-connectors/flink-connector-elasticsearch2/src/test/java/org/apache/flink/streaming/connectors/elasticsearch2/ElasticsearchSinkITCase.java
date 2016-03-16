/**
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
package org.apache.flink.streaming.connectors.elasticsearch2;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchSinkITCase extends StreamingMultipleProgramsTestBase {

	private static final int NUM_ELEMENTS = 20;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testTransportClient() throws Exception {

		File dataDir = tempFolder.newFolder();

		Node node = NodeBuilder.nodeBuilder()
				.settings(Settings.settingsBuilder()
						.put("path.home", dataDir.getParent())
						.put("http.enabled", false)
						.put("path.data", dataDir.getAbsolutePath()))
				// set a custom cluster name to verify that user config works correctly
				.clusterName("my-transport-client-cluster")
				.node();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> config = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		config.put("cluster.name", "my-transport-client-cluster");

		// Can't use {@link TransportAddress} as its not Serializable in Elasticsearch 2.x
		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		source.addSink(new ElasticsearchSink<>(config, transports, new TestElasticsearchSinkFunction()));

		env.execute("Elasticsearch TransportClient Test");

		// verify the results
		Client client = node.client();
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			GetResponse response = client.get(new GetRequest("my-index",
					"my-type", Integer.toString(i))).actionGet();
			Assert.assertEquals("message #" + i, response.getSource().get("data"));
		}

		node.close();
	}

 @Test(expected = IllegalArgumentException.class)
 public void testNullTransportClient() throws Exception {

	File dataDir = tempFolder.newFolder();

	Node node = NodeBuilder.nodeBuilder()
		.settings(Settings.settingsBuilder()
			.put("path.home", dataDir.getParent())
			.put("http.enabled", false)
			.put("path.data", dataDir.getAbsolutePath()))
		// set a custom cluster name to verify that user config works correctly
		.clusterName("my-transport-client-cluster")
		.node();

	final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

	Map<String, String> config = new HashMap<>();
	// This instructs the sink to emit after every element, otherwise they would be buffered
	config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
	config.put("cluster.name", "my-transport-client-cluster");

	source.addSink(new ElasticsearchSink<>(config, null, new TestElasticsearchSinkFunction()));

	env.execute("Elasticsearch TransportClient Test");

	// verify the results
	Client client = node.client();
	for (int i = 0; i < NUM_ELEMENTS; i++) {
	 GetResponse response = client.get(new GetRequest("my-index",
		 "my-type", Integer.toString(i))).actionGet();
	 Assert.assertEquals("message #" + i, response.getSource().get("data"));
	}

	node.close();
 }

 @Test(expected = IllegalArgumentException.class)
 public void testEmptyTransportClient() throws Exception {

	File dataDir = tempFolder.newFolder();

	Node node = NodeBuilder.nodeBuilder()
		.settings(Settings.settingsBuilder()
			.put("path.home", dataDir.getParent())
			.put("http.enabled", false)
			.put("path.data", dataDir.getAbsolutePath()))
		// set a custom cluster name to verify that user config works correctly
		.clusterName("my-transport-client-cluster")
		.node();

	final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

	Map<String, String> config = new HashMap<>();
	// This instructs the sink to emit after every element, otherwise they would be buffered
	config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
	config.put("cluster.name", "my-transport-client-cluster");

	source.addSink(new ElasticsearchSink<>(config, new ArrayList<InetSocketAddress>(), new TestElasticsearchSinkFunction()));

	env.execute("Elasticsearch TransportClient Test");

	// verify the results
	Client client = node.client();
	for (int i = 0; i < NUM_ELEMENTS; i++) {
	 GetResponse response = client.get(new GetRequest("my-index",
		 "my-type", Integer.toString(i))).actionGet();
	 Assert.assertEquals("message #" + i, response.getSource().get("data"));
	}

	node.close();
 }

	@Test(expected = JobExecutionException.class)
	public void testTransportClientFails() throws Exception{
		// this checks whether the TransportClient fails early when there is no cluster to
		// connect to. There isn't a similar test for the Node Client version since that
		// one will block and wait for a cluster to come online

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> config = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		config.put("cluster.name", "my-node-client-cluster");

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		source.addSink(new ElasticsearchSink<>(config, transports, new TestElasticsearchSinkFunction()));

		env.execute("Elasticsearch Node Client Test");
	}

	private static class TestSourceFunction implements SourceFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
			for (int i = 0; i < NUM_ELEMENTS && running; i++) {
				ctx.collect(Tuple2.of(i, "message #" + i));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class TestElasticsearchSinkFunction implements ElasticsearchSinkFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
			Map<String, Object> json = new HashMap<>();
			json.put("data", element.f1);

			return Requests.indexRequest()
					.index("my-index")
					.type("my-type")
					.id(element.f0.toString())
					.source(json);
		}

		@Override
		public void process(Tuple2<Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
			indexer.add(createIndexRequest(element));
		}
	}
}