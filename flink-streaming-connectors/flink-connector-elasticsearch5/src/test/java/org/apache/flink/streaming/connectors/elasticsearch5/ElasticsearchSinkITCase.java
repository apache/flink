/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.elasticsearch5;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty3Plugin;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ElasticsearchSinkITCase extends StreamingMultipleProgramsTestBase {

	private static final int NUM_ELEMENTS = 20;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testTransportClient() throws Exception {

		File dataDir = tempFolder.newFolder();

		Settings settings = Settings.builder()
			.put("cluster.name", "my-transport-client-cluster")
			.put("http.enabled", false)
			.put("path.home", dataDir.getParent())
			.put("path.data", dataDir.getAbsolutePath())
			.put(NetworkModule.HTTP_TYPE_KEY, Netty3Plugin.NETTY_HTTP_TRANSPORT_NAME)
			.put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME)
			.build();

		Node node = new PluginNode(settings);
		node.start();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> esConfig = ImmutableMap.of("cluster.name", "my-transport-client-cluster");
		Map<String, String> sinkConfig = ImmutableMap.of(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		source.addSink(new ElasticsearchSink<>(esConfig, sinkConfig, transports, new TestElasticsearchSinkFunction()));

		env.execute("Elasticsearch TransportClient Test");

		// verify the results
		Client client = node.client();
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			GetResponse response = client.prepareGet("my-index", "my-type", Integer.toString(i)).get();
			assertEquals("message #" + i, response.getSource().get("data"));
		}

		node.close();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullTransportClient() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> esConfig = ImmutableMap.of("cluster.name", "my-transport-client-cluster");
		Map<String, String> sinkConfig = ImmutableMap.of(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		source.addSink(new ElasticsearchSink<>(esConfig, sinkConfig, null, new TestElasticsearchSinkFunction()));

		fail();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmptyTransportClient() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> esConfig = ImmutableMap.of("cluster.name", "my-transport-client-cluster");
		Map<String, String> sinkConfig = ImmutableMap.of(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		source.addSink(new ElasticsearchSink<>(esConfig, sinkConfig, new ArrayList<InetSocketAddress>(), new TestElasticsearchSinkFunction()));

		env.execute("Elasticsearch TransportClient Test");

		fail();
	}

	@Test(expected = JobExecutionException.class)
	public void testTransportClientFails() throws Exception {
		// this checks whether the TransportClient fails early when there is no cluster to
		// connect to. There isn't a similar test for the Node Client version since that
		// one will block and wait for a cluster to come online

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> esConfig = ImmutableMap.of("cluster.name", "my-transport-client-cluster");
		Map<String, String> sinkConfig = ImmutableMap.of(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		source.addSink(new ElasticsearchSink<>(esConfig, sinkConfig, transports, new TestElasticsearchSinkFunction()));

		env.execute("Elasticsearch Node Client Test");

		fail();
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

	private static class PluginNode extends Node {
		public PluginNode(Settings settings) {
			super(InternalSettingsPreparer.prepareEnvironment(settings, null), Collections.<Class<? extends Plugin>>singletonList(Netty3Plugin.class));
		}
	}
}
