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
package org.apache.flink.streaming.connectors.elasticsearch;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ElasticsearchSinkITCase extends StreamingMultipleProgramsTestBase {

	private static final int NUM_ELEMENTS = 20;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testNodeClient() throws Exception{

		File dataDir = tempFolder.newFolder();

		Node node = nodeBuilder()
				.settings(ImmutableSettings.settingsBuilder()
						.put("http.enabled", false)
						.put("path.data", dataDir.getAbsolutePath()))
				// set a custom cluster name to verify that user config works correctly
				.clusterName("my-node-client-cluster")
				.local(true)
				.node();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> config = Maps.newHashMap();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		config.put("cluster.name", "my-node-client-cluster");

		// connect to our local node
		config.put("node.local", "true");

		source.addSink(new ElasticsearchSink<>(config, new TestIndexRequestBuilder()));

		env.execute("Elasticsearch Node Client Test");


		// verify the results
		Client client = node.client();
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			GetResponse response = client.get(new GetRequest("my-index",
					"my-type",
					Integer.toString(i))).actionGet();
			Assert.assertEquals("message #" + i, response.getSource().get("data"));
		}

		node.close();
	}

	@Test
	public void testTransportClient() throws Exception {

		File dataDir = tempFolder.newFolder();

		Node node = nodeBuilder()
				.settings(ImmutableSettings.settingsBuilder()
						.put("http.enabled", false)
						.put("path.data", dataDir.getAbsolutePath()))
						// set a custom cluster name to verify that user config works correctly
				.clusterName("my-node-client-cluster")
				.local(true)
				.node();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> config = Maps.newHashMap();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		config.put("cluster.name", "my-node-client-cluster");

		// connect to our local node
		config.put("node.local", "true");

		List<TransportAddress> transports = Lists.newArrayList();
		transports.add(new LocalTransportAddress("1"));

		source.addSink(new ElasticsearchSink<>(config, transports, new TestIndexRequestBuilder()));

		env.execute("Elasticsearch TransportClient Test");


		// verify the results
		Client client = node.client();
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			GetResponse response = client.get(new GetRequest("my-index",
					"my-type",
					Integer.toString(i))).actionGet();
			Assert.assertEquals("message #" + i, response.getSource().get("data"));
		}

		node.close();
	}

	@Test(expected = JobExecutionException.class)
	public void testTransportClientFails() throws Exception{
		// this checks whether the TransportClient fails early when there is no cluster to
		// connect to. We don't hava such as test for the Node Client version since that
		// one will block and wait for a cluster to come online

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction());

		Map<String, String> config = Maps.newHashMap();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		config.put("cluster.name", "my-node-client-cluster");

		// connect to our local node
		config.put("node.local", "true");

		List<TransportAddress> transports = Lists.newArrayList();
		transports.add(new LocalTransportAddress("1"));

		source.addSink(new ElasticsearchSink<>(config, transports, new TestIndexRequestBuilder()));

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

	private static class TestIndexRequestBuilder implements IndexRequestBuilder<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public IndexRequest createIndexRequest(Tuple2<Integer, String> element, RuntimeContext ctx) {
			Map<String, Object> json = new HashMap<>();
			json.put("data", element.f1);

			return Requests.indexRequest()
					.index("my-index")
					.type("my-type")
					.id(element.f0.toString())
					.source(json);
		}
	}
}
