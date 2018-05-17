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

package org.apache.flink.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch example for Flink Streaming Job.
 *
 * <p>In this streaming job, we generate a bunch of data from numbers, apply operator map
 * made a type conversion. Then we choose elasticsearch as its sink to storage these data.
 *
 * <p>Run test_quickstarts.sh to verify this program. Package this class to a jar, verify the jar,
 * then deploy it on a flink cluster.
 */
public class ElasticsearchStreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> source = env.generateSequence(0, 20)
			.map(new MapFunction<Long, String>() {
				@Override
				public String map(Long value) throws Exception {
					return value.toString();
				}});

		Map<String, String> userConfig = new HashMap<>();
		userConfig.put("cluster.name", "elasticsearch");
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		source.addSink(new ElasticsearchSink<>(userConfig, transports, new ElasticsearchSinkFunction<String>(){
			@Override
			public void process(String element, RuntimeContext ctx, org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer indexer) {
				indexer.add(createIndexRequest(element));
			}
		}));

		// execute program
		env.execute("Flink Streaming Job of writing data to elasticsearch");
	}

	private static IndexRequest createIndexRequest(String element) {
		Map<String, Object> json = new HashMap<>();
		json.put("data", element);

		return Requests.indexRequest()
			.index("my-index")
			.type("my-type")
			.id(element)
			.source(json);
	}
}
