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

package org.apache.flink.streaming.connectors.elasticsearch.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This example shows how to use the Elasticsearch Sink. Before running it you must ensure that
 * you have a cluster named "elasticsearch" running or change the cluster name in the config map.
 */
@SuppressWarnings("serial")
public class ElasticsearchSinkExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> source = env.generateSequence(0, 20).map(new MapFunction<Long, String>() {
			@Override
			public String map(Long value) throws Exception {
				return "message #" + value;
			}
		});

		Map<String, String> userConfig = new HashMap<>();
		userConfig.put("cluster.name", "elasticsearch");
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		List<TransportAddress> transports = new ArrayList<>();
		transports.add(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

		source.addSink(new ElasticsearchSink<>(userConfig, transports, new ElasticsearchSinkFunction<String>() {
			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				indexer.add(createIndexRequest(element));
			}
		}));

		env.execute("Elasticsearch Sink Example");
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
