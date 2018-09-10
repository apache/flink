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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End to end test for Elasticsearch6Sink.
 */
public class Elasticsearch6SinkExample {

	public static void main(String[] args) throws Exception {

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 3) {
			System.out.println("Missing parameters!\n" +
				"Usage: --numRecords <numRecords> --index <index> --type <type>");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(5000);

		DataStream<String> source = env.generateSequence(0, parameterTool.getInt("numRecords") - 1)
			.map(new MapFunction<Long, String>() {
				@Override
				public String map(Long value) throws Exception {
					return "message #" + value;
				}
			});

		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

		ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
			httpHosts,
			(String element, RuntimeContext ctx, RequestIndexer indexer) -> indexer.add(createIndexRequest(element, parameterTool)));

		// this instructs the sink to emit after every element, otherwise they would be buffered
		esSinkBuilder.setBulkFlushMaxActions(1);

		source.addSink(esSinkBuilder.build());

		env.execute("Elasticsearch 6.x end to end sink test example");
	}

	private static IndexRequest createIndexRequest(String element, ParameterTool parameterTool) {
		Map<String, Object> json = new HashMap<>();
		json.put("data", element);

		return Requests.indexRequest()
			.index(parameterTool.getRequired("index"))
			.type(parameterTool.getRequired("type"))
			.id(element)
			.source(json);
	}
}
