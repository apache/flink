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
package org.apache.flink.streaming.connectors.elasticsearch2.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

/**
 * This example shows how to use the Elasticsearch Sink. Before running it you must ensure that
 * you have a cluster named "elasticsearch" running or change the name of cluster in the config map.
 */
public class ElasticsearchExample {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SingleOutputStreamOperator<String> source =
				env.generateSequence(0, 20).map(new MapFunction<Long, String>() {
					/**
					 * The mapping method. Takes an element from the input data set and transforms
					 * it into exactly one element.
					 *
					 * @param value The input value.
					 * @return The transformed value
					 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
					 *                   to fail and may trigger recovery.
					 */
					@Override
					public String map(Long value) throws Exception {
						return "message #" + value;
					}
				});

		Map<String, String> config = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		source.addSink(new ElasticsearchSink<>(config, null, new ElasticsearchSinkFunction<String>(){
			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				indexer.add(createIndexRequest(element));
			}
		}));

		env.execute("Elasticsearch Example");
	}

	public static IndexRequest createIndexRequest(String element) {
		Map<String, Object> json = new HashMap<>();
		json.put("data", element);

		return Requests.indexRequest()
				.index("my-index")
				.type("my-type")
				.id(element)
				.source(json);
	}
}
