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

package org.apache.flink.streaming.connectors.twitter;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterStreaming {

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;
	private static final int NUMBEROFTWEETS = 100;

	private static final Logger LOG = LoggerFactory.getLogger(TwitterStreaming.class);

	public static class TwitterSink implements SinkFunction<Tuple5<Long, Integer, String, String, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple5<Long, Integer, String, String, String> tuple) {
			System.out.println("ID: " + tuple.f0 + " int: " + tuple.f1 + " LANGUAGE: " + tuple.f2);
			System.out.println("NAME: " + tuple.f4);
			System.out.println("TEXT: " + tuple.f3);
			System.out.println("");
		}

	}

	public static class SelectDataFlatMap extends
			JSONParseFlatMap<String, Tuple5<Long, Integer, String, String, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple5<Long, Integer, String, String, String>> out)
				throws Exception {
			try {
				out.collect(new Tuple5<Long, Integer, String, String, String>(
						getLong(value, "id"),
						getInt(value, "entities.hashtags[0].indices[1]"),
						getString(value, "lang"),
						getString(value, "text"),
						getString(value, "user.name")));
			} catch (JSONException e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Field not found");
				}
			} 
		}
	}

	public static void main(String[] args) throws Exception {

		String path = new String();

		if (args != null && args.length == 1) {
			path = args[0];
		} else {
			System.err.println("USAGE:\nTwitterStreaming <pathToPropertiesFile>");
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<String> streamSource = env.addSource(new TwitterSource(path, NUMBEROFTWEETS))
				.setParallelism(SOURCE_PARALLELISM);

		DataStream<Tuple5<Long, Integer, String, String, String>> selectedDataStream = streamSource
				.flatMap(new SelectDataFlatMap());

		selectedDataStream.addSink(new TwitterSink());

		env.execute();
	}
}
