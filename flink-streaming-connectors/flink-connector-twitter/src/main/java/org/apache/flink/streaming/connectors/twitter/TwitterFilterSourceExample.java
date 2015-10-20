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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap;
import org.apache.flink.util.Collector;

/**
 * This is an example how to use TwitterFilterSource. Before executing the
 * example you have to define the access keys of twitter.properties in the
 * resource folder. The access keys can be found in your twitter account.
 */
public class TwitterFilterSourceExample {

	/**
	 * path to the twitter properties
	 */
	private static final String PATH_TO_AUTH_FILE = "/twitter.properties";

	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		TwitterFilterSource twitterSource = new TwitterFilterSource(
				TwitterFilterSourceExample.class.getResource(PATH_TO_AUTH_FILE)
						.getFile());

		twitterSource.trackTerm("obama");
		twitterSource.filterLanguage("en");

		DataStream<String> streamSource = env.addSource(twitterSource).flatMap(
				new JSONParseFlatMap<String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(String s, Collector<String> c)
							throws Exception {
						c.collect(s);
					}
				});

		streamSource.print();

		try {
			env.execute("Twitter Streaming Test");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
