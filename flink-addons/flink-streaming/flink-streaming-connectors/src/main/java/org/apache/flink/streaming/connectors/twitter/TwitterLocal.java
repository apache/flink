/**
 *
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
 *
 */

package org.apache.flink.streaming.connectors.twitter;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.examples.function.JSONParseFlatMap;
import org.apache.flink.streaming.examples.wordcount.WordCountCounter;
import org.apache.flink.util.Collector;

/**
 * This program demonstrate the use of TwitterSource. 
 * Its aim is to count the frequency of the languages of tweets
 */
public class TwitterLocal {

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	/**
	 * FlatMapFunction to determine the language of tweets if possible 
	 */
	public static class SelectLanguageFlatMap extends
			JSONParseFlatMap<Tuple1<String>, Tuple1<String>> {

		private static final long serialVersionUID = 1L;

		/**
		 * Select the language from the incoming JSON text
		 */
		@Override
		public void flatMap(Tuple1<String> value, Collector<Tuple1<String>> out) throws Exception {

			out.collect(new Tuple1<String>(colationOfNull(getField(value.f0, "lang"))));
		}

		/**
		 * Change the null String to space character. Useful when null is undesirable.
		 * @param in
		 * @return
		 */
		protected String colationOfNull(String in) {
			if (in == null) {
				return " ";
			}
			return in;
		}
	}

	public static void main(String[] args) {

		String path = new String();

		if (args != null && args.length == 1) {
			path = args[0];
		} else {
			System.err.println("USAGE:\n haho TwitterLocal itt <pathToPropertiesFile>");
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<Tuple1<String>> streamSource = env.addSource(new TwitterSource(path, 100),
				SOURCE_PARALLELISM);


		DataStream<Tuple2<String, Integer>> dataStream = streamSource
				.flatMap(new SelectLanguageFlatMap())
				.partitionBy(0)
				.map(new WordCountCounter());

		dataStream.addSink(new SinkFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void invoke(Tuple2<String, Integer> tuple) {
				System.out.println(tuple);

			}
		});

		env.execute();
	}
}
