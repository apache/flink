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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

@SuppressWarnings("serial")
public class StreamingProgram {
	
	public static void main(String[] args) throws Exception {
		
		final String jarFile = args[0];
		final String host = args[1];
		final int port = Integer.parseInt(args[2]);
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, jarFile);

		DataStream<String> text = env.fromElements(WordCountData.TEXT);

		DataStream<Tuple2<String, Integer>> counts =
				text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
						StringTokenizer tokenizer = new StringTokenizer(value);
						while (tokenizer.hasMoreTokens()){
							out.collect(new Tuple2<String, Integer>(tokenizer.nextToken(), 1));
						}
					}
				}).groupBy(0).sum(1);

		counts.addSink(new SinkFunction<Tuple2<String, Integer>>() {
			@Override
			public void invoke(Tuple2<String, Integer> value) throws Exception {
			}
		});

		env.execute();
	}
}
