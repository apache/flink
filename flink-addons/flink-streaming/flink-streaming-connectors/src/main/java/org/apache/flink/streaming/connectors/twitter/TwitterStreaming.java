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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.DataStream;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.examples.function.JSONParseFlatMap;
import org.apache.flink.util.Collector;

public class TwitterStreaming {

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	public static class TwitterSink extends SinkFunction<Tuple5<Long, Long, String, String, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple5<Long, Long, String, String, String> tuple) {
			System.out.println(tuple.f0 + " " + tuple.f1 + " " + tuple.f4);
			System.out.println("NAME: " + tuple.f2);
			System.out.println(tuple.f3);
			System.out.println(" ");
		}

	}
	
	public static class SelectDataFlatMap extends
			JSONParseFlatMap<Tuple1<String>, Tuple5<Long, Long, String, String, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple1<String> value,
				Collector<Tuple5<Long, Long, String, String, String>> out)
				throws Exception {

			out.collect(new Tuple5<Long, Long, String, String, String>(
					convertDateString2Long(getField(value.f0, "id")),
					convertDateString2LongDate(getField(value.f0, "created_at")),
					colationOfNull(getField(value.f0, "user.name")),
					colationOfNull(getField(value.f0, "text")),
					getField(value.f0, "lang")));
		}
		
		protected String colationOfNull(String in){
			if(in==null){
				return " ";
			}
			return in;
		}
		
		protected Long convertDateString2LongDate(String dateString) {
			if (dateString!=(null)) {
				String[] dateArray = dateString.split(" ");
				return Long.parseLong(dateArray[2])*100000+Long.parseLong(dateArray[5]);
			}
			return 0L;
		}
		
		protected Long convertDateString2Long(String dateString) {
			if (dateString != null) {
				return Long.parseLong(dateString);
			}
			return 0L;
		}
	}

	public static void main(String[] args) {
		
		String path = "/home/eszes/git/auth.properties";

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<Tuple1<String>> streamSource = env.addSource(
				new TwitterSource(path,100), SOURCE_PARALLELISM);

		DataStream<Tuple5<Long, Long, String, String, String>> selectedDataStream = streamSource
				.flatMap(new SelectDataFlatMap());

		selectedDataStream.addSink(new TwitterSink());

		env.execute();
	}
}
