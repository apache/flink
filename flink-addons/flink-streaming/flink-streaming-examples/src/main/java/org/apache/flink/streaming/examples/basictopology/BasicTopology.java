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

package org.apache.flink.streaming.examples.basictopology;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class BasicTopology {

	public static class BasicSource implements SourceFunction<Tuple1<String>> {

		private static final long serialVersionUID = 1L;
		Tuple1<String> tuple = new Tuple1<String>("streaming");

		@Override
		public void invoke(Collector<Tuple1<String>> out) throws Exception {
			// continuously emit a tuple
			while (true) {
				out.collect(tuple);
			}
		}
	}

	public static class BasicMap implements MapFunction<Tuple1<String>, Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		// map to the same tuple
		@Override
		public Tuple1<String> map(Tuple1<String> value) throws Exception {
			return value;
		}

	}

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<Tuple1<String>> stream = env.addSource(new BasicSource(), SOURCE_PARALLELISM)
				.map(new BasicMap());

		stream.print();

		env.execute();
	}
}
