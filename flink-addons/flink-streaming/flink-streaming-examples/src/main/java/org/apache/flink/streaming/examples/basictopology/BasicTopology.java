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

package org.apache.flink.streaming.examples.basictopology;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class BasicTopology {

	public static class BasicSource implements SourceFunction<String> {

		private static final long serialVersionUID = 1L;
		String str = new String("streaming");

		@Override
		public void invoke(Collector<String> out) throws Exception {
			// continuous emit
			while (true) {
				out.collect(str);
			}
		}
	}

	public static class IdentityMap implements MapFunction<String, String> {
		private static final long serialVersionUID = 1L;
		// map to the same value
		@Override
		public String map(String value) throws Exception {
			return value;
		}

	}

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<String> stream = env.addSource(new BasicSource(), SOURCE_PARALLELISM)
				.map(new IdentityMap());

		stream.print();

		env.execute();
	}
}
