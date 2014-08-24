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

package org.apache.flink.streaming.examples.window.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.log4j.Level;

public class WindowJoinLocal {

	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;

	// This example will join two streams with a sliding window. One which emits
	// people's grades and one which emits people's salaries.

	public static void main(String[] args) {

		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(PARALLELISM);

		DataStream<Tuple4<String, String, Integer, Long>> dataStream1 = env.addSource(
				new WindowJoinSourceOne(), SOURCE_PARALLELISM);

		@SuppressWarnings("unchecked")
		DataStream<Tuple3<String, Integer, Integer>> dataStream2 = env
				.addSource(new WindowJoinSourceTwo(), SOURCE_PARALLELISM).merge(dataStream1)
				.partitionBy(1).flatMap(new WindowJoinTask());

		dataStream2.print();

		env.execute();

	}
}
