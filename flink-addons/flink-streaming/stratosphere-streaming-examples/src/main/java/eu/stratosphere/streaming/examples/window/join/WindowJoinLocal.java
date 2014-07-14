/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.examples.window.join;

import org.apache.log4j.Level;

import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.streaming.api.DataStream;
import eu.stratosphere.streaming.api.StreamExecutionEnvironment;
import eu.stratosphere.streaming.examples.join.JoinSink;
import eu.stratosphere.streaming.util.LogUtils;

public class WindowJoinLocal {

	private static final int PARALELISM = 1;
	private static final int SOURCE_PARALELISM = 1;

	// This example will join two streams with a sliding window. One which emits
	// people's grades and one which emits people's salaries.

	public static void main(String[] args) {

		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		DataStream<Tuple4<String, String, Integer, Long>> dataStream1 = env.addSource(
				new WindowJoinSourceOne(), SOURCE_PARALELISM);

		DataStream<Tuple3<String, Integer, Integer>> dataStream2 = env
				.addSource(new WindowJoinSourceTwo(), SOURCE_PARALELISM)
				.connectWith(dataStream1)
				.partitionBy(1)
				.flatMap(new WindowJoinTask(), PARALELISM)
				.addSink(new JoinSink());
		
		dataStream2.print();

		env.execute();

	}
}
