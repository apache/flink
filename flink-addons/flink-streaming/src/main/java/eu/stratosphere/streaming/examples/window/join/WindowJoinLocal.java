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

	public static void main(String[] args) {

		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		StreamExecutionEnvironment context = new StreamExecutionEnvironment();

		DataStream<Tuple4<String, String, Integer, Long>> source1 = context
				.addSource(new WindowJoinSourceOne());

		@SuppressWarnings("unused")
		DataStream<Tuple3<String, Integer, Integer>> source2 = context
				.addSource(new WindowJoinSourceTwo()).connectWith(source1).partitionBy(1)
				.flatMap(new WindowJoinTask()).addSink(new JoinSink());

		context.execute();

	}
}
