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

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

/**
 * This example uses count based tumbling windowing with multiple eviction
 * policies at the same time.
 */
public class MultiplePoliciesExample {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> stream = env.addSource(new BasicSource())
				.groupBy(0)
				.window(Count.of(2))
				.every(Count.of(3), Count.of(5))
				.reduceGroup(new Concat());

		// emit result
		if (fileOutput) {
			stream.writeAsText(outputPath, 1);
		} else {
			stream.print();
		}

		// execute the program
		env.execute("Multiple Policies Example");
	}

	/**
	 * This source function indefinitely provides String inputs for the
	 * topology.
	 */
	public static final class BasicSource implements SourceFunction<String> {

		private static final long serialVersionUID = 1L;

		private final static String STR_1 = new String("streaming");
		private final static String STR_2 = new String("flink");

		@Override
		public void invoke(Collector<String> out) throws Exception {
			// continuous emit
			while (true) {
				out.collect(STR_1);
				out.collect(STR_2);
			}
		}
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * This reduce function does a String concat.
	 */
	public static final class Concat implements GroupReduceFunction<String, String> {

		/**
		 * Auto generates version ID
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<String> values, Collector<String> out) throws Exception {
			String output = "|";
			for (String v : values) {
				output = output + v + "|";
			}
			out.collect(output);
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 1) {
				outputPath = args[0];
			} else {
				System.err.println("Usage: MultiplePoliciesExample <result path>");
				return false;
			}
		} else {
			System.out.println("Executing MultiplePoliciesExample with generated data.");
			System.out.println("  Provide parameter to write to file.");
			System.out.println("  Usage: MultiplePoliciesExample <result path>");
		}
		return true;
	}
}
