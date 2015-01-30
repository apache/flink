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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

/**
 * This example uses count based sliding windows to illustrate different
 * possibilities for the realization of sliding windows. Take a look on the code
 * which is commented out to see different setups.
 */
public class SlidingExample {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * SIMPLE-EXAMPLE: Use this to always keep the newest 10 elements in the
		 * buffer Resulting windows will have an overlap of 5 elements
		 */

		// DataStream<String> stream = env.addSource(new CountingSource())
		// .window(Count.of(10))
		// .every(Count.of(5))
		// .reduce(new Concat());

		/*
		 * ADVANCED-EXAMPLE: Use this to have the last element of the last
		 * window as first element of the next window while the window size is
		 * always 5
		 */

		DataStream<String> stream = env.addSource(new CountingSource())
				.window(Count.of(5)
				.withDelete(4))
				.every(Count.of(4)
				.startingAt(-1))
				.reduce(new Concat());

		// emit result
		if (fileOutput) {
			stream.writeAsText(outputPath, 1);
		} else {
			stream.print();
		}

		// execute the program
		env.execute("Sliding Example");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	private static final class CountingSource implements SourceFunction<String> {
		private static final long serialVersionUID = 1L;

		private int counter = 0;

		@Override
		public void invoke(Collector<String> collector) throws Exception {
			// continuous emit
			while (true) {
				if (counter > 9999) {
					counter = 0;
				}
				collector.collect("V" + counter++);
			}
		}
	}

	/**
	 * This reduce function does a String concat.
	 */
	private static final class Concat implements ReduceFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String reduce(String value1, String value2) throws Exception {
			return value1 + "|" + value2;
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
				System.err.println("Usage: SlidingExample <result path>");
				return false;
			}
		} else {
			System.out.println("Executing SlidingExample with generated data.");
			System.out.println("  Provide parameter to write to file.");
			System.out.println("  Usage: SlidingExample <result path>");
		}
		return true;
	}

}
