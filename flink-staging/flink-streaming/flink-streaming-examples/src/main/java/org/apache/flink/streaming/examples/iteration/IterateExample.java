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

package org.apache.flink.streaming.examples.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Example illustrating iterations in Flink streaming. <p/> <p> The program sums up random numbers and counts additions
 * it performs to reach a specific threshold in an iterative streaming fashion. </p>
 * <p/>
 * <p/>
 * This example shows how to use: <ul> <li>streaming iterations, <li>buffer timeout to enhance latency, <li>directed
 * outputs. </ul>
 */
public class IterateExample {

	private static final int BOUND = 100;

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up input for the stream of integer pairs

		// obtain execution environment and set setBufferTimeout to 1 to enable
		// continuous flushing of the output buffers (lowest latency)
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
				.setBufferTimeout(1);

		// create input stream of integer pairs
		DataStream<Tuple2<Integer, Integer>> inputStream;
		if (fileInput) {
			inputStream = env.readTextFile(inputPath).map(new FibonacciInputMap());
		} else {
			inputStream = env.addSource(new RandomFibonacciSource());
		}

		// create an iterative data stream from the input with 5 second timeout
		IterativeDataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = inputStream.map(new InputMap())
				.iterate(5000);

		// apply the step function to get the next Fibonacci number
		// increment the counter and split the output with the output selector
		SplitDataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it.map(new Step())
				.split(new MySelector());

		// close the iteration by selecting the tuples that were directed to the
		// 'iterate' channel in the output selector
		it.closeWith(step.select("iterate"));

		// to produce the final output select the tuples directed to the
		// 'output' channel then get the input pairs that have the greatest iteration counter
		// on a 1 second sliding window
		DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output")
				.map(new OutputMap());

		// emit results
		if (fileOutput) {
			numbers.writeAsText(outputPath, 1);
		} else {
			numbers.print();
		}

		// execute the program
		env.execute("Streaming Iteration Example");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Generate random integer pairs from the range from 0 to BOUND/2
	 */
	private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		private Random rnd = new Random();

		@Override
		public boolean reachedEnd() throws Exception {
			return false;
		}

		@Override
		public Tuple2<Integer, Integer> next() throws Exception {
			int first = rnd.nextInt(BOUND / 2 - 1) + 1;
			int second = rnd.nextInt(BOUND / 2 - 1) + 1;

			Thread.sleep(500L);
			return new Tuple2<Integer, Integer>(first, second);
		}

	}

	/**
	 * Generate random integer pairs from the range from 0 to BOUND/2
	 */
	private static class FibonacciInputMap implements MapFunction<String, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(String value) throws Exception {
			String record = value.substring(1, value.length() - 1);
			String[] splitted = record.split(",");
			return new Tuple2<Integer, Integer>(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]));
		}
	}

	/**
	 * Map the inputs so that the next Fibonacci numbers can be calculated while preserving the original input tuple A
	 * counter is attached to the tuple and incremented in every iteration step
	 */
	public static class InputMap implements MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer,
			Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws
				Exception {
			return new Tuple5<Integer, Integer, Integer, Integer, Integer>(value.f0, value.f1, value.f0, value.f1, 0);
		}
	}

	/**
	 * Iteration step function that calculates the next Fibonacci number
	 */
	public static class Step implements
			MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer,
					Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer,
				Integer> value) throws Exception {
			return new Tuple5<Integer, Integer, Integer, Integer, Integer>(value.f0, value.f1, value.f3, value.f2 +
					value.f3, ++value.f4);
		}
	}

	/**
	 * OutputSelector testing which tuple needs to be iterated again.
	 */
	public static class MySelector implements OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
			List<String> output = new ArrayList<String>();
			if (value.f2 < BOUND && value.f3 < BOUND) {
				output.add("iterate");
			} else {
				output.add("output");
			}
			return output;
		}
	}

	/**
	 * Giving back the input pair and the counter
	 */
	public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
			Tuple2<Tuple2<Integer, Integer>, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer>
				value) throws
				Exception {
			return new Tuple2<Tuple2<Integer, Integer>, Integer>(new Tuple2<Integer, Integer>(value.f0, value.f1),
					value.f4);
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileInput = false;
	private static boolean fileOutput = false;
	private static String inputPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			if (args.length == 1) {
				fileOutput = true;
				outputPath = args[0];
			} else if (args.length == 2) {
				fileInput = true;
				inputPath = args[0];
				fileOutput = true;
				outputPath = args[1];
			} else {
				System.err.println("Usage: IterateExample <result path>");
				return false;
			}
		} else {
			System.out.println("Executing IterateExample with generated data.");
			System.out.println("  Provide parameter to write to file.");
			System.out.println("  Usage: IterateExample <result path>");
		}
		return true;
	}

}
