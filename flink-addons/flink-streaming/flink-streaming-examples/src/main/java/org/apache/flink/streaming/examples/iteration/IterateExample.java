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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example illustrating iterations in Flink streaming.
 * 
 * <p>
 * The program sums up random numbers and counts additions it performs to reach
 * a specific threshold in an iterative streaming fashion.
 * </p>
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>streaming iterations,
 * <li>buffer timeout to enhance latency,
 * <li>directed outputs.
 * </ul>
 */
public class IterateExample {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up input for the stream of (0,0) pairs
		List<Tuple2<Double, Integer>> input = new ArrayList<Tuple2<Double, Integer>>();
		for (int i = 0; i < 1000; i++) {
			input.add(new Tuple2<Double, Integer>(0., 0));
		}

		// obtain execution environment and set setBufferTimeout(0) to enable
		// continuous flushing of the output buffers (lowest latency)
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
				.setBufferTimeout(1);

		// create an iterative data stream from the input with 5 second timeout
		IterativeDataStream<Tuple2<Double, Integer>> it = env.fromCollection(input).shuffle()
				.iterate(5000);

		// apply the step function to add new random value to the tuple and to
		// increment the counter and split the output with the output selector
		SplitDataStream<Tuple2<Double, Integer>> step = it.map(new Step()).shuffle()
				.split(new MySelector());

		// close the iteration by selecting the tuples that were directed to the
		// 'iterate' channel in the output selector
		it.closeWith(step.select("iterate"));

		// to produce the final output select the tuples directed to the
		// 'output' channel then project it to the desired second field

		DataStream<Tuple1<Integer>> numbers = step.select("output").project(1).types(Integer.class);

		// emit result
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
	 * Iteration step function which takes an input (Double , Integer) and
	 * produces an output (Double + random, Integer + 1).
	 */
	public static class Step extends
			RichMapFunction<Tuple2<Double, Integer>, Tuple2<Double, Integer>> {
		private static final long serialVersionUID = 1L;
		private transient Random rnd;

		public void open(Configuration parameters) {
			rnd = new Random();
		}

		@Override
		public Tuple2<Double, Integer> map(Tuple2<Double, Integer> value) throws Exception {
			return new Tuple2<Double, Integer>(value.f0 + rnd.nextDouble(), value.f1 + 1);
		}
	}

	/**
	 * OutputSelector testing which tuple needs to be iterated again.
	 */
	public static class MySelector implements OutputSelector<Tuple2<Double, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> select(Tuple2<Double, Integer> value) {
			List<String> output = new ArrayList<String>();
			if (value.f0 > 100) {
				output.add("output");
			} else {
				output.add("iterate");
			}
			return output;
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
