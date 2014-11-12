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
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Example illustrating iterations in Flink streaming programs. The program will
 * sum up random numbers and counts how many additions it needs to reach a
 * specific threshold in an iterative streaming fashion.
 *
 */
public class IterateExample {

	public static void main(String[] args) throws Exception {

		// Set up our input for the stream of (0,0)s
		List<Tuple2<Double, Integer>> input = new ArrayList<Tuple2<Double, Integer>>();
		for (int i = 0; i < 1000; i++) {
			input.add(new Tuple2<Double, Integer>(0., 0));
		}

		// Obtain execution environment and use setBufferTimeout(0) to enable
		// continuous flushing of the output buffers (lowest latency).
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
				.setBufferTimeout(0);

		// Create an iterative datastream from the input.
		IterativeDataStream<Tuple2<Double, Integer>> it = env.fromCollection(input).shuffle()
				.iterate();

		// We make sure that the iteration terminates if no new data received in
		// the stream for 5 seconds
		it.setMaxWaitTime(5000);

		// We apply the stepfunction to add a new random value to the tuple and
		// increment the counter, then we split the output with our
		// outputselector.
		SplitDataStream<Tuple2<Double, Integer>> step = it.map(new Step()).shuffle()
				.split(new MySelector());

		// We close the iteration be selecting the tuples that was directed to
		// 'iterate' in the outputselector
		it.closeWith(step.select("iterate"));

		// To produce the final output we select he tuples directed to 'output'
		// than project it to the second field
		step.select("output").project(1).types(Integer.class).print();

		// Execute the streaming program
		env.execute();
	}

	/**
	 * Iteration step function which takes an input (Double , Integer) and
	 * produces an output (Double+random, Integer +1)
	 *
	 */
	public static class Step implements
			MapFunction<Tuple2<Double, Integer>, Tuple2<Double, Integer>> {

		private static final long serialVersionUID = 1L;

		Random rnd;

		public Step() {
			rnd = new Random();
		}

		@Override
		public Tuple2<Double, Integer> map(Tuple2<Double, Integer> value) throws Exception {

			return new Tuple2<Double, Integer>(value.f0 + rnd.nextDouble(), value.f1 + 1);
		}

	}

	/**
	 * s OutputSelector test which tuple needed to be iterated again.
	 */
	public static class MySelector extends OutputSelector<Tuple2<Double, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void select(Tuple2<Double, Integer> value, Collection<String> outputs) {
			if (value.f0 > 200) {
				outputs.add("output");
			} else {
				outputs.add("iterate");
			}
		}

	}
}
