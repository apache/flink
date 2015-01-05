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

package org.apache.flink.streaming.examples.join;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.RichSourceFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Example illustrating join over sliding windows of streams in Flink.
 * 
 * <p>
 * his example will join two streams with a sliding window. One which emits
 * grades and one which emits salaries of people.
 * </p>
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>do windowed joins,
 * <li>use tuple data types,
 * <li>write a simple streaming program.
 */
public class WindowJoin {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// obtain execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// connect to the data sources for grades and salaries
		DataStream<Tuple2<String, Integer>> grades = env.addSource(new GradeSource());
		DataStream<Tuple2<String, Integer>> salaries = env.addSource(new SalarySource());

		// apply a temporal join over the two stream based on the names over one
		// second windows
		DataStream<Tuple3<String, Integer, Integer>> joinedStream = grades
						.join(salaries)
						.onWindow(1, TimeUnit.SECONDS)
						.where(0)
						.equalTo(0)
						.with(new MyJoinFunction());
		
		// emit result
		if (fileOutput) {
			joinedStream.writeAsText(outputPath, 1);
		} else {
			joinedStream.print();
		}

		// execute program
		env.execute("Windowed Join Example");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	private final static String[] names = { "tom", "jerry", "alice", "bob", "john", "grace" };
	private final static int GRADE_COUNT = 5;
	private final static int SALARY_MAX = 10000;
	private final static int SLEEP_TIME = 10;

	/**
	 * Continuously emit tuples with random names and integers (grades).
	 */
	public static class GradeSource implements SourceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private Random rand;
		private Tuple2<String, Integer> outTuple;

		public GradeSource() {
			rand = new Random();
			outTuple = new Tuple2<String, Integer>();
		}

		@Override
		public void invoke(Collector<Tuple2<String, Integer>> out) throws Exception {
			while (true) {
				outTuple.f0 = names[rand.nextInt(names.length)];
				outTuple.f1 = rand.nextInt(GRADE_COUNT) + 1;
				out.collect(outTuple);
				Thread.sleep(rand.nextInt(SLEEP_TIME) + 1);
			}
		}
	}

	/**
	 * Continuously emit tuples with random names and integers (salaries).
	 */
	public static class SalarySource extends RichSourceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private transient Random rand;
		private transient Tuple2<String, Integer> outTuple;

		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			rand = new Random();
			outTuple = new Tuple2<String, Integer>();
		}

		@Override
		public void invoke(Collector<Tuple2<String, Integer>> out) throws Exception {
			while (true) {
				outTuple.f0 = names[rand.nextInt(names.length)];
				outTuple.f1 = rand.nextInt(SALARY_MAX) + 1;
				out.collect(outTuple);
				Thread.sleep(rand.nextInt(SLEEP_TIME) + 1);
			}
		}
	}

	public static class MyJoinFunction
			implements
			JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		private Tuple3<String, Integer, Integer> joined = new Tuple3<String, Integer, Integer>();

		@Override
		public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first,
				Tuple2<String, Integer> second) throws Exception {
			joined.f0 = first.f0;
			joined.f1 = first.f1;
			joined.f2 = second.f1;
			return joined;
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
				System.err.println("Usage: WindowJoin <result path>");
				return false;
			}
		} else {
			System.out.println("Executing WindowJoin with generated data.");
			System.out.println("  Provide parameter to write to file.");
			System.out.println("  Usage: WindowJoin <result path>");
		}
		return true;
	}
}
