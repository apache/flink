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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;

import java.util.Random;

/**
 * Example illustrating join over sliding windows of streams in Flink.
 * <p/>
 * <p>
 * his example will join two streams with a sliding window. One which emits
 * grades and one which emits salaries of people.
 * </p>
 * <p/>
 * <p/>
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

	private static DataStream<Tuple2<String, Integer>> grades;
	private static DataStream<Tuple2<String, Integer>> salaries;

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// obtain execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// connect to the data sources for grades and salaries
		setInputStreams(env);

		// apply a temporal join over the two stream based on the names over one
		// second windows
		DataStream<Tuple3<String, Integer, Integer>> joinedStream = grades
				.join(salaries)
				.onWindow(1, new MyTimestamp(0), new MyTimestamp(0))
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

	private final static String[] names = {"tom", "jerry", "alice", "bob", "john", "grace"};
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
		public boolean reachedEnd() throws Exception {
			return false;
		}

		@Override
		public Tuple2<String, Integer> next() throws Exception {
			outTuple.f0 = names[rand.nextInt(names.length)];
			outTuple.f1 = rand.nextInt(GRADE_COUNT) + 1;
			Thread.sleep(rand.nextInt(SLEEP_TIME) + 1);
			return outTuple;
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
		public boolean reachedEnd() throws Exception {
			return false;
		}

		@Override
		public Tuple2<String, Integer> next() throws Exception {
			outTuple.f0 = names[rand.nextInt(names.length)];
			outTuple.f1 = rand.nextInt(SALARY_MAX) + 1;
			Thread.sleep(rand.nextInt(SLEEP_TIME) + 1);
			return outTuple;
		}

	}

	public static class MySourceMap extends RichMapFunction<String, Tuple2<String, Integer>> {

		private String[] record;

		public MySourceMap() {
			record = new String[2];
		}

		@Override
		public Tuple2<String, Integer> map(String line) throws Exception {
			record = line.substring(1, line.length() - 1).split(",");
			return new Tuple2<String, Integer>(record[0], Integer.parseInt(record[1]));
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

	public static class MyTimestamp implements Timestamp<Tuple2<String, Integer>> {
		private int counter;

		public MyTimestamp(int starttime) {
			this.counter = starttime;
		}

		@Override
		public long getTimestamp(Tuple2<String, Integer> value) {
			counter += SLEEP_TIME;
			return counter;
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileInput = false;
	private static boolean fileOutput = false;

	private static String gradesPath;
	private static String salariesPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			if (args.length == 1) {
				fileOutput = true;
				outputPath = args[0];
			} else if (args.length == 3) {
				fileInput = true;
				fileOutput = true;
				gradesPath = args[0];
				salariesPath = args[1];
				outputPath = args[2];
			} else {
				System.err.println("Usage: WindowJoin <result path> or WindowJoin <input path 1> <input path 1> " +
						"<result path>");
				return false;
			}
		} else {
			System.out.println("Executing WindowJoin with generated data.");
			System.out.println("  Provide parameter to write to file.");
			System.out.println("  Usage: WindowJoin <result path>");
		}
		return true;
	}

	private static void setInputStreams(StreamExecutionEnvironment env) {
		if (fileInput) {
			grades = env.readTextFile(gradesPath).map(new MySourceMap());
			salaries = env.readTextFile(salariesPath).map(new MySourceMap());
		} else {
			grades = env.addSource(new GradeSource());
			salaries = env.addSource(new SalarySource());
		}
	}
}
