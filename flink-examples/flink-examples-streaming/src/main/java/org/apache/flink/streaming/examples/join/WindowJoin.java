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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Example illustrating join over sliding windows of streams in Flink.
 *
 * <p>
 * This example will join two streams with a sliding window. One which emits grades and one which
 * emits salaries of people. The input format for both sources has an additional timestamp
 * as field 0. This is used to to event-time windowing. Time timestamps must be
 * monotonically increasing.
 *
 * This example shows how to:
 * <ul>
 *   <li>do windowed joins,
 *   <li>use tuple data types,
 *   <li>write a simple streaming program.
 * </ul>
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
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// connect to the data sources for grades and salaries
		Tuple2<DataStream<Tuple3<Long, String, Integer>>, DataStream<Tuple3<Long, String, Integer>>> input = getInputStreams(env);
		DataStream<Tuple3<Long, String, Integer>> grades = input.f0;
		DataStream<Tuple3<Long, String, Integer>> salaries = input.f1;

		// extract the timestamps
		grades = grades.assignTimestamps(new MyTimestampExtractor());
		salaries = salaries.assignTimestamps(new MyTimestampExtractor());

		// apply a temporal join over the two stream based on the names over one
		// second windows
		DataStream<Tuple3<String, Integer, Integer>> joinedStream = grades
				.join(salaries)
				.where(new NameKeySelector())
				.equalTo(new NameKeySelector())
				.window(TumblingTimeWindows.of(Time.of(5, TimeUnit.MILLISECONDS)))
				.apply(new MyJoinFunction());

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
	public static class GradeSource implements SourceFunction<Tuple3<Long, String, Integer>> {
		private static final long serialVersionUID = 1L;

		private Random rand;
		private Tuple3<Long, String, Integer> outTuple;
		private volatile boolean isRunning = true;
		private int counter;

		public GradeSource() {
			rand = new Random();
			outTuple = new Tuple3<>();
		}

		@Override
		public void run(SourceContext<Tuple3<Long, String, Integer>> ctx) throws Exception {
			while (isRunning && counter < 100) {
				outTuple.f0 = System.currentTimeMillis();
				outTuple.f1 = names[rand.nextInt(names.length)];
				outTuple.f2 = rand.nextInt(GRADE_COUNT) + 1;
				Thread.sleep(rand.nextInt(SLEEP_TIME) + 1);
				counter++;
				ctx.collect(outTuple);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	/**
	 * Continuously emit tuples with random names and integers (salaries).
	 */
	public static class SalarySource extends RichSourceFunction<Tuple3<Long, String, Integer>> {
		private static final long serialVersionUID = 1L;

		private transient Random rand;
		private transient Tuple3<Long, String, Integer> outTuple;
		private volatile boolean isRunning;
		private int counter;

		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			rand = new Random();
			outTuple = new Tuple3<Long, String, Integer>();
			isRunning = true;
		}


		@Override
		public void run(SourceContext<Tuple3<Long, String, Integer>> ctx) throws Exception {
			while (isRunning && counter < 100) {
				outTuple.f0 = System.currentTimeMillis();
				outTuple.f1 = names[rand.nextInt(names.length)];
				outTuple.f2 = rand.nextInt(SALARY_MAX) + 1;
				Thread.sleep(rand.nextInt(SLEEP_TIME) + 1);
				counter++;
				ctx.collect(outTuple);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	public static class MySourceMap extends RichMapFunction<String, Tuple3<Long, String, Integer>> {

		private static final long serialVersionUID = 1L;

		private String[] record;

		public MySourceMap() {
			record = new String[2];
		}

		@Override
		public Tuple3<Long, String, Integer> map(String line) throws Exception {
			record = line.substring(1, line.length() - 1).split(",");
			return new Tuple3<>(Long.parseLong(record[0]), record[1], Integer.parseInt(record[2]));
		}
	}

	public static class MyJoinFunction
			implements
			JoinFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, Tuple3<String, Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		private Tuple3<String, Integer, Integer> joined = new Tuple3<>();

		@Override
		public Tuple3<String, Integer, Integer> join(Tuple3<Long, String, Integer> first,
				Tuple3<Long, String, Integer> second) throws Exception {
			joined.f0 = first.f1;
			joined.f1 = first.f2;
			joined.f2 = second.f2;
			return joined;
		}
	}

	private static class MyTimestampExtractor implements TimestampExtractor<Tuple3<Long, String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractTimestamp(Tuple3<Long, String, Integer> element, long currentTimestamp) {
			return element.f0;
		}

		@Override
		public long extractWatermark(Tuple3<Long, String, Integer> element, long currentTimestamp) {
			return element.f0 - 1;
		}

		@Override
		public long getCurrentWatermark() {
			return Long.MIN_VALUE;
		}
	}

	private static class NameKeySelector implements KeySelector<Tuple3<Long, String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple3<Long, String, Integer> value) throws Exception {
			return value.f1;
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
				System.err.println("Usage: WindowJoin <result path> or WindowJoin <input path 1> <input path 2> " +
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

	private static Tuple2<DataStream<Tuple3<Long, String, Integer>>, DataStream<Tuple3<Long, String, Integer>>> getInputStreams(
			StreamExecutionEnvironment env) {

		DataStream<Tuple3<Long, String, Integer>> grades;
		DataStream<Tuple3<Long, String, Integer>> salaries;

		if (fileInput) {
			grades = env.readTextFile(gradesPath).map(new MySourceMap());
			salaries = env.readTextFile(salariesPath).map(new MySourceMap());
		} else {
			grades = env.addSource(new GradeSource());
			salaries = env.addSource(new SalarySource());
		}

		return Tuple2.of(grades, salaries);
	}

}
