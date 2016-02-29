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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
@SuppressWarnings("serial")
public class WindowJoin {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		System.out.println("Usage: WindowJoin --grades <path> --salaries <path> --output <path>");

		// obtain execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// connect to the data sources for grades and salaries
		DataStream<Tuple3<Long, String, Integer>> grades = getGradesPath(env, params);
		DataStream<Tuple3<Long, String, Integer>> salaries = getSalariesPath(env, params);

		// extract the timestamps
		grades = grades.assignTimestampsAndWatermarks(new MyTimestampExtractor());
		salaries = salaries.assignTimestampsAndWatermarks(new MyTimestampExtractor());

		// apply a temporal join over the two stream based on the names over one
		// second windows
		DataStream<Tuple3<String, Integer, Integer>> joinedStream = grades
				.join(salaries)
				.where(new NameKeySelector())
				.equalTo(new NameKeySelector())
				.window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.MILLISECONDS)))
				.apply(new MyJoinFunction());

		// emit result
		if (params.has("output")) {
			joinedStream.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
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

	private static class MyTimestampExtractor extends AscendingTimestampExtractor<Tuple3<Long, String, Integer>> {

		@Override
		public long extractAscendingTimestamp(Tuple3<Long, String, Integer> element) {
			return element.f0;
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

	private static DataStream<Tuple3<Long, String, Integer>> getGradesPath(StreamExecutionEnvironment env, ParameterTool params) {
		if (params.has("grades")) {
			return env.readTextFile(params.get("grades")).map(new MySourceMap());
		} else {
			System.out.println("Executing WindowJoin example with default grades data set.");
			System.out.println("Use --grades to specify file input.");
			return env.addSource(new GradeSource());
		}
	}

	private static DataStream<Tuple3<Long, String, Integer>> getSalariesPath(StreamExecutionEnvironment env, ParameterTool params) {
		if (params.has("salaries")) {
			return env.readTextFile(params.get("salaries")).map(new MySourceMap());
		} else {
			System.out.println("Executing WindowJoin example with default salaries data set.");
			System.out.println("Use --salaries to specify file input.");
			return env.addSource(new SalarySource());
		}
	}

}
