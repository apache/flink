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

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.TestListResultSink;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.Ignore;
import org.junit.Test;

public class WindowCrossJoinTest extends StreamingMultipleProgramsTestBase {

	private static ArrayList<Tuple2<Tuple2<Integer, String>, Integer>> joinExpectedResults = new ArrayList<Tuple2<Tuple2<Integer, String>, Integer>>();
	private static ArrayList<Tuple2<Tuple2<Integer, String>, Integer>> crossExpectedResults = new ArrayList<Tuple2<Tuple2<Integer, String>, Integer>>();

	private static class MyTimestamp<T> implements Timestamp<T> {

		private static final long serialVersionUID = 1L;

		@Override
		public long getTimestamp(T value) {
			return 101L;
		}
	}

	/**
	 * TODO: enable once new join operator is ready
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setBufferTimeout(1);

		TestListResultSink<Tuple2<Tuple2<Integer, String>, Integer>> joinResultSink =
				new TestListResultSink<Tuple2<Tuple2<Integer, String>, Integer>>();
		TestListResultSink<Tuple2<Tuple2<Integer, String>, Integer>> crossResultSink =
				new TestListResultSink<Tuple2<Tuple2<Integer, String>, Integer>>();

		ArrayList<Tuple2<Integer, String>> in1 = new ArrayList<Tuple2<Integer, String>>();
		ArrayList<Tuple1<Integer>> in2 = new ArrayList<Tuple1<Integer>>();

		in1.add(new Tuple2<Integer, String>(10, "a"));
		in1.add(new Tuple2<Integer, String>(20, "b"));
		in1.add(new Tuple2<Integer, String>(20, "x"));
		in1.add(new Tuple2<Integer, String>(0, "y"));

		in2.add(new Tuple1<Integer>(0));
		in2.add(new Tuple1<Integer>(5));
		in2.add(new Tuple1<Integer>(20));

		joinExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(20, "b"), 20));
		joinExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(20, "x"), 20));
		joinExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(0, "y"), 0));

		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(10, "a"), 0));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(10, "a"), 5));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(10, "a"), 20));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(20, "b"), 0));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(20, "b"), 5));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(20, "b"), 20));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(20, "x"), 0));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(20, "x"), 5));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(20, "x"), 20));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(0, "y"), 0));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(0, "y"), 5));
		crossExpectedResults.add(new Tuple2<Tuple2<Integer, String>, Integer>(
				new Tuple2<Integer, String>(0, "y"), 20));

		DataStream<Tuple2<Integer, String>> inStream1 = env.fromCollection(in1);
		DataStream<Tuple1<Integer>> inStream2 = env.fromCollection(in2);

		inStream1
				.join(inStream2)
				.onWindow(1000, new MyTimestamp<Tuple2<Integer, String>>(),
						new MyTimestamp<Tuple1<Integer>>(), 100).where(0).equalTo(0)
				.map(new ResultMap())
				.addSink(joinResultSink);

		env.execute();

		assertEquals(new HashSet<Tuple2<Tuple2<Integer, String>, Integer>>(joinExpectedResults),
				new HashSet<Tuple2<Tuple2<Integer, String>, Integer>>(joinResultSink.getResult()));
		assertEquals(new HashSet<Tuple2<Tuple2<Integer, String>, Integer>>(crossExpectedResults),
				new HashSet<Tuple2<Tuple2<Integer, String>, Integer>>(crossResultSink.getResult()));
	}

	private static class ResultMap implements
			MapFunction<Tuple2<Tuple2<Integer, String>, Tuple1<Integer>>,
					Tuple2<Tuple2<Integer, String>, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Tuple2<Integer, String>, Integer> map(Tuple2<Tuple2<Integer, String>, Tuple1<Integer>> value) throws Exception {
			return new Tuple2<Tuple2<Integer, String>, Integer>(value.f0, value.f1.f0);
		}
	}

}
