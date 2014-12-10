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

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.junit.Test;

public class WindowCrossJoinTest implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final long MEMORYSIZE = 32;

	private static ArrayList<Tuple2<Tuple2<Integer, String>, Integer>> joinResults = new ArrayList<Tuple2<Tuple2<Integer, String>, Integer>>();
	private static ArrayList<Tuple2<Tuple2<Integer, String>, Integer>> joinExpectedResults = new ArrayList<Tuple2<Tuple2<Integer, String>, Integer>>();

	private static ArrayList<Tuple2<Tuple2<Integer, String>, Integer>> crossResults = new ArrayList<Tuple2<Tuple2<Integer, String>, Integer>>();
	private static ArrayList<Tuple2<Tuple2<Integer, String>, Integer>> crossExpectedResults = new ArrayList<Tuple2<Tuple2<Integer, String>, Integer>>();

	@Test
	public void test() throws Exception {
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		env.setBufferTimeout(1);

		ArrayList<Tuple2<Integer, String>> in1 = new ArrayList<Tuple2<Integer, String>>();
		ArrayList<Integer> in2 = new ArrayList<Integer>();

		in1.add(new Tuple2<Integer, String>(10, "a"));
		in1.add(new Tuple2<Integer, String>(20, "b"));
		in1.add(new Tuple2<Integer, String>(20, "x"));
		in1.add(new Tuple2<Integer, String>(0, "y"));

		in2.add(0);
		in2.add(5);
		in2.add(20);

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
		DataStream<Integer> inStream2 = env.fromCollection(in2);

		inStream1.join(inStream2).onWindow(1000, 1000, new MyTimestamp1(), new MyTimestamp2())
				.where(0).equalTo(0).addSink(new JoinResultSink());

		inStream1.cross(inStream2).onWindow(1000, 1000, new MyTimestamp1(), new MyTimestamp2())
				.with(new CrossFunction<Tuple2<Integer,String>, Integer, Tuple2<Tuple2<Integer,String>, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Tuple2<Integer, String>, Integer> cross(
							Tuple2<Integer, String> val1, Integer val2) throws Exception {
						return new Tuple2<Tuple2<Integer,String>, Integer>(val1, val2);
					}
				})
				.addSink(new CrossResultSink());

		env.executeTest(MEMORYSIZE);

		assertEquals(joinExpectedResults, joinResults);
		assertEquals(crossExpectedResults, crossResults);
	}

	private static class MyTimestamp1 implements TimeStamp<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long getTimestamp(Tuple2<Integer, String> value) {
			return 101L;
		}

		@Override
		public long getStartTime() {
			return 100L;
		}
	}

	private static class MyTimestamp2 implements TimeStamp<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public long getTimestamp(Integer value) {
			return 101L;
		}

		@Override
		public long getStartTime() {
			return 100L;
		}
	}

	private static class JoinResultSink implements
			SinkFunction<Tuple2<Tuple2<Integer, String>, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple2<Tuple2<Integer, String>, Integer> value) {
			joinResults.add(value);
		}
	}

	private static class CrossResultSink implements
			SinkFunction<Tuple2<Tuple2<Integer, String>, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple2<Tuple2<Integer, String>, Integer> value) {
			crossResults.add(value);
		}
	}
}
