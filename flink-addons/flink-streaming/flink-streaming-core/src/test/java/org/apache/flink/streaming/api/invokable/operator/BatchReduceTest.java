/**
 *
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
 *
 */

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.junit.Test;

public class BatchReduceTest {

	private static ArrayList<Double> avgs = new ArrayList<Double>();
	private static final int BATCH_SIZE = 5;
	private static final int PARALLELISM = 1;
	private static final long MEMORYSIZE = 32;

	public static final class MyBatchReduce implements GroupReduceFunction<Double, Double> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Double> values, Collector<Double> out) throws Exception {

			Double sum = 0.;
			Double count = 0.;
			for (Double value : values) {
				sum += value;
				count++;
			}
			if (count > 0) {
				out.collect(new Double(sum / count));
			}
		}
	}

	public static final class MySink implements SinkFunction<Double> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Double tuple) {
			avgs.add(tuple);
		}

	}

	public static final class MySource implements SourceFunction<Double> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<Double> collector) {
			for (Double i = 1.; i <= 100; i++) {
				collector.collect(new Double(i));
			}
		}
	}

	public static final class MySlidingBatchReduce implements RichFunction,
			GroupReduceFunction<Long, String> {
		private static final long serialVersionUID = 1L;

		double startTime;

		@Override
		public void reduce(Iterable<Long> values, Collector<String> out) throws Exception {
			for (Long value : values) {
				out.collect(value.toString());
			}
			out.collect(END_OF_BATCH);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			startTime = (double) System.currentTimeMillis() / 1000;
		}

		@Override
		public void close() throws Exception {
		}

		@Override
		public RuntimeContext getRuntimeContext() {
			return null;
		}

		@Override
		public void setRuntimeContext(RuntimeContext t) {
			// TODO Auto-generated method stub

		}
	}

	private static List<SortedSet<String>> sink = new ArrayList<SortedSet<String>>();
	private static final String END_OF_BATCH = "end of batch";

	public static final class MySlidingSink implements SinkFunction<String> {

		private static final long serialVersionUID = 1L;

		SortedSet<String> currentSet = new TreeSet<String>();

		@Override
		public void invoke(String string) {
			if (string.equals(END_OF_BATCH)) {
				sink.add(currentSet);
				currentSet = new TreeSet<String>();
			} else {
				currentSet.add(string);
			}
		}
	}

	private final static int SLIDING_BATCH_SIZE = 9;
	private final static int SLIDE_SIZE = 6;
	private static final int SEQUENCE_SIZE = 30;
	private LocalStreamEnvironment env;
	
	private void slidingStream() {
		env.generateSequence(1, SEQUENCE_SIZE)
		.batchReduce(new MySlidingBatchReduce(), SLIDING_BATCH_SIZE, SLIDE_SIZE)
		.addSink(new MySlidingSink());
	}
	
	private void slidingTest() {
		int firstInBatch = 1;

		for (SortedSet<String> set : sink) {
			int to = Math.min(firstInBatch + SLIDING_BATCH_SIZE - 1, SEQUENCE_SIZE);
			assertEquals(getExpectedSet(to), set);
			firstInBatch += SLIDE_SIZE;
		}
	}
	
	private void nonSlidingStream() {
		env.addSource(new MySource()).batchReduce(new MyBatchReduce(), BATCH_SIZE)
		.addSink(new MySink());
	}
	
	private void nonSlidingTest() {
		for (int i = 0; i < avgs.size(); i++) {
			assertEquals(3.0 + i * BATCH_SIZE, avgs.get(i), 0);
		}
	}
	
	@Test
	public void test() {
		LogUtils.initializeDefaultConsoleLogger(Level.OFF, Level.OFF);

		env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);

		slidingStream();
		nonSlidingStream();
		
		env.executeTest(MEMORYSIZE);

		slidingTest();
		nonSlidingTest();
	}

	private SortedSet<String> getExpectedSet(int to) {
		SortedSet<String> expectedSet = new TreeSet<String>();
		for (int i = to; i > to - SLIDING_BATCH_SIZE; i--) {
			expectedSet.add(Integer.toString(i));
		}
		return expectedSet;
	}
}
