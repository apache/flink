/**
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

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.util.MockInvokable;
import org.apache.flink.util.Collector;
import org.junit.Test;


public class BatchReduceTest {

	public static final class MySlidingBatchReduce implements GroupReduceFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Integer> values, Collector<String> out) throws Exception {
			for (Integer value : values) {
				out.collect(value.toString());
			}
			out.collect(END_OF_BATCH);
		}
	}

	private final static String END_OF_BATCH = "end of batch";
	private final static int SLIDING_BATCH_SIZE = 3;
	private final static int SLIDE_SIZE = 2;

	@Test
	public void slidingBatchReduceTest() {
		BatchReduceInvokable<Integer, String> invokable = new BatchReduceInvokable<Integer, String>(
				new MySlidingBatchReduce(), SLIDING_BATCH_SIZE, SLIDE_SIZE);

		List<String> expected = Arrays.asList("1", "2", "3", END_OF_BATCH, "3", "4", "5",
				END_OF_BATCH, "5", "6", "7", END_OF_BATCH);
		List<String> actual = MockInvokable.createAndExecute(invokable,
				Arrays.asList(1, 2, 3, 4, 5, 6, 7));

		assertEquals(expected, actual);
	}

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
	
	private static final int BATCH_SIZE = 5;

	@Test
	public void nonSlidingBatchReduceTest() {
		List<Double> inputs = new ArrayList<Double>();
		for (Double i = 1.; i <= 100; i++) {
			inputs.add(i);
		}
		
		BatchReduceInvokable<Double, Double> invokable = new BatchReduceInvokable<Double, Double>(new MyBatchReduce(), BATCH_SIZE, BATCH_SIZE);
		
		List<Double> avgs = MockInvokable.createAndExecute(invokable, inputs);

		for (int i = 0; i < avgs.size(); i++) {
			assertEquals(3.0 + i * BATCH_SIZE, avgs.get(i), 0);
		}
	}
}
