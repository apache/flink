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

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.util.MockInvokable;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class GroupedWindowGroupReduceInvokableTest {

	@Test
	public void windowReduceTest() {
		List<Tuple2<String, Integer>> inputs2 = new ArrayList<Tuple2<String, Integer>>();
		inputs2.add(new Tuple2<String, Integer>("a", 1));
		inputs2.add(new Tuple2<String, Integer>("a", 2));
		inputs2.add(new Tuple2<String, Integer>("b", 2));
		inputs2.add(new Tuple2<String, Integer>("b", 2));
		inputs2.add(new Tuple2<String, Integer>("b", 5));
		inputs2.add(new Tuple2<String, Integer>("a", 7));
		inputs2.add(new Tuple2<String, Integer>("b", 9));
		inputs2.add(new Tuple2<String, Integer>("b", 10));
		//1,2-4,5-7,8-10

		List<Tuple2<String, Integer>> expected2 = new ArrayList<Tuple2<String, Integer>>();
		expected2.add(new Tuple2<String, Integer>("a", 3));
		expected2.add(new Tuple2<String, Integer>("b", 4));
		expected2.add(new Tuple2<String, Integer>("b", 5));
		expected2.add(new Tuple2<String, Integer>("a", 7));
		expected2.add(new Tuple2<String, Integer>("b", 10));

		GroupedWindowGroupReduceInvokable<Tuple2<String, Integer>, Tuple2<String, Integer>> invokable2 = new GroupedWindowGroupReduceInvokable<Tuple2<String, Integer>, Tuple2<String, Integer>>(
				new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void reduce(Iterable<Tuple2<String, Integer>> values,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						Tuple2<String, Integer> outTuple = new Tuple2<String, Integer>("", 0);
						
						for (@SuppressWarnings("unused") Tuple2<String, Integer> value : values) {
						}
						
						for (Tuple2<String, Integer> value : values) {
							outTuple.f0 = value.f0;
							outTuple.f1 += value.f1;
						}
						out.collect(outTuple);
					}
				}, 2, 3, 0, new TimeStamp<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public long getTimestamp(Tuple2<String, Integer> value) {
						return value.f1;
					}

					@Override
					public long getStartTime() {
						return 1;
					}
				});

		List<Tuple2<String, Integer>> actual2 = MockInvokable.createAndExecute(invokable2, inputs2);

		
		assertEquals(new HashSet<Tuple2<String, Integer>>(expected2),
				new HashSet<Tuple2<String, Integer>>(actual2));
		assertEquals(expected2.size(), actual2.size());

	}

}
