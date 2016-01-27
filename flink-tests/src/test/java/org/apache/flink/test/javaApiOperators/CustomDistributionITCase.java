/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.javaApiOperators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.distribution.CustomDistribution;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.junit.Test;


import static org.junit.Assert.assertEquals;


public class CustomDistributionITCase {
	
	@Test
	public void testRangeWithDistribution1() throws Exception{

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<Tuple3<Integer, Integer, String>> input1 = env.fromElements(
				new Tuple3<>(1, 1, "Hi"),
				new Tuple3<>(1, 2, "Hello"),
				new Tuple3<>(1, 3, "Hello world"),
				new Tuple3<>(2, 4, "Hello world, how are you?"),
				new Tuple3<>(2, 5, "I am fine."),
				new Tuple3<>(3, 6, "Luke Skywalker"),
				new Tuple3<>(4, 7, "Comment#1"),
				new Tuple3<>(4, 8, "Comment#2"),
				new Tuple3<>(4, 9, "Comment#3"),
				new Tuple3<>(5, 10, "Comment#4"));
		
		IntValue[] keys = new IntValue[4];
		
		env.setParallelism(5);

		for (int i = 0; i < keys.length; i++)
		{
			keys[i] = new IntValue(i + 1);
		}

		CustomDistribution cd = new CustomDistribution(keys);

		DataSet<Tuple1<IntValue>> out1 = DataSetUtils.partitionByRange(input1.mapPartition(
				new MapPartitionFunction<Tuple3<Integer, Integer, String>, Tuple1<IntValue>>() {
			public void mapPartition(Iterable<Tuple3<Integer, Integer, String>> values, Collector<Tuple1<IntValue>> out) {
				IntValue key1;
				for (Tuple3<Integer, Integer, String> s : values) {
					key1 = new IntValue(s.f0);
					out.collect(new Tuple1<>(key1));
				}
			}
		}), cd, 0).groupBy(0).sum(0);

		String expected = "[(3), (4), (3), (12), (5)]";
		assertEquals(expected, out1.collect().toString());
	}

	@Test
	public void testRangeWithDistribution2() throws Exception{

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<Tuple2<Tuple2<Integer, Integer>, String>> input1 = env.fromElements(
				new Tuple2<>(new Tuple2<>(1, 1), "Hi"),
				new Tuple2<>(new Tuple2<>(1, 2), "Hello"),
				new Tuple2<>(new Tuple2<>(1, 3), "Hello world"),
				new Tuple2<>(new Tuple2<>(2, 4), "Hello world, how are you?"),
				new Tuple2<>(new Tuple2<>(2, 5), "I am fine."),
				new Tuple2<>(new Tuple2<>(3, 6), "Luke Skywalker"),
				new Tuple2<>(new Tuple2<>(4, 7), "Comment#1"),
				new Tuple2<>(new Tuple2<>(4, 8), "Comment#2"),
				new Tuple2<>(new Tuple2<>(4, 9), "Comment#3"),
				new Tuple2<>(new Tuple2<>(5, 10), "Comment#4"));

		IntValue[][] keys = new IntValue[2][2];

		env.setParallelism(3);

		for (int i = 0; i < 2; i++)
		{
			for (int j = 0; j < 2; j++)
			{
				keys[i][j] = new IntValue(i + j);
			}
		}

		CustomDistribution cd = new CustomDistribution(keys);

		DataSet<Tuple1<IntValue>> out1= DataSetUtils.partitionByRange(input1.mapPartition(
				new MapPartitionFunction<Tuple2<Tuple2<Integer, Integer>, String>, Tuple1<Tuple2<IntValue, IntValue>>>() {
					public void mapPartition(Iterable<Tuple2<Tuple2<Integer, Integer>, String>> values, Collector<Tuple1<Tuple2<IntValue, IntValue>>> out) {
						IntValue key1;
						IntValue key2;
						for (Tuple2<Tuple2<Integer, Integer>, String> s : values) {
							key1 = new IntValue(s.f0.f0);
							key2 = new IntValue(s.f0.f1);
							out.collect(new Tuple1<>(new Tuple2<>(key1, key2)));
						}
					}
				}), cd, 0).mapPartition(new MapPartitionFunction<Tuple1<Tuple2<IntValue, IntValue>>, Tuple1<IntValue>>() {
			public void mapPartition(Iterable<Tuple1<Tuple2<IntValue, IntValue>>> values, Collector<Tuple1<IntValue>> out) {
				Tuple1<IntValue> key;
				for (Tuple1<Tuple2<IntValue, IntValue>> s : values) {
					key = new Tuple1<>(s.f0.f0);
					out.collect(key);
				}
			}
		}).groupBy(0).sum(0);

		String expected = "[(1), (4), (2), (3), (5), (12)]";
		assertEquals(expected, out1.collect().toString());
	}
}
