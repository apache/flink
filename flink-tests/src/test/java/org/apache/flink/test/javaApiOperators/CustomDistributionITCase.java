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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.distribution.CustomDistribution;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.junit.Test;


import java.util.List;

import static org.junit.Assert.assertTrue;


public class CustomDistributionITCase {
	
	@Test
	public void testPartitionWithDistribution1() throws Exception{
		/*
		 * Test the record partitioned rightly with one field according to the customized data distribution
		 */

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<Tuple3<Integer, Integer, String>> input1 = env.fromElements(
				new Tuple3<>(1, 1, "Hi"),
				new Tuple3<>(1, 2, "Hello"),
				new Tuple3<>(1, 3, "Hello world"),
				new Tuple3<>(2, 4, "how are you?"),
				new Tuple3<>(2, 5, "I am fine."),
				new Tuple3<>(3, 6, "Luke Skywalker"),
				new Tuple3<>(4, 7, "Comment#1"),
				new Tuple3<>(4, 8, "Comment#2"),
				new Tuple3<>(4, 9, "Comment#3"),
				new Tuple3<>(5, 10, "Comment#4"));

		final IntValue[] keys = new IntValue[3];

		for (int i = 0; i < 3; i++) {
			keys[i] = new IntValue((i + 1) * 2);
		}

		final CustomDistribution cd = new CustomDistribution(keys);

		env.setParallelism(3);

		DataSet<Boolean> out1 = DataSetUtils.partitionByRange(input1.map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple2<IntValue, IntValue>>() {
			@Override
			public Tuple2<IntValue, IntValue> map(Tuple3<Integer, Integer, String> value) throws Exception {
				IntValue key1;
				IntValue key2;
				key1 = new IntValue(value.f0);
				key2 = new IntValue(value.f1);
				return new Tuple2<>(key1, key2);
			}
		}), cd, 0).mapPartition(new RichMapPartitionFunction<Tuple2<IntValue, IntValue>, Boolean>() {
			@Override
			public void mapPartition(Iterable<Tuple2<IntValue, IntValue>> values, Collector<Boolean> out) throws Exception {
				boolean boo = true;
				for (Tuple2<IntValue, IntValue> s : values) {
					IntValue intValues= (IntValue)cd.getBucketBoundary(getRuntimeContext().getIndexOfThisSubtask(), 3)[0];
					if (s.f0.getValue() > intValues.getValue()) {
						boo = false;
					}
				}
				out.collect(boo);
			}
		});

		List<Boolean> result = out1.collect();
		for (int i = 0; i < result.size(); i++) {
			assertTrue("The record is not emitted to the right partition", result.get(i));
		}
	}

	@Test
	public void testRangeWithDistribution2() throws Exception{
		/*
		 * Test the record partitioned rightly with two fields according to the customized data distribution
		 */

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		DataSet<Tuple2<Tuple2<Integer, Integer>, String>> input1 = env.fromElements(
				new Tuple2<>(new Tuple2<>(1, 1), "Hi"),
				new Tuple2<>(new Tuple2<>(1, 2), "Hello"),
				new Tuple2<>(new Tuple2<>(1, 3), "Hello world"),
				new Tuple2<>(new Tuple2<>(2, 4), "how are you?"),
				new Tuple2<>(new Tuple2<>(2, 5), "I am fine."),
				new Tuple2<>(new Tuple2<>(3, 6), "Luke Skywalker"),
				new Tuple2<>(new Tuple2<>(4, 7), "Comment#1"),
				new Tuple2<>(new Tuple2<>(4, 8), "Comment#2"),
				new Tuple2<>(new Tuple2<>(4, 9), "Comment#3"),
				new Tuple2<>(new Tuple2<>(5, 10), "Comment#4"));

		IntValue[][] keys = new IntValue[2][2];
		env.setParallelism(2);

		for (int i = 0; i < 2; i++) {
			keys[i][0] = new IntValue((i + 1) * 3);
		}

		for (int i = 0; i < 2; i++) {
			keys[i][1] = new IntValue((i + 1) * 5);
		}

		final CustomDistribution cd = new CustomDistribution(keys);

		DataSet<Boolean> out1 = DataSetUtils.partitionByRange(input1.map(new MapFunction<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<IntValue, IntValue>>() {
			@Override
			public Tuple2<IntValue, IntValue> map(Tuple2<Tuple2<Integer, Integer>, String> value) throws Exception {
				IntValue key1;
				IntValue key2;
				key1 = new IntValue(value.f0.f0);
				key2 = new IntValue(value.f0.f1);
				return new Tuple2<>(key1, key2);
			}
		}), cd, 0, 1).mapPartition(new RichMapPartitionFunction<Tuple2<IntValue, IntValue>, Boolean>() {
			@Override
			public void mapPartition(Iterable<Tuple2<IntValue, IntValue>> values, Collector<Boolean> out) throws Exception {
				boolean boo = true;
				for (Tuple2<IntValue, IntValue> s : values) {
					IntValue[] intValues= (IntValue[])cd.getBucketBoundary(getRuntimeContext().getIndexOfThisSubtask(), 3);
					if (s.f0.getValue() > intValues[0].getValue() || s.f1.getValue() > intValues[1].getValue()) {
						boo = false;
					}
				}
				out.collect(boo);
			}
		});

		List<Boolean> result = out1.collect();
		for (int i = 0; i < result.size(); i++) {
			assertTrue("The record is not emitted to the right partition", result.get(i));
		}
	}
}
