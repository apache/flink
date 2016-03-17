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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.test.distribution.TestDataDist;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
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

		DataSet<Tuple3<Integer, Long, String>> input1 = CollectionDataSets.get3TupleDataSet(env);

		final TestDataDist dist = new TestDataDist(1);

		env.setParallelism(dist.getParallelism());

		DataSet<Boolean> out1 = DataSetUtils.partitionByRange(input1, dist, 0).mapPartition(new RichMapPartitionFunction<Tuple3<Integer, Long, String>, Boolean>() {
			@Override
			public void mapPartition(Iterable<Tuple3<Integer, Long, String>> values, Collector<Boolean> out) throws Exception {
				boolean boo = true;
				int partitionIndex = getRuntimeContext().getIndexOfThisSubtask();

				for (Tuple3<Integer, Long, String> s : values) {
					if (s.f0 > (partitionIndex + 1) * 7 || s.f0 < (partitionIndex) * 7) {
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

		DataSet<Tuple3<Integer, Long, String>> input1 = CollectionDataSets.get3TupleDataSet(env);

		final TestDataDist dist = new TestDataDist(2);

		env.setParallelism(dist.getParallelism());

		DataSet<Boolean> out1 = DataSetUtils.partitionByRange(input1.map(new MapFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Integer, String>>() {
			@Override
			public Tuple3<Integer, Integer, String> map(Tuple3<Integer, Long, String> value) throws Exception {
				return new Tuple3<>(value.f0, value.f1.intValue(), value.f2);
			}
		}), dist, 0, 1).mapPartition(new RichMapPartitionFunction<Tuple3<Integer, Integer, String>, Boolean>() {
			@Override
			public void mapPartition(Iterable<Tuple3<Integer, Integer, String>> values, Collector<Boolean> out) throws Exception {
				boolean boo = true;
				int partitionIndex = getRuntimeContext().getIndexOfThisSubtask();

				for (Tuple3<Integer, Integer, String> s : values) {
					if (s.f0 > (partitionIndex + 1) * 7 || s.f0 < partitionIndex * 7 ||
							s.f1 > (partitionIndex * 2 + 3) || s.f1 < (partitionIndex * 2 + 1)) {
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
