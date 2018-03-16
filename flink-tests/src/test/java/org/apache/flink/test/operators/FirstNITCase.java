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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Integration tests for {@link DataSet#first}.
 */
@RunWith(Parameterized.class)
public class FirstNITCase extends MultipleProgramsTestBase {
	public FirstNITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testFirstNOnUngroupedDS() throws Exception {
		/*
		 * First-n on ungrouped data set
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple1<Integer>> seven = ds.first(7).map(new OneMapper()).sum(0);

		List<Tuple1<Integer>> result = seven.collect();

		String expected = "(7)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testFirstNOnGroupedDS() throws Exception {
		/*
		 * First-n on grouped data set
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple2<Long, Integer>> first = ds.groupBy(1).first(4)
				.map(new OneMapper2()).groupBy(0).sum(1);

		List<Tuple2<Long, Integer>> result = first.collect();

		String expected = "(1,1)\n(2,2)\n(3,3)\n(4,4)\n(5,4)\n(6,4)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testFirstNOnGroupedAndSortedDS() throws Exception {
		/*
		 * First-n on grouped and sorted data set
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple2<Long, Integer>> first = ds.groupBy(1).sortGroup(0, Order.DESCENDING).first(3)
				.project(1, 0);

		List<Tuple2<Long, Integer>> result = first.collect();

		String expected = "(1,1)\n"
				+ "(2,3)\n(2,2)\n"
				+ "(3,6)\n(3,5)\n(3,4)\n"
				+ "(4,10)\n(4,9)\n(4,8)\n"
				+ "(5,15)\n(5,14)\n(5,13)\n"
				+ "(6,21)\n(6,20)\n(6,19)\n";

		compareResultAsText(result, expected);
	}

	/**
	 * Test for FLINK-2135.
	 */
	@Test
	public void testFaultyCast() throws Exception {
		ExecutionEnvironment ee = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> b = ee.fromElements("a", "b");
		GroupReduceOperator<String, String> a = b.groupBy(new KeySelector<String, Long>() {
			@Override
			public Long getKey(String value) throws Exception {
				return 1L;
			}
		}).sortGroup(new KeySelector<String, Double>() {
			@Override
			public Double getKey(String value) throws Exception {
				return 1.0;
			}
		}, Order.DESCENDING).first(1);

		List<String> result = b.collect();

		String expected = "a\nb";

		compareResultAsText(result, expected);
	}

	private static class OneMapper implements MapFunction<Tuple3<Integer, Long, String>, Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;
		private final Tuple1<Integer> one = new Tuple1<Integer>(1);
		@Override
		public Tuple1<Integer> map(Tuple3<Integer, Long, String> value) {
			return one;
		}
	}

	private static class OneMapper2 implements MapFunction<Tuple3<Integer, Long, String>, Tuple2<Long, Integer>> {
		private static final long serialVersionUID = 1L;
		private final Tuple2<Long, Integer> one = new Tuple2<>(0L, 1);

		@Override
		public Tuple2<Long, Integer> map(Tuple3<Integer, Long, String> value) {
			one.f0 = value.f1;
			return one;
		}
	}

}
