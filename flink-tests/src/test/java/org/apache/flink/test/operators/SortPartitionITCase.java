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
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Tests for {@link DataSet#sortPartition}.
 */
@RunWith(Parameterized.class)
public class SortPartitionITCase extends MultipleProgramsTestBase {

	public SortPartitionITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testSortPartitionByKeyField() throws Exception {
		/*
		 * Test sort partition on key field
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		List<Tuple1<Boolean>> result = ds
				.map(new IdMapper<Tuple3<Integer, Long, String>>()).setParallelism(4) // parallelize input
				.sortPartition(1, Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<>(new Tuple3Checker()))
				.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testSortPartitionByTwoKeyFields() throws Exception {
		/*
		 * Test sort partition on two key fields
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		List<Tuple1<Boolean>> result = ds
				.map(new IdMapper<Tuple5<Integer, Long, Integer, String, Long>>()).setParallelism(2) // parallelize input
				.sortPartition(4, Order.ASCENDING)
				.sortPartition(2, Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<>(new Tuple5Checker()))
				.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSortPartitionByFieldExpression() throws Exception {
		/*
		 * Test sort partition on field expression
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		List<Tuple1<Boolean>> result = ds
				.map(new IdMapper()).setParallelism(4) // parallelize input
				.sortPartition("f1", Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<>(new Tuple3Checker()))
				.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testSortPartitionByTwoFieldExpressions() throws Exception {
		/*
		 * Test sort partition on two field expressions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		List<Tuple1<Boolean>> result = ds
				.map(new IdMapper<Tuple5<Integer, Long, Integer, String, Long>>()).setParallelism(2) // parallelize input
				.sortPartition("f4", Order.ASCENDING)
				.sortPartition("f2", Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<>(new Tuple5Checker()))
				.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testSortPartitionByNestedFieldExpression() throws Exception {
		/*
		 * Test sort partition on nested field expressions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds = CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
		List<Tuple1<Boolean>> result = ds
				.map(new IdMapper<Tuple2<Tuple2<Integer, Integer>, String>>()).setParallelism(3) // parallelize input
				.sortPartition("f0.f1", Order.ASCENDING)
				.sortPartition("f1", Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<>(new NestedTupleChecker()))
				.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testSortPartitionPojoByNestedFieldExpression() throws Exception {
		/*
		 * Test sort partition on field expression
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		DataSet<POJO> ds = CollectionDataSets.getMixedPojoDataSet(env);
		List<Tuple1<Boolean>> result = ds
				.map(new IdMapper<POJO>()).setParallelism(1) // parallelize input
				.sortPartition("nestedTupleWithCustom.f1.myString", Order.ASCENDING)
				.sortPartition("number", Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<>(new PojoChecker()))
				.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testSortPartitionParallelismChange() throws Exception {
		/*
		 * Test sort partition with parallelism change
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		List<Tuple1<Boolean>> result = ds
				.sortPartition(1, Order.DESCENDING).setParallelism(3) // change parallelism
				.mapPartition(new OrderCheckMapper<>(new Tuple3Checker()))
				.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testSortPartitionWithKeySelector1() throws Exception {
		/*
		 * Test sort partition on an extracted key
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		List<Tuple1<Boolean>> result = ds
			.map(new IdMapper<Tuple3<Integer, Long, String>>()).setParallelism(4) // parallelize input
			.sortPartition(new KeySelector<Tuple3<Integer, Long, String>, Long>() {
				@Override
				public Long getKey(Tuple3<Integer, Long, String> value) throws Exception {
					return value.f1;
				}
			}, Order.ASCENDING)
			.mapPartition(new OrderCheckMapper<>(new Tuple3AscendingChecker()))
			.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testSortPartitionWithKeySelector2() throws Exception {
		/*
		 * Test sort partition on an extracted key
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		List<Tuple1<Boolean>> result = ds
			.map(new IdMapper<Tuple3<Integer, Long, String>>()).setParallelism(4) // parallelize input
			.sortPartition(new KeySelector<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>>() {
				@Override
				public Tuple2<Integer, Long> getKey(Tuple3<Integer, Long, String> value) throws Exception {
					return new Tuple2<>(value.f0, value.f1);
				}
			}, Order.DESCENDING)
			.mapPartition(new OrderCheckMapper<>(new Tuple3Checker()))
			.distinct().collect();

		String expected = "(true)\n";

		compareResultAsText(result, expected);
	}

	private interface OrderChecker<T> extends Serializable {
		boolean inOrder(T t1, T t2);
	}

	@SuppressWarnings("serial")
	private static class Tuple3Checker implements OrderChecker<Tuple3<Integer, Long, String>> {
		@Override
		public boolean inOrder(Tuple3<Integer, Long, String> t1, Tuple3<Integer, Long, String> t2) {
			return t1.f1 >= t2.f1;
		}
	}

	@SuppressWarnings("serial")
	private static class Tuple3AscendingChecker implements OrderChecker<Tuple3<Integer, Long, String>> {
		@Override
		public boolean inOrder(Tuple3<Integer, Long, String> t1, Tuple3<Integer, Long, String> t2) {
			return t1.f1 <= t2.f1;
		}
	}

	@SuppressWarnings("serial")
	private static class Tuple5Checker implements OrderChecker<Tuple5<Integer, Long, Integer, String, Long>> {
		@Override
		public boolean inOrder(Tuple5<Integer, Long, Integer, String, Long> t1,
				Tuple5<Integer, Long, Integer, String, Long> t2) {
			return t1.f4 < t2.f4 || t1.f4.equals(t2.f4) && t1.f2 >= t2.f2;
		}
	}

	@SuppressWarnings("serial")
	private static class NestedTupleChecker implements OrderChecker<Tuple2<Tuple2<Integer, Integer>, String>> {
		@Override
		public boolean inOrder(Tuple2<Tuple2<Integer, Integer>, String> t1,
				Tuple2<Tuple2<Integer, Integer>, String> t2) {
			return t1.f0.f1 < t2.f0.f1 ||
					t1.f0.f1.equals(t2.f0.f1) && t1.f1.compareTo(t2.f1) >= 0;
		}
	}

	@SuppressWarnings("serial")
	private static class PojoChecker implements OrderChecker<POJO> {
		@Override
		public boolean inOrder(POJO t1, POJO t2) {
			return t1.nestedTupleWithCustom.f1.myString.compareTo(t2.nestedTupleWithCustom.f1.myString) < 0 ||
					t1.nestedTupleWithCustom.f1.myString.compareTo(t2.nestedTupleWithCustom.f1.myString) == 0 &&
					t1.number >= t2.number;
		}
	}

	@SuppressWarnings("unused, serial")
	private static class OrderCheckMapper<T> implements MapPartitionFunction<T, Tuple1<Boolean>> {

		OrderChecker<T> checker;

		public OrderCheckMapper() {}

		public OrderCheckMapper(OrderChecker<T> checker) {
			this.checker = checker;
		}

		@Override
		public void mapPartition(Iterable<T> values, Collector<Tuple1<Boolean>> out) throws Exception {

			Iterator<T> it = values.iterator();
			if (!it.hasNext()) {
				out.collect(new Tuple1<>(true));
			} else {
				T last = it.next();

				while (it.hasNext()) {
					T next = it.next();
					if (!checker.inOrder(last, next)) {
						out.collect(new Tuple1<>(false));
						return;
					}
					last = next;
				}
				out.collect(new Tuple1<>(true));
			}
		}
	}

	@SuppressWarnings("serial")
	private static class IdMapper<T> implements MapFunction<T, T> {

		@Override
		public T map(T value) throws Exception {
			return value;
		}
	}
}
