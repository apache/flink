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
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Iterator;

@RunWith(Parameterized.class)
public class SortPartitionITCase extends MultipleProgramsTestBase {

	public SortPartitionITCase(TestExecutionMode mode){
		super(mode);
	}

	private String resultPath;
	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testSortPartitionByKeyField() throws Exception {
		/*
		 * Test sort partition on key field
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(4);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		ds
				.map(new IdMapper()).setParallelism(4) // parallelize input
				.sortPartition(1, Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<Tuple3<Integer, Long, String>>(new Tuple3Checker()))
				.distinct()
				.writeAsText(resultPath);

		env.execute();

		expected = "(true)\n";
	}

	@Test
	public void testSortPartitionByTwoKeyFields() throws Exception {
		/*
		 * Test sort partition on two key fields
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(2);

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		ds
				.map(new IdMapper()).setParallelism(2) // parallelize input
				.sortPartition(4, Order.ASCENDING)
				.sortPartition(2, Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<Tuple5<Integer, Long, Integer, String, Long>>(new Tuple5Checker()))
				.distinct()
				.writeAsText(resultPath);

		env.execute();

		expected = "(true)\n";
	}

	@Test
	public void testSortPartitionByFieldExpression() throws Exception {
		/*
		 * Test sort partition on field expression
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(4);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		ds
				.map(new IdMapper()).setParallelism(4) // parallelize input
				.sortPartition("f1", Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<Tuple3<Integer, Long, String>>(new Tuple3Checker()))
				.distinct()
				.writeAsText(resultPath);

		env.execute();

		expected = "(true)\n";
	}

	@Test
	public void testSortPartitionByTwoFieldExpressions() throws Exception {
		/*
		 * Test sort partition on two field expressions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(2);

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		ds
				.map(new IdMapper()).setParallelism(2) // parallelize input
				.sortPartition("f4", Order.ASCENDING)
				.sortPartition("f2", Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<Tuple5<Integer, Long, Integer, String, Long>>(new Tuple5Checker()))
				.distinct()
				.writeAsText(resultPath);

		env.execute();

		expected = "(true)\n";
	}

	@Test
	public void testSortPartitionByNestedFieldExpression() throws Exception {
		/*
		 * Test sort partition on nested field expressions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(3);

		DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds = CollectionDataSets.getGroupSortedNestedTupleDataSet(env);
		ds
				.map(new IdMapper()).setParallelism(3) // parallelize input
				.sortPartition("f0.f1", Order.ASCENDING)
				.sortPartition("f1", Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<Tuple2<Tuple2<Integer, Integer>, String>>(new NestedTupleChecker()))
				.distinct()
				.writeAsText(resultPath);

		env.execute();

		expected = "(true)\n";
	}

	@Test
	public void testSortPartitionPojoByNestedFieldExpression() throws Exception {
		/*
		 * Test sort partition on field expression
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(3);

		DataSet<POJO> ds = CollectionDataSets.getMixedPojoDataSet(env);
		ds
				.map(new IdMapper()).setParallelism(1) // parallelize input
				.sortPartition("nestedTupleWithCustom.f1.myString", Order.ASCENDING)
				.sortPartition("number", Order.DESCENDING)
				.mapPartition(new OrderCheckMapper<POJO>(new PojoChecker()))
				.distinct()
				.writeAsText(resultPath);

		env.execute();

		expected = "(true)\n";
	}

	@Test
	public void testSortPartitionDOPChange() throws Exception {
		/*
		 * Test sort partition with DOP change
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(3);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		ds
				.sortPartition(1, Order.DESCENDING).setParallelism(3) // change DOP
				.mapPartition(new OrderCheckMapper<Tuple3<Integer, Long, String>>(new Tuple3Checker()))
				.distinct()
				.writeAsText(resultPath);

		env.execute();

		expected = "(true)\n";
	}

	public static interface OrderChecker<T> extends Serializable {

		public boolean inOrder(T t1, T t2);
	}

	public static class Tuple3Checker implements OrderChecker<Tuple3<Integer, Long, String>> {
		@Override
		public boolean inOrder(Tuple3<Integer, Long, String> t1, Tuple3<Integer, Long, String> t2) {
			return t1.f1 >= t2.f1;
		}
	}

	public static class Tuple5Checker implements OrderChecker<Tuple5<Integer, Long, Integer, String, Long>> {
		@Override
		public boolean inOrder(Tuple5<Integer, Long, Integer, String, Long> t1,
								Tuple5<Integer, Long, Integer, String, Long> t2) {
			return t1.f4 < t2.f4 || t1.f4 == t2.f4 && t1.f2 >= t2.f2;
		}
	}

	public static class NestedTupleChecker implements OrderChecker<Tuple2<Tuple2<Integer, Integer>, String>> {
		@Override
		public boolean inOrder(Tuple2<Tuple2<Integer, Integer>, String> t1,
								Tuple2<Tuple2<Integer, Integer>, String> t2) {
			return t1.f0.f1 < t2.f0.f1 ||
					t1.f0.f1 == t2.f0.f1 && t1.f1.compareTo(t2.f1) >= 0;
 		}
	}

	public static class PojoChecker implements OrderChecker<POJO> {
		@Override
		public boolean inOrder(POJO t1,
							   POJO t2) {
			return t1.nestedTupleWithCustom.f1.myString.compareTo(t2.nestedTupleWithCustom.f1.myString) < 0 ||
					t1.nestedTupleWithCustom.f1.myString.compareTo(t2.nestedTupleWithCustom.f1.myString) == 0 &&
							t1.number >= t2.number;
		}
	}

	public static class OrderCheckMapper<T> implements MapPartitionFunction<T, Tuple1<Boolean>> {

		OrderChecker<T> checker;

		public OrderCheckMapper() {}

		public OrderCheckMapper(OrderChecker<T> checker) {
			this.checker = checker;
		}

		@Override
		public void mapPartition(Iterable<T> values, Collector<Tuple1<Boolean>> out) throws Exception {

			Iterator<T> it = values.iterator();
			if(!it.hasNext()) {
				out.collect(new Tuple1<Boolean>(true));
				return;
			} else {
				T last = it.next();

				while (it.hasNext()) {
					T next = it.next();
					if (!checker.inOrder(last, next)) {
						out.collect(new Tuple1<Boolean>(false));
						return;
					}
					last = next;
				}
				out.collect(new Tuple1<Boolean>(true));
			}
		}
	}


	public static class IdMapper<T> implements MapFunction<T, T> {

		@Override
		public T map(T value) throws Exception {
			return value;
		}
	}
}
