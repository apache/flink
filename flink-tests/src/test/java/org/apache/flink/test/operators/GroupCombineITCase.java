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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * The GroupCombine operator is not easy to test because it is essentially just a combiner. The result can be
 * the result of a normal groupReduce at any stage its execution. The basic idea is to preserve the grouping key
 * in the partial result, so that we can do a reduceGroup afterwards to finalize the results for verification.
 * In addition, we can use hashPartition to partition the data and check if no shuffling (just combining) has
 * been performed.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class GroupCombineITCase extends MultipleProgramsTestBase {

	public GroupCombineITCase(TestExecutionMode mode) {
		super(mode);
	}

	private static String identityResult = "1,1,Hi\n" +
			"2,2,Hello\n" +
			"3,2,Hello world\n" +
			"4,3,Hello world, how are you?\n" +
			"5,3,I am fine.\n" +
			"6,3,Luke Skywalker\n" +
			"7,4,Comment#1\n" +
			"8,4,Comment#2\n" +
			"9,4,Comment#3\n" +
			"10,4,Comment#4\n" +
			"11,5,Comment#5\n" +
			"12,5,Comment#6\n" +
			"13,5,Comment#7\n" +
			"14,5,Comment#8\n" +
			"15,5,Comment#9\n" +
			"16,6,Comment#10\n" +
			"17,6,Comment#11\n" +
			"18,6,Comment#12\n" +
			"19,6,Comment#13\n" +
			"20,6,Comment#14\n" +
			"21,6,Comment#15\n";

	@Test
	public void testAllGroupCombineIdentity() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds
				// combine
				.combineGroup(new IdentityFunction())
				// fully reduce
				.reduceGroup(new IdentityFunction());

		List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

		compareResultAsTuples(result, identityResult);
	}

	@Test
	public void testIdentity() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds
				// combine
				.combineGroup(new IdentityFunction())
				// fully reduce
				.reduceGroup(new IdentityFunction());

		List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

		compareResultAsTuples(result, identityResult);
	}

	@Test
	public void testIdentityWithGroupBy() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds
				.groupBy(1)
				// combine
				.combineGroup(new IdentityFunction())
				// fully reduce
				.reduceGroup(new IdentityFunction());

		List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

		compareResultAsTuples(result, identityResult);
	}

	@Test
	public void testIdentityWithGroupByAndSort() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds
				.groupBy(1)
				.sortGroup(1, Order.DESCENDING)
				// reduce partially
				.combineGroup(new IdentityFunction())
				.groupBy(1)
				.sortGroup(1, Order.DESCENDING)
				// fully reduce
				.reduceGroup(new IdentityFunction());

		List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

		compareResultAsTuples(result, identityResult);
	}

	@Test
	public void testPartialReduceWithIdenticalInputOutputType() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// data
		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple2<Long, Tuple3<Integer, Long, String>>> dsWrapped = ds
				// wrap values as Kv pairs with the grouping key as key
				.map(new Tuple3KvWrapper());

		List<Tuple3<Integer, Long, String>> result = dsWrapped
				.groupBy(0)
				// reduce partially
				.combineGroup(new Tuple3toTuple3GroupReduce())
				.groupBy(0)
				// reduce fully to check result
				.reduceGroup(new Tuple3toTuple3GroupReduce())
				//unwrap
				.map(new MapFunction<Tuple2<Long, Tuple3<Integer, Long, String>>, Tuple3<Integer, Long, String>>() {
					@Override
					public Tuple3<Integer, Long, String> map(Tuple2<Long, Tuple3<Integer, Long, String>> value) throws Exception {
						return value.f1;
					}
				}).collect();

		String expected = "1,1,combined\n" +
				"5,4,combined\n" +
				"15,9,combined\n" +
				"34,16,combined\n" +
				"65,25,combined\n" +
				"111,36,combined\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testPartialReduceWithDifferentInputOutputType() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// data
		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple2<Long, Tuple3<Integer, Long, String>>> dsWrapped = ds
				// wrap values as Kv pairs with the grouping key as key
				.map(new Tuple3KvWrapper());

		List<Tuple2<Integer, Long>> result = dsWrapped
				.groupBy(0)
				// reduce partially
				.combineGroup(new Tuple3toTuple2GroupReduce())
				.groupBy(0)
				// reduce fully to check result
				.reduceGroup(new Tuple2toTuple2GroupReduce())
				//unwrap
				.map(new MapFunction<Tuple2<Long, Tuple2<Integer, Long>>, Tuple2<Integer, Long>>() {
					@Override
					public Tuple2<Integer, Long> map(Tuple2<Long, Tuple2<Integer, Long>> value) throws Exception {
						return value.f1;
					}
				}).collect();

		String expected = "1,3\n" +
				"5,20\n" +
				"15,58\n" +
				"34,52\n" +
				"65,70\n" +
				"111,96\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	// check if no shuffle is being executed
	public void testCheckPartitionShuffleGroupBy() throws Exception {

		org.junit.Assume.assumeTrue(mode != TestExecutionMode.COLLECTION);

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// data
		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		// partition and group data
		UnsortedGrouping<Tuple3<Integer, Long, String>> partitionedDS = ds.partitionByHash(0).groupBy(1);

		List<Tuple2<Long, Integer>> result = partitionedDS
				.combineGroup(
						new GroupCombineFunction<Tuple3<Integer, Long, String>, Tuple2<Long, Integer>>() {
			@Override
			public void combine(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple2<Long, Integer>> out) throws Exception {
				int count = 0;
				long key = 0;
				for (Tuple3<Integer, Long, String> value : values) {
					key = value.f1;
					count++;
				}
				out.collect(new Tuple2<>(key, count));
			}
		}).collect();

		String[] localExpected = new String[] { "(6,6)", "(5,5)" + "(4,4)", "(3,3)", "(2,2)", "(1,1)" };

		String[] resultAsStringArray = new String[result.size()];
		for (int i = 0; i < resultAsStringArray.length; ++i) {
			resultAsStringArray[i] = result.get(i).toString();
		}
		Arrays.sort(resultAsStringArray);

		Assert.assertEquals("The two arrays were identical.", false, Arrays.equals(localExpected, resultAsStringArray));
	}

	@Test
	// check if parallelism of 1 results in the same data like a shuffle
	public void testCheckPartitionShuffleDOP1() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		// data
		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		// partition and group data
		UnsortedGrouping<Tuple3<Integer, Long, String>> partitionedDS = ds.partitionByHash(0).groupBy(1);

		List<Tuple2<Long, Integer>> result = partitionedDS
				.combineGroup(
				new GroupCombineFunction<Tuple3<Integer, Long, String>, Tuple2<Long, Integer>>() {
					@Override
					public void combine(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple2<Long, Integer>> out) throws Exception {
						int count = 0;
						long key = 0;
						for (Tuple3<Integer, Long, String> value : values) {
							key = value.f1;
							count++;
						}
						out.collect(new Tuple2<>(key, count));
					}
				}).collect();

		String expected = "6,6\n" +
				"5,5\n" +
				"4,4\n" +
				"3,3\n" +
				"2,2\n" +
				"1,1\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	// check if all API methods are callable
	public void testAPI() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple1<String>> ds = CollectionDataSets.getStringDataSet(env).map(new MapFunction<String, Tuple1<String>>() {
			@Override
			public Tuple1<String> map(String value) throws Exception {
				return new Tuple1<>(value);
			}
		});

		// all methods on DataSet
		ds.combineGroup(new GroupCombineFunctionExample())
		.output(new DiscardingOutputFormat<Tuple1<String>>());

		// all methods on UnsortedGrouping
		ds.groupBy(0).combineGroup(new GroupCombineFunctionExample())
		.output(new DiscardingOutputFormat<Tuple1<String>>());

		// all methods on SortedGrouping
		ds.groupBy(0).sortGroup(0, Order.ASCENDING).combineGroup(new GroupCombineFunctionExample())
		.output(new DiscardingOutputFormat<Tuple1<String>>());

		env.execute();
	}

	private static class GroupCombineFunctionExample implements GroupCombineFunction<Tuple1<String>, Tuple1<String>> {

		@Override
		public void combine(Iterable<Tuple1<String>> values, Collector<Tuple1<String>> out) throws Exception {
			for (Tuple1<String> value : values) {
				out.collect(value);
			}
		}
	}

	/**
	 * For Scala GroupCombineITCase.
	 */
	public static class ScalaGroupCombineFunctionExample implements GroupCombineFunction<scala.Tuple1<String>, scala.Tuple1<String>> {

		@Override
		public void combine(Iterable<scala.Tuple1<String>> values, Collector<scala.Tuple1<String>> out) throws Exception {
			for (scala.Tuple1<String> value : values) {
				out.collect(value);
			}
		}
	}

	private static class IdentityFunction implements GroupCombineFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>>,
	GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {

		@Override
		public void combine(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			for (Tuple3<Integer, Long, String> value : values) {
				out.collect(new Tuple3<>(value.f0, value.f1, value.f2));
			}
		}

		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			for (Tuple3<Integer, Long, String> value : values) {
				out.collect(new Tuple3<>(value.f0, value.f1, value.f2));
			}
		}
	}

	private static class Tuple3toTuple3GroupReduce implements KvGroupReduce<Long, Tuple3<Integer, Long, String>,
			Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {

		@Override
		public void combine(Iterable<Tuple2<Long, Tuple3<Integer, Long, String>>> values, Collector<Tuple2<Long,
				Tuple3<Integer, Long, String>>> out) throws Exception {
			int i = 0;
			long l = 0;
			long key = 0;

			// collapse groups
			for (Tuple2<Long, Tuple3<Integer, Long, String>> value : values) {
				key = value.f0;
				Tuple3<Integer, Long, String> extracted = value.f1;
				i += extracted.f0;
				l += extracted.f1;
			}

			Tuple3<Integer, Long, String> result = new Tuple3<>(i, l, "combined");
			out.collect(new Tuple2<>(key, result));
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Tuple3<Integer, Long, String>>> values,
				Collector<Tuple2<Long, Tuple3<Integer, Long, String>>> out) throws Exception {
			combine(values, out);
		}
	}

	private static class Tuple3toTuple2GroupReduce implements KvGroupReduce<Long, Tuple3<Integer, Long, String>,
			Tuple2<Integer, Long>, Tuple2<Integer, Long>> {

		@Override
		public void combine(Iterable<Tuple2<Long, Tuple3<Integer, Long, String>>> values, Collector<Tuple2<Long,
				Tuple2<Integer, Long>>> out) throws Exception {
			int i = 0;
			long l = 0;
			long key = 0;

			// collapse groups
			for (Tuple2<Long, Tuple3<Integer, Long, String>> value : values) {
				key = value.f0;
				Tuple3<Integer, Long, String> extracted = value.f1;
				i += extracted.f0;
				l += extracted.f1 + extracted.f2.length();
			}

			Tuple2<Integer, Long> result = new Tuple2<>(i, l);
			out.collect(new Tuple2<>(key, result));
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Tuple2<Integer, Long>>> values, Collector<Tuple2<Long,
				Tuple2<Integer, Long>>> out) throws Exception {
			new Tuple2toTuple2GroupReduce().reduce(values, out);
		}
	}

	private static class Tuple2toTuple2GroupReduce implements KvGroupReduce<Long, Tuple2<Integer, Long>,
			Tuple2<Integer, Long>, Tuple2<Integer, Long>> {

		@Override
		public void combine(Iterable<Tuple2<Long, Tuple2<Integer, Long>>> values, Collector<Tuple2<Long, Tuple2<Integer,
				Long>>> out) throws Exception {
			int i = 0;
			long l = 0;
			long key = 0;

			// collapse groups
			for (Tuple2<Long, Tuple2<Integer, Long>> value : values) {
				key = value.f0;
				Tuple2<Integer, Long> extracted = value.f1;
				i += extracted.f0;
				l += extracted.f1;
			}

			Tuple2<Integer, Long> result = new Tuple2<>(i, l);

			out.collect(new Tuple2<>(key, result));
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Tuple2<Integer, Long>>> values, Collector<Tuple2<Long,
				Tuple2<Integer, Long>>> out) throws Exception {
			combine(values, out);
		}
	}

	private class Tuple3KvWrapper implements MapFunction<Tuple3<Integer, Long, String>, Tuple2<Long,
			Tuple3<Integer, Long, String>>> {
		@Override
		public Tuple2<Long, Tuple3<Integer, Long, String>> map(Tuple3<Integer, Long, String> value) throws Exception {
			return new Tuple2<>(value.f1, value);
		}
	}

	private interface CombineAndReduceGroup <IN, INT, OUT> extends GroupCombineFunction<IN, INT>,
			GroupReduceFunction<INT, OUT> {
	}

	private interface KvGroupReduce<K, V, INT, OUT> extends CombineAndReduceGroup<Tuple2<K, V>, Tuple2<K, INT>,
			Tuple2<K, OUT>> {
	}

}
