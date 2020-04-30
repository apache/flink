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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link MapPartitionFunction}.
 */
@RunWith(Parameterized.class)
@SuppressWarnings("serial")
public class PartitionITCase extends MultipleProgramsTestBase {

	public PartitionITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testHashPartitionByKeyField() throws Exception {
		/*
		 * Test hash partition by key field
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Long> uniqLongs = ds
				.partitionByHash(1)
				.mapPartition(new UniqueTupleLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "1\n" +
				"2\n" +
				"3\n" +
				"4\n" +
				"5\n" +
				"6\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testRangePartitionByKeyField() throws Exception {
		/*
		 * Test range partition by key field
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Long> uniqLongs = ds
			.partitionByRange(1)
			.mapPartition(new UniqueTupleLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "1\n" +
			"2\n" +
			"3\n" +
			"4\n" +
			"5\n" +
			"6\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testHashPartitionByKeyField2() throws Exception {
		/*
		 * Test hash partition by key field
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		AggregateOperator<Tuple3<Integer, Long, String>> sum = ds
			.map(new PrefixMapper())
			.partitionByHash(1, 2)
			.groupBy(1, 2)
			.sum(0);

		List<Tuple3<Integer, Long, String>> result = sum.collect();

		String expected = "(1,1,Hi)\n" +
			"(5,2,Hello)\n" +
			"(4,3,Hello)\n" +
			"(5,3,I am )\n" +
			"(6,3,Luke )\n" +
			"(34,4,Comme)\n" +
			"(65,5,Comme)\n" +
			"(111,6,Comme)";

		compareResultAsText(result, expected);
	}

	@Test
	public void testRangePartitionByKeyField2() throws Exception {
		/*
		 * Test range partition by key field
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		AggregateOperator<Tuple3<Integer, Long, String>> sum = ds
			.map(new PrefixMapper())
			.partitionByRange(1, 2)
			.groupBy(1, 2)
			.sum(0);

		List<Tuple3<Integer, Long, String>> result = sum.collect();

		String expected = "(1,1,Hi)\n" +
		"(5,2,Hello)\n" +
		"(4,3,Hello)\n" +
		"(5,3,I am )\n" +
		"(6,3,Luke )\n" +
		"(34,4,Comme)\n" +
		"(65,5,Comme)\n" +
		"(111,6,Comme)";

		compareResultAsText(result, expected);
	}

	@Test
	public void testHashPartitionOfAtomicType() throws Exception {
		/*
		 * Test hash partition of atomic type
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> uniqLongs = env.generateSequence(1, 6)
			.union(env.generateSequence(1, 6))
			.rebalance()
			.partitionByHash("*")
			.mapPartition(new UniqueLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "1\n" +
			"2\n" +
			"3\n" +
			"4\n" +
			"5\n" +
			"6\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testRangePartitionOfAtomicType() throws Exception {
		/*
		 * Test range partition of atomic type
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> uniqLongs = env.generateSequence(1, 6)
			.union(env.generateSequence(1, 6))
			.rebalance()
			.partitionByRange("*")
			.mapPartition(new UniqueLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "1\n" +
			"2\n" +
			"3\n" +
			"4\n" +
			"5\n" +
			"6\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testHashPartitionByKeySelector() throws Exception {
		/*
		 * Test hash partition by key selector
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Long> uniqLongs = ds
				.partitionByHash(new KeySelector1())
				.mapPartition(new UniqueTupleLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "1\n" +
				"2\n" +
				"3\n" +
				"4\n" +
				"5\n" +
				"6\n";

		compareResultAsText(result, expected);
	}

	private static class PrefixMapper implements MapFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {
		@Override
		public Tuple3<Integer, Long, String> map(Tuple3<Integer, Long, String> value) throws Exception {
			if (value.f2.length() > 5) {
				value.f2 = value.f2.substring(0, 5);
			}
			return value;
		}
	}

	@Test
	public void testRangePartitionByKeySelector() throws Exception {
		/*
		 * Test range partition by key selector
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Long> uniqLongs = ds
			.partitionByRange(new KeySelector1())
			.mapPartition(new UniqueTupleLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "1\n" +
			"2\n" +
			"3\n" +
			"4\n" +
			"5\n" +
			"6\n";

		compareResultAsText(result, expected);
	}

	private static class KeySelector1 implements KeySelector<Tuple3<Integer, Long, String>, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(Tuple3<Integer, Long, String> value) throws Exception {
			return value.f1;
		}

	}

	@Test
	public void testForcedRebalancing() throws Exception {
		/*
		 * Test forced rebalancing
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// generate some number in parallel
		DataSet<Long> ds = env.generateSequence(1, 3000);
		DataSet<Tuple2<Integer, Integer>> uniqLongs = ds
				// introduce some partition skew by filtering
				.filter(new Filter1())
				// rebalance
				.rebalance()
				// count values in each partition
				.map(new PartitionIndexMapper())
				.groupBy(0)
				.reduce(new Reducer1())
				// round counts to mitigate runtime scheduling effects (lazy split assignment)
				.map(new Mapper1());

		List<Tuple2<Integer, Integer>> result = uniqLongs.collect();

		StringBuilder expected = new StringBuilder();
		int numPerPartition = 2220 / env.getParallelism() / 10;
		for (int i = 0; i < env.getParallelism(); i++) {
			expected.append('(').append(i).append(',')
			.append(numPerPartition).append(")\n");
		}

		compareResultAsText(result, expected.toString());
	}

	private static class Filter1 implements FilterFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Long value) throws Exception {
			return value > 780;
		}
	}

	private static class Reducer1 implements ReduceFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) {
			return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
		}
	}

	private static class Mapper1 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer,
	Integer>>{
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
			value.f1 = (value.f1 / 10);
			return value;
		}

	}

	@Test
	public void testHashPartitionByKeyFieldAndDifferentParallelism() throws Exception {
		/*
		 * Test hash partition by key field and different parallelism
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Long> uniqLongs = ds
				.partitionByHash(1).setParallelism(4)
				.mapPartition(new UniqueTupleLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "1\n" +
				"2\n" +
				"3\n" +
				"4\n" +
				"5\n" +
				"6\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testRangePartitionByKeyFieldAndDifferentParallelism() throws Exception {
		/*
		 * Test range partition by key field and different parallelism
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Long> uniqLongs = ds
			.partitionByRange(1).setParallelism(4)
			.mapPartition(new UniqueTupleLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "1\n" +
			"2\n" +
			"3\n" +
			"4\n" +
			"5\n" +
			"6\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testHashPartitionWithKeyExpression() throws Exception {
		/*
		 * Test hash partition with key expression
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		DataSet<POJO> ds = CollectionDataSets.getDuplicatePojoDataSet(env);
		DataSet<Long> uniqLongs = ds
				.partitionByHash("nestedPojo.longNumber").setParallelism(4)
				.mapPartition(new UniqueNestedPojoLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "10000\n" +
				"20000\n" +
				"30000\n";

		compareResultAsText(result, expected);
	}

	@Test
	public void testRangePartitionWithKeyExpression() throws Exception {
		/*
		 * Test range partition with key expression
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		DataSet<POJO> ds = CollectionDataSets.getDuplicatePojoDataSet(env);
		DataSet<Long> uniqLongs = ds
			.partitionByRange("nestedPojo.longNumber").setParallelism(4)
			.mapPartition(new UniqueNestedPojoLongMapper());
		List<Long> result = uniqLongs.collect();

		String expected = "10000\n" +
			"20000\n" +
			"30000\n";

		compareResultAsText(result, expected);
	}

	private static class UniqueTupleLongMapper implements MapPartitionFunction<Tuple3<Integer, Long, String>, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<Tuple3<Integer, Long, String>> records, Collector<Long> out) throws Exception {
			HashSet<Long> uniq = new HashSet<>();
			for (Tuple3<Integer, Long, String> t : records) {
				uniq.add(t.f1);
			}
			for (Long l : uniq) {
				out.collect(l);
			}
		}
	}

	private static class UniqueLongMapper implements MapPartitionFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<Long> longs, Collector<Long> out) throws Exception {
			HashSet<Long> uniq = new HashSet<>();
			for (Long l : longs) {
				uniq.add(l);
			}
			for (Long l : uniq) {
				out.collect(l);
			}
		}
	}

	private static class UniqueNestedPojoLongMapper implements MapPartitionFunction<POJO, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<POJO> records, Collector<Long> out) throws Exception {
			HashSet<Long> uniq = new HashSet<>();
			for (POJO t : records) {
				uniq.add(t.nestedPojo.longNumber);
			}
			for (Long l : uniq) {
				out.collect(l);
			}
		}
	}

	private static class PartitionIndexMapper extends RichMapFunction<Long, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(Long value) throws Exception {
			return new Tuple2<>(this.getRuntimeContext().getIndexOfThisSubtask(), 1);
		}
	}

	@Test
	public void testRangePartitionerOnSequenceData() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Long> dataSource = env.generateSequence(0, 10000);
		KeySelector<Long, Long> keyExtractor = new ObjectSelfKeySelector();

		MapPartitionFunction<Long, Tuple2<Long, Long>> minMaxSelector = new MinMaxSelector<>(new LongComparator(true));

		Comparator<Tuple2<Long, Long>> tuple2Comparator = new Tuple2Comparator(new LongComparator(true));

		List<Tuple2<Long, Long>> collected = dataSource.partitionByRange(keyExtractor).mapPartition(minMaxSelector).collect();
		Collections.sort(collected, tuple2Comparator);

		long previousMax = -1;
		for (Tuple2<Long, Long> tuple2 : collected) {
			if (previousMax == -1) {
				previousMax = tuple2.f1;
			} else {
				long currentMin = tuple2.f0;
				assertTrue(tuple2.f0 < tuple2.f1);
				assertEquals(previousMax + 1, currentMin);
				previousMax = tuple2.f1;
			}
		}
	}

	@Test(expected = InvalidProgramException.class)
	public void testRangePartitionInIteration() throws Exception {

		// does not apply for collection execution
		if (super.mode == TestExecutionMode.COLLECTION) {
			throw new InvalidProgramException("Does not apply for collection execution");
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Long> source = env.generateSequence(0, 10000);

		DataSet<Tuple2<Long, String>> tuples = source.map(new MapFunction<Long, Tuple2<Long, String>>() {
			@Override
			public Tuple2<Long, String> map(Long v) throws Exception {
				return new Tuple2<>(v, Long.toString(v));
			}
		});

		DeltaIteration<Tuple2<Long, String>, Tuple2<Long, String>> it = tuples.iterateDelta(tuples, 10, 0);
		DataSet<Tuple2<Long, String>> body = it.getWorkset()
			.partitionByRange(1) // Verify that range partition is not allowed in iteration
			.join(it.getSolutionSet())
			.where(0).equalTo(0).projectFirst(0).projectSecond(1);
		DataSet<Tuple2<Long, String>> result = it.closeWith(body, body);

		result.collect(); // should fail
	}

	@Test
	public void testRangePartitionerOnSequenceDataWithOrders() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple2<Long, Long>> dataSet = env.generateSequence(0, 10000)
				.map(new MapFunction<Long, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Long value) throws Exception {
				return new Tuple2<>(value / 5000, value % 5000);
			}
		});

		final Tuple2Comparator<Long> tuple2Comparator = new Tuple2Comparator<>(new LongComparator(true),
				new LongComparator(false));

		MinMaxSelector<Tuple2<Long, Long>> minMaxSelector = new MinMaxSelector<>(tuple2Comparator);

		final List<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>> collected = dataSet.partitionByRange(0, 1)
				.withOrders(Order.ASCENDING, Order.DESCENDING)
				.mapPartition(minMaxSelector)
				.collect();

		Collections.sort(collected, new Tuple2Comparator<>(tuple2Comparator));

		Tuple2<Long, Long> previousMax = null;
		for (Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> tuple2 : collected) {
			assertTrue("Min element in each partition should be smaller than max.",
					tuple2Comparator.compare(tuple2.f0, tuple2.f1) <= 0);
			if (previousMax == null) {
				previousMax = tuple2.f1;
			} else {
				assertTrue("Partitions overlap. Previous max should be smaller than current min.",
						tuple2Comparator.compare(previousMax, tuple2.f0) < 0);
				if (previousMax.f0.equals(tuple2.f0.f0)) {
					//check that ordering on the second key is correct
					assertEquals("Ordering on the second field should be continous.",
							previousMax.f1 - 1, tuple2.f0.f1.longValue());
				}
				previousMax = tuple2.f1;
			}
		}
	}

	@Test
	public void testRangePartitionerOnSequenceNestedDataWithOrders() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final DataSet<Tuple2<Tuple2<Long, Long>, Long>> dataSet = env.generateSequence(0, 10000)
				.map(new MapFunction<Long, Tuple2<Tuple2<Long, Long>, Long>>() {
					@Override
					public Tuple2<Tuple2<Long, Long>, Long> map(Long value) throws Exception {
						return new Tuple2<>(new Tuple2<>(value / 5000, value % 5000), value);
					}
				});

		final Tuple2Comparator<Long> tuple2Comparator = new Tuple2Comparator<>(new LongComparator(true),
				new LongComparator(true));
		MinMaxSelector<Tuple2<Long, Long>> minMaxSelector = new MinMaxSelector<>(tuple2Comparator);

		final List<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>> collected = dataSet.partitionByRange(0)
				.withOrders(Order.ASCENDING)
				.mapPartition(new MapPartitionFunction<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Long, Long>>() {
					@Override
					public void mapPartition(Iterable<Tuple2<Tuple2<Long, Long>, Long>> values,
							Collector<Tuple2<Long, Long>> out) throws Exception {
						for (Tuple2<Tuple2<Long, Long>, Long> value : values) {
							out.collect(value.f0);
						}
					}
				})
				.mapPartition(minMaxSelector)
				.collect();

		Collections.sort(collected, new Tuple2Comparator<>(tuple2Comparator));

		Tuple2<Long, Long> previousMax = null;
		for (Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> tuple2 : collected) {
			assertTrue("Min element in each partition should be smaller than max.",
					tuple2Comparator.compare(tuple2.f0, tuple2.f1) <= 0);
			if (previousMax == null) {
				previousMax = tuple2.f1;
			} else {
				assertTrue("Partitions overlap. Previous max should be smaller than current min.",
						tuple2Comparator.compare(previousMax, tuple2.f0) < 0);
				if (previousMax.f0.equals(tuple2.f0.f0)) {
					assertEquals("Ordering on the second field should be continous.",
							previousMax.f1 + 1, tuple2.f0.f1.longValue());
				}
				previousMax = tuple2.f1;
			}
		}
	}

	@Test
	public void testRangePartitionerWithKeySelectorOnSequenceNestedDataWithOrders() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final DataSet<Tuple2<ComparablePojo, Long>> dataSet = env.generateSequence(0, 10000)
				.map(new MapFunction<Long, Tuple2<ComparablePojo, Long>>() {
					@Override
					public Tuple2<ComparablePojo, Long> map(Long value) throws Exception {
						return new Tuple2<>(new ComparablePojo(value / 5000, value % 5000), value);
					}
				});

		final List<Tuple2<ComparablePojo, ComparablePojo>> collected = dataSet
				.partitionByRange(new KeySelector<Tuple2<ComparablePojo, Long>, ComparablePojo>() {
					@Override
					public ComparablePojo getKey(Tuple2<ComparablePojo, Long> value) throws Exception {
						return value.f0;
					}
				})
				.withOrders(Order.ASCENDING)
				.mapPartition(new MinMaxSelector<>(new ComparablePojoComparator()))
				.mapPartition(new ExtractComparablePojo())
				.collect();

		final Comparator<Tuple2<ComparablePojo, ComparablePojo>> pojoComparator =
				new Comparator<Tuple2<ComparablePojo, ComparablePojo>>() {
			@Override
			public int compare(Tuple2<ComparablePojo, ComparablePojo> o1, Tuple2<ComparablePojo, ComparablePojo> o2) {
				return o1.f0.compareTo(o2.f1);
			}
		};
		Collections.sort(collected, pojoComparator);

		ComparablePojo previousMax = null;
		for (Tuple2<ComparablePojo, ComparablePojo> element : collected) {
			assertTrue("Min element in each partition should be smaller than max.",
					element.f0.compareTo(element.f1) <= 0);
			if (previousMax == null) {
				previousMax = element.f1;
			} else {
				assertTrue("Partitions overlap. Previous max should be smaller than current min.",
						previousMax.compareTo(element.f0) < 0);
				if (previousMax.first.equals(element.f0.first)) {
					assertEquals("Ordering on the second field should be continous.",
							previousMax.second - 1, element.f0.second.longValue());
				}
				previousMax = element.f1;
			}
		}
	}

	private static class ExtractComparablePojo implements MapPartitionFunction<
			Tuple2<Tuple2<ComparablePojo, Long>, Tuple2<ComparablePojo, Long>>,
			Tuple2<ComparablePojo, ComparablePojo>> {

		@Override
		public void mapPartition(Iterable<Tuple2<Tuple2<ComparablePojo, Long>, Tuple2<ComparablePojo, Long>>> values,
				Collector<Tuple2<ComparablePojo, ComparablePojo>> out) throws Exception {
			for (Tuple2<Tuple2<ComparablePojo, Long>, Tuple2<ComparablePojo, Long>> value : values) {
				out.collect(new Tuple2<>(value.f0.f0, value.f1.f0));
			}
		}
	}

	private static class ComparablePojoComparator implements Comparator<Tuple2<ComparablePojo, Long>>, Serializable {

		@Override
		public int compare(Tuple2<ComparablePojo, Long> o1, Tuple2<ComparablePojo, Long> o2) {
			return o1.f0.compareTo(o2.f0);
		}
	}

	private static class ComparablePojo implements Comparable<ComparablePojo> {
		private Long first;
		private Long second;

		public Long getFirst() {
			return first;
		}

		public void setFirst(Long first) {
			this.first = first;
		}

		public Long getSecond() {
			return second;
		}

		public void setSecond(Long second) {
			this.second = second;
		}

		public ComparablePojo(Long first, Long second) {
			this.first = first;
			this.second = second;
		}

		public ComparablePojo() {
		}

		@Override
		public int compareTo(ComparablePojo o) {
			final int firstResult = Long.compare(this.first, o.first);
			if (firstResult == 0) {
				return (-1) * Long.compare(this.second, o.second);
			}

			return firstResult;
		}
	}

	private static class ObjectSelfKeySelector implements KeySelector<Long, Long> {
		@Override
		public Long getKey(Long value) throws Exception {
			return value;
		}
	}

	private static class MinMaxSelector<T> implements MapPartitionFunction<T, Tuple2<T, T>> {

		private final Comparator<T> comparator;

		public MinMaxSelector(Comparator<T> comparator) {
			this.comparator = comparator;
		}

		@Override
		public void mapPartition(Iterable<T> values, Collector<Tuple2<T, T>> out) throws Exception {
			Iterator<T> itr = values.iterator();
			T min = itr.next();
			T max = min;
			T value;
			while (itr.hasNext()) {
				value = itr.next();
				if (comparator.compare(value, min) < 0) {
					min = value;
				}
				if (comparator.compare(value, max) > 0) {
					max = value;
				}

			}

			Tuple2<T, T> result = new Tuple2<>(min, max);
			out.collect(result);
		}
	}

	private static class Tuple2Comparator<T> implements Comparator<Tuple2<T, T>>, Serializable {

		private final Comparator<T> firstComparator;
		private final Comparator<T> secondComparator;

		public Tuple2Comparator(Comparator<T> comparator) {
			this(comparator, comparator);
		}

		public Tuple2Comparator(Comparator<T> firstComparator,
								Comparator<T> secondComparator) {
			this.firstComparator = firstComparator;
			this.secondComparator = secondComparator;
		}

		@Override
		public int compare(Tuple2<T, T> first, Tuple2<T, T> second) {
			long result = firstComparator.compare(first.f0, second.f0);
			if (result > 0) {
				return 1;
			} else if (result < 0) {
				return -1;
			}

			result = secondComparator.compare(first.f1, second.f1);
			if (result > 0) {
				return 1;
			} else if (result < 0) {
				return -1;
			}

			return 0;
		}
	}

	private static class LongComparator implements Comparator<Long>, Serializable {

		private final boolean ascending;

		public LongComparator(boolean ascending) {
			this.ascending = ascending;
		}

		@Override
		public int compare(Long o1, Long o2) {
			if (ascending) {
				return Long.compare(o1, o2);
			} else {
				return (-1) * Long.compare(o1, o2);
			}
		}
	}

}
