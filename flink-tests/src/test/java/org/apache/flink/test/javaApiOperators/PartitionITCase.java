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

import java.util.HashSet;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

@RunWith(Parameterized.class)
public class PartitionITCase extends MultipleProgramsTestBase {

	public PartitionITCase(TestExecutionMode mode){
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
	public void testHashPartitionByKeyField() throws Exception {
		/*
		 * Test hash partition by key field
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Long> uniqLongs = ds
				.partitionByHash(1)
				.mapPartition(new UniqueLongMapper());
		uniqLongs.writeAsText(resultPath);
		env.execute();

		expected = "1\n" +
				"2\n" +
				"3\n" +
				"4\n" +
				"5\n" +
				"6\n";
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
				.mapPartition(new UniqueLongMapper());
		uniqLongs.writeAsText(resultPath);
		env.execute();

		expected = 	"1\n" +
				"2\n" +
				"3\n" +
				"4\n" +
				"5\n" +
				"6\n";
	}

	public static class KeySelector1 implements KeySelector<Tuple3<Integer,Long,String>, Long> {
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
		DataSet<Long> ds = env.generateSequence(1,3000);
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

		uniqLongs.writeAsText(resultPath);

		env.execute();

		StringBuilder result = new StringBuilder();
		int numPerPartition = 2220 / env.getParallelism() / 10;
		for (int i = 0; i < env.getParallelism(); i++) {
			result.append('(').append(i).append(',').append(numPerPartition).append(")\n");
		}

		expected = result.toString();
	}

	public static class Filter1 implements FilterFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Long value) throws Exception {
			if (value <= 780) {
				return false;
			} else {
				return true;
			}
		}
	}

	public static class Reducer1 implements ReduceFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) {
			return new Tuple2<Integer, Integer>(v1.f0, v1.f1+v2.f1);
		}
	}

	public static class Mapper1 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer,
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
				.mapPartition(new UniqueLongMapper());
		uniqLongs.writeAsText(resultPath);

		env.execute();

		expected = 	"1\n" +
				"2\n" +
				"3\n" +
				"4\n" +
				"5\n" +
				"6\n";
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
		uniqLongs.writeAsText(resultPath);

		env.execute();

		expected = 	"10000\n" +
				"20000\n" +
				"30000\n";
	}

	public static class UniqueLongMapper implements MapPartitionFunction<Tuple3<Integer,Long,String>, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<Tuple3<Integer, Long, String>> records, Collector<Long> out) throws Exception {
			HashSet<Long> uniq = new HashSet<Long>();
			for(Tuple3<Integer,Long,String> t : records) {
				uniq.add(t.f1);
			}
			for(Long l : uniq) {
				out.collect(l);
			}
		}
	}
	
	public static class UniqueNestedPojoLongMapper implements MapPartitionFunction<POJO, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<POJO> records, Collector<Long> out) throws Exception {
			HashSet<Long> uniq = new HashSet<Long>();
			for(POJO t : records) {
				uniq.add(t.nestedPojo.longNumber);
			}
			for(Long l : uniq) {
				out.collect(l);
			}
		}
	}
	
	public static class PartitionIndexMapper extends RichMapFunction<Long, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(Long value) throws Exception {
			return new Tuple2<Integer, Integer>(this.getRuntimeContext().getIndexOfThisSubtask(), 1);
		}
	}
}
