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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
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

@SuppressWarnings("serial")
@RunWith(Parameterized.class)
/**
 * The GroupReducePartial operator is not easy to test because it is essentially just a combiner. The result can be
 * the result of a normal groupReduce at any stage its execution. The basic idea is to preserve the grouping key
 * in the partial result, so that we can do a reduceGroup afterwards to finalize the results for verification.
 */
public class GroupReducePartialITCase extends MultipleProgramsTestBase {

	public GroupReducePartialITCase(ExecutionMode mode) {
		super(mode);
	}

	private String resultPath;

	private String expected;

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

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception {
		if (expected != null) {
			compareResultsByLinesInMemory(expected, resultPath);
		}
	}

	@Test
	public void testAllGroupReducePartial() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);


		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds
				// reduce partially
				.reduceGroupPartially(new IdentityFunction())
				// fully reduce
				.reduceGroup(new IdentityFunction());


		reduceDs.writeAsCsv(resultPath);

		env.execute();

		expected = identityResult;
	}

	@Test
	public void testIdentity() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds
				// reduce partially
				.reduceGroupPartially(new IdentityFunction())
				// fully reduce
				.reduceGroup(new IdentityFunction());

		reduceDs.writeAsCsv(resultPath);

		env.execute();

		expected = identityResult;
	}

	@Test
	public void testIdentityWithGroupBy() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds
				.groupBy(1)
				// reduce partially
				.reduceGroupPartially(new IdentityFunction())
				// fully reduce
				.reduceGroup(new IdentityFunction());


		reduceDs.writeAsCsv(resultPath);

		env.execute();

		expected = identityResult;
	}

	@Test
	public void testIdentityWithGroupByAndSort() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds
				.groupBy(1)
				.sortGroup(1, Order.DESCENDING)
				// reduce partially
				.reduceGroupPartially(new IdentityFunction())
				.groupBy(1)
				.sortGroup(1, Order.DESCENDING)
				// fully reduce
				.reduceGroup(new IdentityFunction());

		reduceDs.writeAsCsv(resultPath);

		env.execute();

		expected = identityResult;
	}

	@Test
	public void testPartialReduceWithIdenticalInputOutputType() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// data
		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple2<Long, Tuple3<Integer, Long, String>>> dsWrapped = ds
				// wrap values as Kv pairs with the grouping key as key
				.map(new Tuple3KvWrapper());

		dsWrapped
				.groupBy(0)
				// reduce partially
				.reduceGroupPartially(new Tuple3toTuple3GroupReduce())
				.groupBy(0)
				// reduce fully to check result
				.reduceGroup(new Tuple3toTuple3GroupReduce())
				//unwrap
				.map(new MapFunction<Tuple2<Long, Tuple3<Integer, Long, String>>, Tuple3<Integer, Long, String>>() {
					@Override
					public Tuple3<Integer, Long, String> map(Tuple2<Long, Tuple3<Integer, Long, String>> value) throws Exception {
						return value.f1;
					}
				})
				.writeAsCsv(resultPath);



		env.execute();

		expected = "1,1,reduced\n" +
				"5,4,reduced\n" +
				"15,9,reduced\n" +
				"34,16,reduced\n" +
				"65,25,reduced\n" +
				"111,36,reduced\n";
	}

	@Test
	public void testPartialReduceWithDifferentInputOutputType() throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// data
		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);

		DataSet<Tuple2<Long, Tuple3<Integer, Long, String>>> dsWrapped = ds
				// wrap values as Kv pairs with the grouping key as key
				.map(new Tuple3KvWrapper());

		dsWrapped
				.groupBy(0)
						// reduce partially
				.reduceGroupPartially(new Tuple3toTuple2GroupReduce())
				.groupBy(0)
						// reduce fully to check result
				.reduceGroup(new Tuple2toTuple2GroupReduce())
						//unwrap
				.map(new MapFunction<Tuple2<Long,Tuple2<Integer,Long>>, Tuple2<Integer,Long>>() {
					@Override
					public Tuple2<Integer, Long> map(Tuple2<Long, Tuple2<Integer, Long>> value) throws Exception {
						return value.f1;
					}
				})
				.writeAsCsv(resultPath);



		env.execute();

		expected = "1,3\n" +
				"5,20\n" +
				"15,58\n" +
				"34,52\n" +
				"65,70\n" +
				"111,96\n";

	}

	public static class IdentityFunction implements GroupReduceFunction<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {

		@Override
		public void reduce(Iterable<Tuple3<Integer, Long, String>> values, Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			for (Tuple3<Integer, Long, String> value : values) {
				out.collect(new Tuple3<Integer, Long, String>(value.f0, value.f1, value.f2));
			}
		}
	}


	public static class Tuple3toTuple3GroupReduce implements KvGroupReduce<Long, Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {

		public void reduce(Iterable<Tuple2<Long, Tuple3<Integer, Long, String>>> values, Collector<Tuple2<Long, Tuple3<Integer, Long, String>>> out) throws Exception {
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

			Tuple3<Integer, Long, String> result = new Tuple3<Integer, Long, String>(i, l, "reduced");
			out.collect(new Tuple2<Long, Tuple3<Integer, Long, String>>(key, result));
		}
	}

	public static class Tuple3toTuple2GroupReduce implements KvGroupReduce<Long, Tuple3<Integer, Long, String>, Tuple2<Integer, Long>> {

		@Override
		public void reduce(Iterable<Tuple2<Long, Tuple3<Integer, Long, String>>> values, Collector<Tuple2<Long, Tuple2<Integer, Long>>> out) throws Exception {
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

			Tuple2<Integer, Long> result = new Tuple2<Integer, Long>(i, l);
			out.collect(new Tuple2<Long, Tuple2<Integer, Long>>(key, result));
		}
	}

	public static class Tuple2toTuple2GroupReduce implements KvGroupReduce<Long, Tuple2<Integer, Long>, Tuple2<Integer, Long>> {

		public void reduce(Iterable<Tuple2<Long, Tuple2<Integer, Long>>> values, Collector<Tuple2<Long, Tuple2<Integer, Long>>> out) throws Exception {
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

			Tuple2<Integer, Long> result = new Tuple2<Integer, Long>(i, l);

			out.collect(new Tuple2<Long, Tuple2<Integer, Long>>(key, result));
		}
	}

	public class Tuple3KvWrapper implements MapFunction<Tuple3<Integer, Long, String>, Tuple2<Long, Tuple3<Integer, Long, String>>> {
		@Override
		public Tuple2<Long, Tuple3<Integer, Long, String>> map(Tuple3<Integer, Long, String> value) throws Exception {
			return new Tuple2<Long,Tuple3<Integer, Long, String>>(value.f1, value);
		}
	}

	public interface KvGroupReduce<K, V, OUT> extends GroupReduceFunction<Tuple2<K, V>, Tuple2<K, OUT>> {
	}

}