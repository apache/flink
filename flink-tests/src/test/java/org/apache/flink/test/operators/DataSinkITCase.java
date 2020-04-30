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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * Tests for data sinks.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class DataSinkITCase extends MultipleProgramsTestBase {

	public DataSinkITCase(TestExecutionMode mode) {
		super(mode);
	}

	private String resultPath;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		final File folder = tempFolder.newFolder();
		final File resultFile = new File(folder, UUID.randomUUID().toString());
		resultPath = resultFile.toURI().toString();
	}

	@Test
	public void testIntSortingParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> ds = CollectionDataSets.getIntegerDataSet(env);
		ds.writeAsText(resultPath).sortLocalOutput("*", Order.DESCENDING).setParallelism(1);

		env.execute();

		String expected = "5\n5\n5\n5\n5\n4\n4\n4\n4\n3\n3\n3\n2\n2\n1\n";
		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);

	}

	@Test
	public void testStringSortingParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> ds = CollectionDataSets.getStringDataSet(env);
		ds.writeAsText(resultPath).sortLocalOutput("*", Order.ASCENDING).setParallelism(1);

		env.execute();

		String expected = "Hello\n" +
				"Hello world\n" +
				"Hello world, how are you?\n" +
				"Hi\n" +
				"I am fine.\n" +
				"LOL\n" +
				"Luke Skywalker\n" +
				"Random comment\n";

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);
	}

	@Test
	public void testTupleSortingSingleAscParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		ds.writeAsCsv(resultPath).sortLocalOutput(0, Order.ASCENDING).setParallelism(1);

		env.execute();

		String expected = "1,1,Hi\n" +
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

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);
	}

	@Test
	public void testTupleSortingSingleDescParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		ds.writeAsCsv(resultPath).sortLocalOutput(0, Order.DESCENDING).setParallelism(1);

		env.execute();

		String expected = "21,6,Comment#15\n" +
				"20,6,Comment#14\n" +
				"19,6,Comment#13\n" +
				"18,6,Comment#12\n" +
				"17,6,Comment#11\n" +
				"16,6,Comment#10\n" +
				"15,5,Comment#9\n" +
				"14,5,Comment#8\n" +
				"13,5,Comment#7\n" +
				"12,5,Comment#6\n" +
				"11,5,Comment#5\n" +
				"10,4,Comment#4\n" +
				"9,4,Comment#3\n" +
				"8,4,Comment#2\n" +
				"7,4,Comment#1\n" +
				"6,3,Luke Skywalker\n" +
				"5,3,I am fine.\n" +
				"4,3,Hello world, how are you?\n" +
				"3,2,Hello world\n" +
				"2,2,Hello\n" +
				"1,1,Hi\n";

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);
	}

	@Test
	public void testTupleSortingDualParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		ds.writeAsCsv(resultPath)
			.sortLocalOutput(1, Order.DESCENDING).sortLocalOutput(0, Order.ASCENDING)
			.setParallelism(1);

		env.execute();

		String expected = "16,6,Comment#10\n" +
				"17,6,Comment#11\n" +
				"18,6,Comment#12\n" +
				"19,6,Comment#13\n" +
				"20,6,Comment#14\n" +
				"21,6,Comment#15\n" +
				"11,5,Comment#5\n" +
				"12,5,Comment#6\n" +
				"13,5,Comment#7\n" +
				"14,5,Comment#8\n" +
				"15,5,Comment#9\n" +
				"7,4,Comment#1\n" +
				"8,4,Comment#2\n" +
				"9,4,Comment#3\n" +
				"10,4,Comment#4\n" +
				"4,3,Hello world, how are you?\n" +
				"5,3,I am fine.\n" +
				"6,3,Luke Skywalker\n" +
				"2,2,Hello\n" +
				"3,2,Hello world\n" +
				"1,1,Hi\n";

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);
	}

	@Test
	public void testTupleSortingNestedParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Tuple2<Integer, Integer>, String, Integer>> ds =
				CollectionDataSets.getGroupSortedNestedTupleDataSet2(env);
		ds.writeAsText(resultPath)
			.sortLocalOutput("f0.f1", Order.ASCENDING)
			.sortLocalOutput("f1", Order.DESCENDING)
			.setParallelism(1);

		env.execute();

		String expected =
				"((2,1),a,3)\n" +
				"((2,2),b,4)\n" +
				"((1,2),a,1)\n" +
				"((3,3),c,5)\n" +
				"((1,3),a,2)\n" +
				"((3,6),c,6)\n" +
				"((4,9),c,7)\n";

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);
	}

	@Test
	public void testTupleSortingNestedParallelism1_2() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Tuple2<Integer, Integer>, String, Integer>> ds =
				CollectionDataSets.getGroupSortedNestedTupleDataSet2(env);
		ds.writeAsText(resultPath)
			.sortLocalOutput(1, Order.ASCENDING)
			.sortLocalOutput(2, Order.DESCENDING)
			.setParallelism(1);

		env.execute();

		String expected =
				"((2,1),a,3)\n" +
				"((1,3),a,2)\n" +
				"((1,2),a,1)\n" +
				"((2,2),b,4)\n" +
				"((4,9),c,7)\n" +
				"((3,6),c,6)\n" +
				"((3,3),c,5)\n";

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);
	}

	@Test
	public void testPojoSortingSingleParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<CollectionDataSets.POJO> ds = CollectionDataSets.getMixedPojoDataSet(env);
		ds.writeAsText(resultPath).sortLocalOutput("number", Order.ASCENDING).setParallelism(1);

		env.execute();

		String expected = "1 First (10,100,1000,One) 10100\n" +
				"2 First_ (10,105,1000,One) 10200\n" +
				"3 First (11,102,3000,One) 10200\n" +
				"4 First_ (11,106,1000,One) 10300\n" +
				"5 First (11,102,2000,One) 10100\n" +
				"6 Second_ (20,200,2000,Two) 10100\n" +
				"7 Third (31,301,2000,Three) 10200\n" +
				"8 Third_ (30,300,1000,Three) 10100\n";

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);
	}

	@Test
	public void testPojoSortingDualParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<CollectionDataSets.POJO> ds = CollectionDataSets.getMixedPojoDataSet(env);
		ds.writeAsText(resultPath)
			.sortLocalOutput("str", Order.ASCENDING)
			.sortLocalOutput("number", Order.DESCENDING)
			.setParallelism(1);

		env.execute();

		String expected =
				"5 First (11,102,2000,One) 10100\n" +
				"3 First (11,102,3000,One) 10200\n" +
				"1 First (10,100,1000,One) 10100\n" +
				"4 First_ (11,106,1000,One) 10300\n" +
				"2 First_ (10,105,1000,One) 10200\n" +
				"6 Second_ (20,200,2000,Two) 10100\n" +
				"7 Third (31,301,2000,Three) 10200\n" +
				"8 Third_ (30,300,1000,Three) 10100\n";

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);

	}

	@Test
	public void testPojoSortingNestedParallelism1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<CollectionDataSets.POJO> ds = CollectionDataSets.getMixedPojoDataSet(env);
		ds.writeAsText(resultPath)
			.sortLocalOutput("nestedTupleWithCustom.f0", Order.ASCENDING)
			.sortLocalOutput("nestedTupleWithCustom.f1.myInt", Order.DESCENDING)
			.sortLocalOutput("nestedPojo.longNumber", Order.ASCENDING)
			.setParallelism(1);

		env.execute();

		String expected =
				"2 First_ (10,105,1000,One) 10200\n" +
				"1 First (10,100,1000,One) 10100\n" +
				"4 First_ (11,106,1000,One) 10300\n" +
				"5 First (11,102,2000,One) 10100\n" +
				"3 First (11,102,3000,One) 10200\n" +
				"6 Second_ (20,200,2000,Two) 10100\n" +
				"8 Third_ (30,300,1000,Three) 10100\n" +
				"7 Third (31,301,2000,Three) 10200\n";

		compareResultsByLinesInMemoryWithStrictOrder(expected, resultPath);
	}

	@Test
	public void testSortingParallelism4() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> ds = env.generateSequence(0, 1000);
		// randomize
		ds.map(new MapFunction<Long, Long>() {

			Random rand = new Random(1234L);
			@Override
			public Long map(Long value) throws Exception {
				return rand.nextLong();
			}
		}).writeAsText(resultPath)
			.sortLocalOutput("*", Order.ASCENDING)
			.setParallelism(4);

		env.execute();

		BufferedReader[] resReaders = getResultReader(resultPath);
		for (BufferedReader br : resReaders) {
			long cmp = Long.MIN_VALUE;
			while (br.ready()) {
				long cur = Long.parseLong(br.readLine());
				assertTrue("Invalid order of sorted output", cmp <= cur);
				cmp = cur;
			}
			br.close();
		}
	}

}
