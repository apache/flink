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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

/**
 * Integration tests for {@link FilterFunction} and {@link RichFilterFunction}.
 */
@RunWith(Parameterized.class)
public class FilterITCase extends MultipleProgramsTestBase {
	public FilterITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testAllRejectingFilter() throws Exception {
		/*
		 * Test all-rejecting filter.
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
				filter(new Filter1());

		List<Tuple3<Integer, Long, String>> result = filterDs.collect();

		String expected = "\n";

		compareResultAsTuples(result, expected);
	}

	private static class Filter1 implements FilterFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
			return false;
		}
	}

	@Test
	public void testAllPassingFilter() throws Exception {
		/*
		 * Test all-passing filter.
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
				filter(new Filter2());
		List<Tuple3<Integer, Long, String>> result = filterDs.collect();

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

		compareResultAsTuples(result, expected);
	}

	private static class Filter2 implements FilterFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
			return true;
		}
	}

	@Test
	public void testFilterOnStringTupleField() throws Exception {
		/*
		 * Test filter on String tuple field.
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
				filter(new Filter3());
		List<Tuple3<Integer, Long, String>> result = filterDs.collect();

		String expected = "3,2,Hello world\n"
				+
				"4,3,Hello world, how are you?\n";

		compareResultAsTuples(result, expected);

	}

	private static class Filter3 implements FilterFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
			return value.f2.contains("world");
		}
	}

	@Test
	public void testFilterOnIntegerTupleField() throws Exception {
		/*
		 * Test filter on Integer tuple field.
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
				filter(new Filter4());
		List<Tuple3<Integer, Long, String>> result = filterDs.collect();

		String expected = "2,2,Hello\n" +
				"4,3,Hello world, how are you?\n" +
				"6,3,Luke Skywalker\n" +
				"8,4,Comment#2\n" +
				"10,4,Comment#4\n" +
				"12,5,Comment#6\n" +
				"14,5,Comment#8\n" +
				"16,6,Comment#10\n" +
				"18,6,Comment#12\n" +
				"20,6,Comment#14\n";

		compareResultAsTuples(result, expected);
	}

	private static class Filter4 implements FilterFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
			return (value.f0 % 2) == 0;
		}
	}

	@Test
	public void testFilterBasicType() throws Exception {
		/*
		 * Test filter on basic type
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> ds = CollectionDataSets.getStringDataSet(env);
		DataSet<String> filterDs = ds.
				filter(new Filter5());
		List<String> result = filterDs.collect();

		String expected = "Hi\n" +
				"Hello\n" +
				"Hello world\n" +
				"Hello world, how are you?\n";

		compareResultAsText(result, expected);
	}

	private static class Filter5 implements FilterFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(String value) throws Exception {
			return value.startsWith("H");
		}
	}

	@Test
	public void testFilterOnCustomType() throws Exception {
		/*
		 * Test filter on custom type
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
		DataSet<CustomType> filterDs = ds.
				filter(new Filter6());
		List<CustomType> result = filterDs.collect();

		String expected = "3,3,Hello world, how are you?\n"
				+
				"3,4,I am fine.\n" +
				"3,5,Luke Skywalker\n";

		compareResultAsText(result, expected);
	}

	private static class Filter6 implements FilterFunction<CustomType> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(CustomType value) throws Exception {
			return value.myString.contains("a");
		}
	}

	@Test
	public void testRichFilterOnStringTupleField() throws Exception {
		/*
		 * Test filter on String tuple field.
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> ints = CollectionDataSets.getIntegerDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
				filter(new RichFilter1()).withBroadcastSet(ints, "ints");
		List<Tuple3<Integer, Long, String>> result = filterDs.collect();

		String expected = "1,1,Hi\n" +
				"2,2,Hello\n" +
				"3,2,Hello world\n" +
				"4,3,Hello world, how are you?\n";

		compareResultAsTuples(result, expected);
	}

	private static class RichFilter1 extends RichFilterFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;

		int literal = -1;

		@Override
		public void open(Configuration config) {
			Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
			for (int i: ints) {
				literal = literal < i ? i : literal;
			}
		}

		@Override
		public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
			return value.f0 < literal;
		}
	}

	@Test
	public void testFilterWithBroadcastVariables() throws Exception {
		/*
		 * Test filter with broadcast variables
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> filterDs = ds.
				filter(new RichFilter2()).withBroadcastSet(intDs, "ints");
		List<Tuple3<Integer, Long, String>> result = filterDs.collect();

		String expected = "11,5,Comment#5\n" +
				"12,5,Comment#6\n" +
				"13,5,Comment#7\n" +
				"14,5,Comment#8\n" +
				"15,5,Comment#9\n";

		compareResultAsTuples(result, expected);
	}

	private static class RichFilter2 extends RichFilterFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		private  int broadcastSum = 0;

		@Override
		public void open(Configuration config) {
			Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
			for (Integer i : ints) {
				broadcastSum += i;
			}
		}

		@Override
		public boolean filter(Tuple3<Integer, Long, String> value) throws Exception {
			return (value.f1 == (broadcastSum / 11));
		}
	}
}
