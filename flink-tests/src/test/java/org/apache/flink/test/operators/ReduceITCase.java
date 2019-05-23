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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.operators.util.CollectionDataSets.PojoWithDateAndEnum;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Integration tests for {@link ReduceFunction} and {@link RichReduceFunction}.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class ReduceITCase extends MultipleProgramsTestBase {

	public ReduceITCase(TestExecutionMode mode){
		super(mode);
	}

	@Test
	public void testReduceOnTuplesWithKeyFieldSelector() throws Exception {
		/*
		 * Reduce on tuples with key field selector
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
				groupBy(1).reduce(new Tuple3Reduce("B-)"));

		List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

		String expected = "1,1,Hi\n" +
				"5,2,B-)\n" +
				"15,3,B-)\n" +
				"34,4,B-)\n" +
				"65,5,B-)\n" +
				"111,6,B-)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testReduceOnTupleWithMultipleKeyFieldSelectors() throws Exception{
		/*
		 * Reduce on tuples with multiple key field selectors
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs = ds.
				groupBy(4, 0).reduce((in1, in2) -> {
					Tuple5<Integer, Long, Integer, String, Long> out = new Tuple5<>();
					out.setFields(in1.f0, in1.f1 + in2.f1, 0, "P-)", in1.f4);
					return out;
				});

		List<Tuple5<Integer, Long, Integer, String, Long>> result = reduceDs
				.collect();

		String expected = "1,1,0,Hallo,1\n" +
				"2,3,2,Hallo Welt wie,1\n" +
				"2,2,1,Hallo Welt,2\n" +
				"3,9,0,P-),2\n" +
				"3,6,5,BCD,3\n" +
				"4,17,0,P-),1\n" +
				"4,17,0,P-),2\n" +
				"5,11,10,GHI,1\n" +
				"5,29,0,P-),2\n" +
				"5,25,0,P-),3\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testReduceOnTuplesWithKeyExtractor() throws Exception {
		/*
		 * Reduce on tuples with key extractor
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
				groupBy(new KeySelector1()).reduce(new Tuple3Reduce("B-)"));

		List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

		String expected = "1,1,Hi\n" +
				"5,2,B-)\n" +
				"15,3,B-)\n" +
				"34,4,B-)\n" +
				"65,5,B-)\n" +
				"111,6,B-)\n";

		compareResultAsTuples(result, expected);
	}

	private static class KeySelector1 implements KeySelector<Tuple3<Integer, Long, String>, Long> {
		private static final long serialVersionUID = 1L;
		@Override
		public Long getKey(Tuple3<Integer, Long, String> in) {
			return in.f1;
		}
	}

	@Test
	public void testReduceOnCustomTypeWithKeyExtractor() throws Exception {
		/*
		 * Reduce on custom type with key extractor
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
		DataSet<CustomType> reduceDs = ds.
				groupBy(new KeySelector2()).reduce(new CustomTypeReduce());

		List<CustomType> result = reduceDs.collect();

		String expected = "1,0,Hi\n" +
				"2,3,Hello!\n" +
				"3,12,Hello!\n" +
				"4,30,Hello!\n" +
				"5,60,Hello!\n" +
				"6,105,Hello!\n";

		compareResultAsText(result, expected);
	}

	private static class KeySelector2 implements KeySelector<CustomType, Integer> {
		private static final long serialVersionUID = 1L;
		@Override
		public Integer getKey(CustomType in) {
			return in.myInt;
		}
	}

	@Test
	public void testAllReduceForTuple() throws Exception {
		/*
		 * All-reduce for tuple
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
				reduce(new AllAddingTuple3Reduce());

		List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

		String expected = "231,91,Hello World\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testAllReduceForCustomTypes() throws Exception {
		/*
		 * All-reduce for custom types
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
		DataSet<CustomType> reduceDs = ds.
				reduce(new AllAddingCustomTypeReduce());

		List<CustomType> result = reduceDs.collect();

		String expected = "91,210,Hello!";

		compareResultAsText(result, expected);
	}

	@Test
	public void testReduceWithBroadcastSet() throws Exception {
		/*
		 * Reduce with broadcast set
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
				groupBy(1).reduce(new BCTuple3Reduce()).withBroadcastSet(intDs, "ints");

		List<Tuple3<Integer, Long, String>> result = reduceDs.collect();

		String expected = "1,1,Hi\n" +
				"5,2,55\n" +
				"15,3,55\n" +
				"34,4,55\n" +
				"65,5,55\n" +
				"111,6,55\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testReduceATupleReturningKeySelector() throws Exception {
		/*
		 * Reduce with a Tuple-returning KeySelector
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple5<Integer, Long,  Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		DataSet<Tuple5<Integer, Long,  Integer, String, Long>> reduceDs = ds
				.groupBy(new KeySelector3()).reduce(new Tuple5Reduce());

		List<Tuple5<Integer, Long, Integer, String, Long>> result = reduceDs
				.collect();

		String expected = "1,1,0,Hallo,1\n" +
				"2,3,2,Hallo Welt wie,1\n" +
				"2,2,1,Hallo Welt,2\n" +
				"3,9,0,P-),2\n" +
				"3,6,5,BCD,3\n" +
				"4,17,0,P-),1\n" +
				"4,17,0,P-),2\n" +
				"5,11,10,GHI,1\n" +
				"5,29,0,P-),2\n" +
				"5,25,0,P-),3\n";

		compareResultAsTuples(result, expected);
	}

	private static class KeySelector3 implements KeySelector<Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Long> getKey(Tuple5<Integer, Long, Integer, String, Long> t) {
			return new Tuple2<Integer, Long>(t.f0, t.f4);
		}
	}

	@Test
	public void testReduceOnTupleWithMultipleKeyExpressions() throws Exception {
		/*
		 * Case 2 with String-based field expression
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs = ds
				.groupBy("f4", "f0").reduce(new Tuple5Reduce());

		List<Tuple5<Integer, Long, Integer, String, Long>> result = reduceDs
				.collect();

		String expected = "1,1,0,Hallo,1\n" +
				"2,3,2,Hallo Welt wie,1\n" +
				"2,2,1,Hallo Welt,2\n" +
				"3,9,0,P-),2\n" +
				"3,6,5,BCD,3\n" +
				"4,17,0,P-),1\n" +
				"4,17,0,P-),2\n" +
				"5,11,10,GHI,1\n" +
				"5,29,0,P-),2\n" +
				"5,25,0,P-),3\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testReduceOnTupleWithMultipleKeyExpressionsWithHashHint() throws Exception {
		/*
		 * Case 2 with String-based field expression
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = CollectionDataSets.get5TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> reduceDs = ds
			.groupBy("f4", "f0").reduce(new Tuple5Reduce()).setCombineHint(CombineHint.HASH);

		List<Tuple5<Integer, Long, Integer, String, Long>> result = reduceDs
			.collect();

		String expected = "1,1,0,Hallo,1\n" +
			"2,3,2,Hallo Welt wie,1\n" +
			"2,2,1,Hallo Welt,2\n" +
			"3,9,0,P-),2\n" +
			"3,6,5,BCD,3\n" +
			"4,17,0,P-),1\n" +
			"4,17,0,P-),2\n" +
			"5,11,10,GHI,1\n" +
			"5,29,0,P-),2\n" +
			"5,25,0,P-),3\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testSupportForDataAndEnumSerialization() throws Exception {
		/**
		 * Test support for Date and enum serialization
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<PojoWithDateAndEnum> ds = env.generateSequence(0, 2).map(new Mapper1());
		ds = ds.union(CollectionDataSets.getPojoWithDateAndEnum(env));

		DataSet<String> res = ds.groupBy("group").reduceGroup(new GroupReducer1());

		List<String> result = res.collect();

		String expected = "ok\nok";

		compareResultAsText(result, expected);
	}

	private static class Mapper1 implements MapFunction<Long, PojoWithDateAndEnum> {
		@Override
		public PojoWithDateAndEnum map(Long value) throws Exception {
			int l = value.intValue();
			switch (l) {
				case 0:
					PojoWithDateAndEnum one = new PojoWithDateAndEnum();
					one.group = "a";
					one.date = new Date(666);
					one.cat = CollectionDataSets.Category.CAT_A;
					return one;
				case 1:
					PojoWithDateAndEnum two = new PojoWithDateAndEnum();
					two.group = "a";
					two.date = new Date(666);
					two.cat = CollectionDataSets.Category.CAT_A;
					return two;
				case 2:
					PojoWithDateAndEnum three = new PojoWithDateAndEnum();
					three.group = "b";
					three.date = new Date(666);
					three.cat = CollectionDataSets.Category.CAT_B;
					return three;
			}
			throw new RuntimeException("Unexpected value for l=" + l);
		}
	}

	private static class GroupReducer1 implements GroupReduceFunction<CollectionDataSets.PojoWithDateAndEnum, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<PojoWithDateAndEnum> values,
				Collector<String> out) throws Exception {
			for (PojoWithDateAndEnum val : values) {
				if (val.cat == CollectionDataSets.Category.CAT_A) {
					Assert.assertEquals("a", val.group);
				} else if (val.cat == CollectionDataSets.Category.CAT_B) {
					Assert.assertEquals("b", val.group);
				} else {
					Assert.fail("error. Cat = " + val.cat);
				}
				Assert.assertEquals(666, val.date.getTime());
			}
			out.collect("ok");
		}
	}

	private static class Tuple3Reduce implements ReduceFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		private final Tuple3<Integer, Long, String> out = new Tuple3<Integer, Long, String>();
		private final String f2Replace;

		public Tuple3Reduce() {
			this.f2Replace = null;
		}

		public Tuple3Reduce(String f2Replace) {
			this.f2Replace = f2Replace;
		}

		@Override
		public Tuple3<Integer, Long, String> reduce(
				Tuple3<Integer, Long, String> in1,
				Tuple3<Integer, Long, String> in2) throws Exception {

			if (f2Replace == null) {
				out.setFields(in1.f0 + in2.f0, in1.f1, in1.f2);
			} else {
				out.setFields(in1.f0 + in2.f0, in1.f1, this.f2Replace);
			}
			return out;
		}
	}

	private static class Tuple5Reduce implements ReduceFunction<Tuple5<Integer, Long, Integer, String, Long>> {
		private static final long serialVersionUID = 1L;
		private final Tuple5<Integer, Long, Integer, String, Long> out = new Tuple5<Integer, Long, Integer, String, Long>();

		@Override
		public Tuple5<Integer, Long, Integer, String, Long> reduce(
				Tuple5<Integer, Long, Integer, String, Long> in1,
				Tuple5<Integer, Long, Integer, String, Long> in2)
						throws Exception {

			out.setFields(in1.f0, in1.f1 + in2.f1, 0, "P-)", in1.f4);
			return out;
		}
	}

	private static class CustomTypeReduce implements ReduceFunction<CustomType> {
		private static final long serialVersionUID = 1L;
		private final CustomType out = new CustomType();

		@Override
		public CustomType reduce(CustomType in1, CustomType in2)
				throws Exception {

			out.myInt = in1.myInt;
			out.myLong = in1.myLong + in2.myLong;
			out.myString = "Hello!";
			return out;
		}
	}

	private static class AllAddingTuple3Reduce implements ReduceFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		private final Tuple3<Integer, Long, String> out = new Tuple3<Integer, Long, String>();

		@Override
		public Tuple3<Integer, Long, String> reduce(
				Tuple3<Integer, Long, String> in1,
				Tuple3<Integer, Long, String> in2) throws Exception {

			out.setFields(in1.f0 + in2.f0, in1.f1 + in2.f1, "Hello World");
			return out;
		}
	}

	private static class AllAddingCustomTypeReduce implements ReduceFunction<CustomType> {
		private static final long serialVersionUID = 1L;
		private final CustomType out = new CustomType();

		@Override
		public CustomType reduce(CustomType in1, CustomType in2)
				throws Exception {

			out.myInt = in1.myInt + in2.myInt;
			out.myLong = in1.myLong + in2.myLong;
			out.myString = "Hello!";
			return out;
		}
	}

	private static class BCTuple3Reduce extends RichReduceFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		private final Tuple3<Integer, Long, String> out = new Tuple3<Integer, Long, String>();
		private String f2Replace = "";

		@Override
		public void open(Configuration config) {

			Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
			int sum = 0;
			for (Integer i : ints) {
				sum += i;
			}
			f2Replace = sum + "";

		}

		@Override
		public Tuple3<Integer, Long, String> reduce(
				Tuple3<Integer, Long, String> in1,
				Tuple3<Integer, Long, String> in2) throws Exception {

			out.setFields(in1.f0 + in2.f0, in1.f1, this.f2Replace);
			return out;
		}
	}

}
