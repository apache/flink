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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.operators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

/**
 * Integration tests for {@link JoinFunction}, {@link FlatJoinFunction},
 * and {@link RichFlatJoinFunction}.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class OuterJoinITCase extends MultipleProgramsTestBase {

	public OuterJoinITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testLeftOuterJoin1() throws Exception {
		testLeftOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_SORT_MERGE);
	}

	@Test
	public void testLeftOuterJoin2() throws Exception {
		testLeftOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_HASH_FIRST);
	}

	@Test
	public void testLeftOuterJoin3() throws Exception {
		testLeftOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_HASH_SECOND);
	}

	@Test
	public void testLeftOuterJoin4() throws Exception {
		testLeftOuterJoinOnTuplesWithKeyPositions(JoinHint.BROADCAST_HASH_SECOND);
	}

	@Test(expected = InvalidProgramException.class)
	public void testLeftOuterJoin5() throws Exception {
		testLeftOuterJoinOnTuplesWithKeyPositions(JoinHint.BROADCAST_HASH_FIRST);
	}

	private void testLeftOuterJoinOnTuplesWithKeyPositions(JoinHint hint) throws Exception {
		/*
		 * UDF Join on tuples with key field positions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.leftOuterJoin(ds2, hint)
						.where(0)
						.equalTo(0)
						.with(new T3T5FlatJoin());

		List<Tuple2<String, String>> result = joinDs.collect();

		String expected = "Hi,Hallo\n" +
				"Hello,Hallo Welt\n" +
				"Hello,Hallo Welt wie\n" +
				"Hello world,null\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testRightOuterJoin1() throws Exception {
		testRightOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_SORT_MERGE);
	}

	@Test
	public void testRightOuterJoin2() throws Exception {
		testRightOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_HASH_FIRST);
	}

	@Test
	public void testRightOuterJoin3() throws Exception {
		testRightOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_HASH_SECOND);
	}

	@Test
	public void testRightOuterJoin4() throws Exception {
		testRightOuterJoinOnTuplesWithKeyPositions(JoinHint.BROADCAST_HASH_FIRST);
	}

	@Test (expected = InvalidProgramException.class)
	public void testRightOuterJoin5() throws Exception {
		testRightOuterJoinOnTuplesWithKeyPositions(JoinHint.BROADCAST_HASH_SECOND);
	}

	private void testRightOuterJoinOnTuplesWithKeyPositions(JoinHint hint) throws Exception {
		/*
		 * UDF Join on tuples with key field positions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.rightOuterJoin(ds2, hint)
						.where(1)
						.equalTo(1)
						.with(new T3T5FlatJoin());

		List<Tuple2<String, String>> result = joinDs.collect();

		String expected = "Hi,Hallo\n" +
				"Hello,Hallo Welt\n" +
				"null,Hallo Welt wie\n" +
				"Hello world,Hallo Welt\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testFullOuterJoin1() throws Exception {
		testFullOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_SORT_MERGE);
	}

	@Test
	public void testFullOuterJoin2() throws Exception {
		testFullOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_HASH_FIRST);
	}

	@Test
	public void testFullOuterJoin3() throws Exception {
		testFullOuterJoinOnTuplesWithKeyPositions(JoinHint.REPARTITION_HASH_SECOND);
	}

	@Test (expected = InvalidProgramException.class)
	public void testFullOuterJoin4() throws Exception {
		testFullOuterJoinOnTuplesWithKeyPositions(JoinHint.BROADCAST_HASH_FIRST);
	}

	@Test (expected = InvalidProgramException.class)
	public void testFullOuterJoin5() throws Exception {
		testFullOuterJoinOnTuplesWithKeyPositions(JoinHint.BROADCAST_HASH_SECOND);
	}

	private void testFullOuterJoinOnTuplesWithKeyPositions(JoinHint hint) throws Exception {
		/*
		 * UDF Join on tuples with key field positions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.fullOuterJoin(ds2, hint)
						.where(0)
						.equalTo(2)
						.with(new T3T5FlatJoin());

		List<Tuple2<String, String>> result = joinDs.collect();

		String expected = "null,Hallo\n" +
				"Hi,Hallo Welt\n" +
				"Hello,Hallo Welt wie\n" +
				"Hello world,null\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinOnTuplesWithCompositeKeyPositions() throws Exception {
		/*
		 * UDF Join on tuples with multiple key field positions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(0, 1)
						.equalTo(0, 4)
						.with(new T3T5FlatJoin());

		List<Tuple2<String, String>> result = joinDs.collect();

		String expected = "Hi,Hallo\n" +
				"Hello,Hallo Welt\n" +
				"Hello world,null\n" +
				"null,Hallo Welt wie\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinWithBroadcastSet() throws Exception {
		/*
		 * Join with broadcast set
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple3<String, String, Integer>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(1)
						.equalTo(4)
						.with(new T3T5BCJoin())
						.withBroadcastSet(intDs, "ints");

		List<Tuple3<String, String, Integer>> result = joinDs.collect();

		String expected = "Hi,Hallo,55\n" +
				"Hi,Hallo Welt wie,55\n" +
				"Hello,Hallo Welt,55\n" +
				"Hello world,Hallo Welt,55\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinWithMixedKeyTypes1() throws Exception {
		/*
		 * Join on a tuple input with key field selector and a custom type input with key extractor
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<CustomType> ds1 = CollectionDataSets.getSmallCustomTypeDataSet(env);
		DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(new KeySelector1())
						.equalTo(0)
						.with(new CustT3Join());

		List<Tuple2<String, String>> result = joinDs.collect();

		String expected = "Hi,Hi\n" +
				"Hello,Hello\n" +
				"Hello world,Hello\n" +
				"null,Hello world\n";

		compareResultAsTuples(result, expected);

	}

	private static class KeySelector1 implements KeySelector<CustomType, Integer> {
		@Override
		public Integer getKey(CustomType value) {
			return value.myInt;
		}
	}

	@Test
	public void testJoinWithMixedKeyTypes2()
			throws Exception {
		/*
		 * Join on a tuple input with key field selector and a custom type input with key extractor
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<CustomType> ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(1)
						.equalTo(new KeySelector2())
						.with(new T3CustJoin());

		List<Tuple2<String, String>> result = joinDs.collect();

		String expected = "null,Hi\n" +
				"Hi,Hello\n" +
				"Hello,Hello world\n" +
				"Hello world,Hello world\n";

		compareResultAsTuples(result, expected);
	}

	private static class KeySelector2 implements KeySelector<CustomType, Long> {
		@Override
		public Long getKey(CustomType value) {
			return value.myLong;
		}
	}

	@Test
	public void testJoinWithTupleReturningKeySelectors() throws Exception {
		/*
		 * UDF Join on tuples with tuple-returning key selectors
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(new KeySelector3()) //0, 1
						.equalTo(new KeySelector4()) // 0, 4
						.with(new T3T5FlatJoin());

		List<Tuple2<String, String>> result = joinDs.collect();

		String expected = "Hi,Hallo\n" +
				"Hello,Hallo Welt\n" +
				"Hello world,null\n" +
				"null,Hallo Welt wie\n";

		compareResultAsTuples(result, expected);
	}

	private static class KeySelector3 implements KeySelector<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Long> getKey(Tuple3<Integer, Long, String> t) {
			return new Tuple2<>(t.f0, t.f1);
		}
	}

	private static class KeySelector4 implements KeySelector<Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Long> getKey(Tuple5<Integer, Long, Integer, String, Long> t) {
			return new Tuple2<>(t.f0, t.f4);
		}
	}

	@Test
	public void testJoinWithNestedKeyExpression1() throws Exception {
		/*
		 * Join nested pojo against tuple (selected using a string)
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
		DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
		DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where("nestedPojo.longNumber")
						.equalTo("f6")
						.with(new ProjectBothFunction<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>());

		List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result = joinDs.collect();

		String expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
				"2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
				"3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinWithNestedKeyExpression2() throws Exception {
		/*
		 * Join nested pojo against tuple (selected as an integer)
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
		DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
		DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where("nestedPojo.longNumber")
						.equalTo(6) // <--- difference!
						.with(new ProjectBothFunction<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>());

		List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result = joinDs.collect();

		String expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
				"2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
				"3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinWithCompositeKeyExpressions() throws Exception {
		/*
		 * selecting multiple fields using expression language
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
		DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
		DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where("nestedPojo.longNumber", "number", "str")
						.equalTo("f6", "f0", "f1")
						.with(new ProjectBothFunction<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>());

		env.setParallelism(1);
		List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result = joinDs.collect();

		String expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
				"2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
				"3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testNestedIntoTuple() throws Exception {
		/*
		 * nested into tuple
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
		DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
		DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where("nestedPojo.longNumber", "number", "nestedTupleWithCustom.f0")
						.equalTo("f6", "f0", "f2")
						.with(new ProjectBothFunction<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>());

		env.setParallelism(1);
		List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result = joinDs.collect();

		String expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
				"2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
				"3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testNestedIntoTupleIntoPojo() throws Exception {
		/*
		 * nested into tuple into pojo
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
		DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 = CollectionDataSets.getSmallTuplebasedDataSet(env);
		DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where("nestedTupleWithCustom.f0", "nestedTupleWithCustom.f1.myInt", "nestedTupleWithCustom.f1.myLong")
						.equalTo("f2", "f3", "f4")
						.with(new ProjectBothFunction<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>());

		env.setParallelism(1);
		List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result = joinDs.collect();

		String expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
				"2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
				"3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testNonPojoToVerifyFullTupleKeys() throws Exception {
		/*
		 * Non-POJO test to verify that full-tuple keys are working.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds1 = CollectionDataSets.getSmallNestedTupleDataSet(env);
		DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds2 = CollectionDataSets.getSmallNestedTupleDataSet(env);
		DataSet<Tuple2<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<Tuple2<Integer, Integer>, String>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(0)
						.equalTo("f0.f0", "f0.f1") // key is now Tuple2<Integer, Integer>
						.with(new ProjectBothFunction<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<Tuple2<Integer, Integer>, String>>());

		env.setParallelism(1);
		List<Tuple2<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<Tuple2<Integer, Integer>, String>>> result = joinDs.collect();

		String expected = "((1,1),one),((1,1),one)\n" +
				"((2,2),two),((2,2),two)\n" +
				"((3,3),three),((3,3),three)\n";

		compareResultAsTuples(result, expected);

	}

	@Test
	public void testNonPojoToVerifyNestedTupleElementSelection() throws Exception {
		/*
		 * Non-POJO test to verify "nested" tuple-element selection.
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds1 = CollectionDataSets.getSmallNestedTupleDataSet(env);
		DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds2 = CollectionDataSets.getSmallNestedTupleDataSet(env);
		DataSet<Tuple2<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<Tuple2<Integer, Integer>, String>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where("f0.f0")
						.equalTo("f0.f0") // key is now Integer from Tuple2<Integer, Integer>
						.with(new ProjectBothFunction<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<Tuple2<Integer, Integer>, String>>());

		env.setParallelism(1);
		List<Tuple2<Tuple2<Tuple2<Integer, Integer>, String>, Tuple2<Tuple2<Integer, Integer>, String>>> result = joinDs.collect();

		String expected = "((1,1),one),((1,1),one)\n" +
				"((2,2),two),((2,2),two)\n" +
				"((3,3),three),((3,3),three)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testFullPojoWithFullTuple() throws Exception {
		/*
		 * full pojo with full tuple
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
		DataSet<Tuple7<Long, Integer, Integer, Long, String, Integer, String>> ds2 = CollectionDataSets.getSmallTuplebasedDataSetMatchingPojo(env);
		DataSet<Tuple2<POJO, Tuple7<Long, Integer, Integer, Long, String, Integer, String>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where("*")
						.equalTo("*")
						.with(new ProjectBothFunction<POJO, Tuple7<Long, Integer, Integer, Long, String, Integer, String>>());

		env.setParallelism(1);
		List<Tuple2<POJO, Tuple7<Long, Integer, Integer, Long, String, Integer, String>>> result = joinDs.collect();

		String expected = "1 First (10,100,1000,One) 10000,(10000,10,100,1000,One,1,First)\n" +
				"2 Second (20,200,2000,Two) 20000,(20000,20,200,2000,Two,2,Second)\n" +
				"3 Third (30,300,3000,Three) 30000,(30000,30,300,3000,Three,3,Third)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinWithAtomicType1() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Integer> ds2 = env.fromElements(1, 2);

		DataSet<Tuple2<Tuple3<Integer, Long, String>, Integer>> joinDs = ds1
				.fullOuterJoin(ds2)
				.where(0)
				.equalTo("*")
				.with(new ProjectBothFunction<Tuple3<Integer, Long, String>, Integer>())
				.returns(new GenericTypeInfo(Tuple2.class));

		List<Tuple2<Tuple3<Integer, Long, String>, Integer>> result = joinDs.collect();

		String expected = "(1,1,Hi),1\n" +
				"(2,2,Hello),2\n" +
				"(3,2,Hello world),null\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinWithAtomicType2() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> ds1 = env.fromElements(1, 2);
		DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);

		DataSet<Tuple2<Integer, Tuple3<Integer, Long, String>>> joinDs = ds1
				.fullOuterJoin(ds2)
				.where("*")
				.equalTo(0)
				.with(new ProjectBothFunction<Integer, Tuple3<Integer, Long, String>>())
				.returns(new GenericTypeInfo(Tuple2.class));

		List<Tuple2<Integer, Tuple3<Integer, Long, String>>> result = joinDs.collect();

		String expected = "1,(1,1,Hi)\n" +
				"2,(2,2,Hello)\n" +
				"null,(3,2,Hello world)\n";

		compareResultAsTuples(result, expected);
	}

	private static class T3T5FlatJoin implements FlatJoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple2<String, String>> {

		@Override
		public void join(Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second,
				Collector<Tuple2<String, String>> out) {

			out.collect(new Tuple2<>(first == null ? null : first.f2, second == null ? null : second.f3));
		}

	}

	private static class T3T5BCJoin extends RichFlatJoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<String, String, Integer>> {

		private int broadcast;

		@Override
		public void open(Configuration config) {
			Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
			int sum = 0;
			for (Integer i : ints) {
				sum += i;
			}
			broadcast = sum;
		}

		@Override
		public void join(Tuple3<Integer, Long, String> first, Tuple5<Integer, Long, Integer, String, Long> second,
				Collector<Tuple3<String, String, Integer>> out) throws Exception {
			out.collect(new Tuple3<>(first == null ? null : first.f2, second == null ? null : second.f3, broadcast));
		}
	}

	private static class T3CustJoin implements JoinFunction<Tuple3<Integer, Long, String>, CustomType, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(Tuple3<Integer, Long, String> first,
				CustomType second) {

			return new Tuple2<>(first == null ? null : first.f2, second == null ? null : second.myString);
		}
	}

	private static class CustT3Join implements JoinFunction<CustomType, Tuple3<Integer, Long, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(CustomType first, Tuple3<Integer, Long, String> second) {

			return new Tuple2<>(first == null ? null : first.myString, second == null ? null : second.f2);
		}
	}

	/**
	 * Deliberately untyped join function, which emits a Tuple2 of the left and right side.
	 */
	private static class ProjectBothFunction<IN1, IN2> implements JoinFunction<IN1, IN2, Tuple2<IN1, IN2>> {
		@Override
		public Tuple2<IN1, IN2> join(IN1 first, IN2 second) throws Exception {
			return new Tuple2<>(first, second);
		}
	}
}
