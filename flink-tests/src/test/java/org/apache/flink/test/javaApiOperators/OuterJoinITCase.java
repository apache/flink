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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class OuterJoinITCase extends MultipleProgramsTestBase {

	public OuterJoinITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Test
	public void testUDFLeftOuterJoinOnTuplesWithKeyFieldPositions() throws Exception {
		/*
		 * UDF Join on tuples with key field positions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.leftOuterJoin(ds2)
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
	public void testUDFRightOuterJoinOnTuplesWithKeyFieldPositions() throws Exception {
		/*
		 * UDF Join on tuples with key field positions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.rightOuterJoin(ds2)
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
	public void testUDFFullOuterJoinOnTuplesWithKeyFieldPositions() throws Exception {
		/*
		 * UDF Join on tuples with key field positions
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple2<String, String>> joinDs =
				ds1.fullOuterJoin(ds2)
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
	public void testUDFJoinOnTuplesWithMultipleKeyFieldPositions() throws Exception {
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

	@Test(expected = InvalidProgramException.class)
	public void testDefaultJoin() throws Exception {
		/*
		 * Default Join on tuples
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.get5TupleDataSet(env);
		DataSet<Tuple2<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(0)
						.equalTo(2);

		joinDs.collect();
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
	public void testJoinOnACustomTypeInputWithKeyExtractorAndATupleInputWithKeyFieldSelector() throws Exception {
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

	public static class KeySelector1 implements KeySelector<CustomType, Integer> {
		@Override
		public Integer getKey(CustomType value) {
			return value.myInt;
		}
	}

	@Test
	public void testProjectOnATuple1Input() throws Exception {
		/*
		 * Project join on a tuple input 1
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple6<String, Long, String, Integer, Long, Long>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(1)
						.equalTo(1)
						.projectFirst(2, 1)
						.projectSecond(3)
						.projectFirst(0)
						.projectSecond(4, 1);

		List<Tuple6<String, Long, String, Integer, Long, Long>> result = joinDs.collect();

		String expected = "Hi,1,Hallo,1,1,1\n" +
				"Hello,2,Hallo Welt,2,2,2\n" +
				"Hello world,2,Hallo Welt,3,2,2\n" +
				"null,null,Hallo Welt wie,null,1,3\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testProjectJoinOnATuple2Input() throws Exception {
		/*
		 * Project join on a tuple input 2
		 */

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = CollectionDataSets.getSmall5TupleDataSet(env);
		DataSet<Tuple6<String, String, Long, Long, Long, Integer>> joinDs =
				ds1.fullOuterJoin(ds2)
						.where(1)
						.equalTo(1)
						.projectSecond(3)
						.projectFirst(2, 1)
						.projectSecond(4, 1)
						.projectFirst(0);

		List<Tuple6<String, String, Long, Long, Long, Integer>> result = joinDs.collect();

		String expected = "Hallo,Hi,1,1,1,1\n" +
				"Hallo Welt,Hello,2,2,2,2\n" +
				"Hallo Welt,Hello world,2,2,2,3\n" +
				"Hallo Welt wie,null,null,1,3,null\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinOnATupleInputWithKeyFieldSelectorAndACustomTypeInputWithKeyExtractor()
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

	public static class KeySelector2 implements KeySelector<CustomType, Long> {
		@Override
		public Long getKey(CustomType value) {
			return value.myLong;
		}
	}

	@Test
	public void testUDFJoinOnTuplesWithTupleReturningKeySelectors() throws Exception {
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

	public static class KeySelector3 implements KeySelector<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Long> getKey(Tuple3<Integer, Long, String> t) {
			return new Tuple2<>(t.f0, t.f1);
		}
	}

	public static class KeySelector4 implements KeySelector<Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Long> getKey(Tuple5<Integer, Long, Integer, String, Long> t) {
			return new Tuple2<>(t.f0, t.f4);
		}
	}

	@Test
	public void testJoinNestedPojoAgainstTupleSelectedUsingString() throws Exception {
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
						.projectFirst()
						.projectSecond();

		List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result = joinDs.collect();

		String expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
				"2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
				"3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testJoinNestedPojoAgainstTupleSelectedUsingInteger() throws Exception {
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
						.projectFirst()
						.projectSecond();

		List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result = joinDs.collect();

		String expected = "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n" +
				"2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n" +
				"3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

		compareResultAsTuples(result, expected);
	}

	@Test
	public void testSelectingMultipleFieldsUsingExpressionLanguage() throws Exception {
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
						.projectFirst()
						.projectSecond();

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
						.projectFirst()
						.projectSecond();

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
						.projectFirst()
						.projectSecond();

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
						.projectFirst()
						.projectSecond();

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
						.projectFirst()
						.projectSecond();

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
						.projectFirst()
						.projectSecond();

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
				.projectFirst()
				.projectSecond();

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
				.projectFirst()
				.projectSecond();

		List<Tuple2<Integer, Tuple3<Integer, Long, String>>> result = joinDs.collect();

		String expected = "1,(1,1,Hi)\n" +
				"2,(2,2,Hello)\n" +
				"null,(3,2,Hello world)\n";

		compareResultAsTuples(result, expected);
	}

	public static class T3T5FlatJoin implements FlatJoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple2<String, String>> {

		@Override
		public void join(Tuple3<Integer, Long, String> first,
				Tuple5<Integer, Long, Integer, String, Long> second,
				Collector<Tuple2<String, String>> out) {

			out.collect(new Tuple2<>(first == null ? null : first.f2, second == null ? null : second.f3));
		}

	}

	public static class T3T5BCJoin extends RichFlatJoinFunction<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>, Tuple3<String, String, Integer>> {

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

	public static class T3CustJoin implements JoinFunction<Tuple3<Integer, Long, String>, CustomType, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(Tuple3<Integer, Long, String> first,
				CustomType second) {

			return new Tuple2<>(first == null ? null : first.f2, second == null ? null : second.myString);
		}
	}

	public static class CustT3Join implements JoinFunction<CustomType, Tuple3<Integer, Long, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> join(CustomType first, Tuple3<Integer, Long, String> second) {

			return new Tuple2<>(first == null ? null : first.myString, second == null ? null : second.f2);
		}
	}
}
