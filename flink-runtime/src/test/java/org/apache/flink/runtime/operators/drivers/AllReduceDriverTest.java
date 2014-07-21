/**
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


package org.apache.flink.runtime.operators.drivers;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.GenericReduce;
import org.apache.flink.api.java.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.AllReduceDriver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.runtime.util.RegularToMutableObjectIterator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.TypeInformation;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("serial")
public class AllReduceDriverTest {

	@Test
	public void testAllReduceDriverImmutableEmpty() {
		try {
			TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>>();

			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(data.get(0));
			MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();
			GatheringCollector<Tuple2<String, Integer>> result = new GatheringCollector<Tuple2<String, Integer>>(typeInfo.createSerializer());

			context.setDriverStrategy(DriverStrategy.ALL_REDUCE);
			context.setInput1(input, typeInfo.createSerializer());
			context.setCollector(result);

			AllReduceDriver<Tuple2<String, Integer>> driver = new AllReduceDriver<Tuple2<String, Integer>>();
			driver.setup(context);
			driver.prepare();
			driver.run();

			Assert.assertEquals("Expected no collect() for empty input", 0, result.getList().size());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testAllReduceDriverImmutable() {
		try {
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(data.get(0));

			ConcatSumFirstReducer udf1 = new ConcatSumFirstReducer();
			ConcatSumSecondReducer udf2 = new ConcatSumSecondReducer();

			String expectedString = "abcddeeeffff";
			int expectedValue = 78;

			verifyAllReduceDriver(data, null, typeInfo, udf1, expectedString, expectedValue);
			verifyAllReduceDriver(data, null, typeInfo, udf2, expectedString, expectedValue);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testAllReduceDriverMutable() {
		try {
			List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
			TypeInformation<Tuple2<StringValue, IntValue>> typeInfo = TypeExtractor.getForObject(data.get(0));

			ConcatSumFirstMutableReducer udf1 = new ConcatSumFirstMutableReducer();
			ConcatSumSecondMutableReducer udf2 = new ConcatSumSecondMutableReducer();

			String expectedString = "abcddeeeffff";
			int expectedValue = 78;

			verifyAllReduceDriver(data, null, typeInfo, udf1, expectedString, expectedValue);
			verifyAllReduceDriver(data, null, typeInfo, udf2, expectedString, expectedValue);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testAllReduceDriverImmutableWithInitialValue() {
		try {
			{
				// initial value only
				List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();

				TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(data.get(0));

				ConcatSumFirstReducer firstUdf = new ConcatSumFirstReducer();
				ConcatSumSecondReducer secondUdf = new ConcatSumSecondReducer();

				String expectedString = "a";
				int expectedValue = 1;

				verifyAllReduceDriver(Collections.EMPTY_LIST, data.get(0), typeInfo, firstUdf, expectedString, expectedValue);
				verifyAllReduceDriver(Collections.EMPTY_LIST, data.get(0), typeInfo, secondUdf, expectedString, expectedValue);
			}

			{
				// initial value and single input
				List<Tuple2<String, Integer>> firstData = DriverTestData.createReduceImmutableData();
				List<Tuple2<String, Integer>> secondData = DriverTestData.createReduceImmutableData();

				TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(firstData.get(0));

				ConcatSumFirstReducer firstUdf = new ConcatSumFirstReducer();
				ConcatSumSecondReducer secondUdf = new ConcatSumSecondReducer();

				String expectedString = "aa";
				int expectedValue = 2;

				verifyAllReduceDriver(firstData.subList(0, 1), firstData.get(0), typeInfo, firstUdf, expectedString, expectedValue);
				verifyAllReduceDriver(secondData.subList(0, 1), secondData.get(0), typeInfo, secondUdf, expectedString, expectedValue);
			}

			{
				// initial value and two inputs
				List<Tuple2<String, Integer>> firstData = DriverTestData.createReduceImmutableData();
				List<Tuple2<String, Integer>> secondData = DriverTestData.createReduceImmutableData();

				TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(firstData.get(0));

				ConcatSumFirstReducer firstUdf = new ConcatSumFirstReducer();
				ConcatSumSecondReducer secondUdf = new ConcatSumSecondReducer();

				String expectedString = "aab";
				int expectedValue = 4;

				verifyAllReduceDriver(firstData.subList(0, 2), firstData.get(0), typeInfo, firstUdf, expectedString, expectedValue);
				verifyAllReduceDriver(secondData.subList(0, 2), secondData.get(0), typeInfo, secondUdf, expectedString, expectedValue);
			}

			{
				// initial value and all
				List<Tuple2<String, Integer>> firstData = DriverTestData.createReduceImmutableData();
				List<Tuple2<String, Integer>> secondData = DriverTestData.createReduceImmutableData();

				TypeInformation<Tuple2<String, Integer>> typeInfo = TypeExtractor.getForObject(firstData.get(0));

				ConcatSumFirstReducer firstUdf = new ConcatSumFirstReducer();
				ConcatSumSecondReducer secondUdf = new ConcatSumSecondReducer();

				String expectedString = "aabcddeeeffff";
				int expectedValue = 79;

				verifyAllReduceDriver(firstData, firstData.get(0), typeInfo, firstUdf, expectedString, expectedValue);
				verifyAllReduceDriver(secondData, secondData.get(0), typeInfo, secondUdf, expectedString, expectedValue);
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testAllReduceDriverMutableWithInitialValue() {
		try {
			{
				// initial value only
				List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();

				TypeInformation<Tuple2<StringValue, IntValue>> typeInfo = TypeExtractor.getForObject(data.get(0));

				ConcatSumFirstMutableReducer firstUdf = new ConcatSumFirstMutableReducer();
				ConcatSumSecondMutableReducer secondUdf = new ConcatSumSecondMutableReducer();

				String expectedString = "a";
				int expectedValue = 1;

				verifyAllReduceDriver(Collections.EMPTY_LIST, data.get(0), typeInfo, firstUdf, expectedString, expectedValue);
				verifyAllReduceDriver(Collections.EMPTY_LIST, data.get(0), typeInfo, secondUdf, expectedString, expectedValue);
			}

			{
				// initial value and single data point
				List<Tuple2<StringValue, IntValue>> firstData = DriverTestData.createReduceMutableData();
				List<Tuple2<StringValue, IntValue>> secondData = DriverTestData.createReduceMutableData();

				TypeInformation<Tuple2<StringValue, IntValue>> typeInfo = TypeExtractor.getForObject(firstData.get(0));

				ConcatSumFirstMutableReducer firstUdf = new ConcatSumFirstMutableReducer();
				ConcatSumSecondMutableReducer secondUdf = new ConcatSumSecondMutableReducer();

				String expectedString = "aa";
				int expectedValue = 2;

				verifyAllReduceDriver(firstData.subList(0, 1), firstData.get(0), typeInfo, firstUdf, expectedString, expectedValue);
				verifyAllReduceDriver(secondData.subList(0, 1), secondData.get(0), typeInfo, secondUdf, expectedString, expectedValue);
			}

			{
				// initial value and data points
				List<Tuple2<StringValue, IntValue>> firstData = DriverTestData.createReduceMutableData();
				List<Tuple2<StringValue, IntValue>> secondData = DriverTestData.createReduceMutableData();

				TypeInformation<Tuple2<StringValue, IntValue>> typeInfo = TypeExtractor.getForObject(firstData.get(0));

				ConcatSumFirstMutableReducer firstUdf = new ConcatSumFirstMutableReducer();
				ConcatSumSecondMutableReducer secondUdf = new ConcatSumSecondMutableReducer();

				String expectedString = "aab";
				int expectedValue = 4;

				verifyAllReduceDriver(firstData.subList(0, 2), firstData.get(0), typeInfo, secondUdf, expectedString, expectedValue);
				verifyAllReduceDriver(secondData.subList(0, 2), secondData.get(0), typeInfo, secondUdf, expectedString, expectedValue);
			}

			{
				// initial value and all
				List<Tuple2<StringValue, IntValue>> firstData = DriverTestData.createReduceMutableData();
				List<Tuple2<StringValue, IntValue>> secondData = DriverTestData.createReduceMutableData();

				TypeInformation<Tuple2<StringValue, IntValue>> typeInfo = TypeExtractor.getForObject(firstData.get(0));

				ConcatSumFirstMutableReducer firstUdf = new ConcatSumFirstMutableReducer();
				ConcatSumSecondMutableReducer secondUdf = new ConcatSumSecondMutableReducer();

				String expectedString = "aabcddeeeffff";
				int expectedValue = 79;

				verifyAllReduceDriver(firstData.subList(0, firstData.size()), firstData.get(0), typeInfo, firstUdf, expectedString, expectedValue);
				verifyAllReduceDriver(secondData.subList(0, secondData.size()), secondData.get(0), typeInfo, secondUdf, expectedString, expectedValue);
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private <S, T> void verifyAllReduceDriver(List<Tuple2<S, T>> data, Tuple2<S, T> initialValue,
											TypeInformation<Tuple2<S, T>> typeInfo, ReduceFunction<Tuple2<S, T>> udf,
											String expectedString, int expectedInt) throws Exception {

		TestTaskContext<GenericReduce<Tuple2<S, T>>, Tuple2<S, T>> context =
				new TestTaskContext<GenericReduce<Tuple2<S, T>>, Tuple2<S, T>>();

		TypeSerializer<Tuple2<S, T>> serializer = typeInfo.createSerializer();

		MutableObjectIterator<Tuple2<S, T>> input = new RegularToMutableObjectIterator<Tuple2<S, T>>(data.iterator(), serializer);

		GatheringCollector<Tuple2<S, T>> result = new GatheringCollector<Tuple2<S, T>>(serializer);

		context.setDriverStrategy(DriverStrategy.ALL_REDUCE);
		context.setInput1(input, typeInfo.createSerializer());
		context.setCollector(result);
		context.setUdf(udf);

		if (initialValue != null) {
			context.getTaskConfig().getStubParameters().setBytes(
					ReduceOperatorBase.INITIAL_VALUE_KEY,
					InstantiationUtil.serializeToByteArray(serializer, initialValue));
		}

		AllReduceDriver<Tuple2<S, T>> driver = new AllReduceDriver<Tuple2<S, T>>();
		driver.setup(context);
		driver.prepare();
		driver.run();

		Assert.assertEquals("Expected single collected result.", 1, result.getList().size());

		Tuple2<S, T> res = result.getList().get(0);

		char[] actualStringCharArray;
		int actualInt;

		if (res.f0.getClass() == StringValue.class) {
			actualStringCharArray = ((StringValue) res.f0).getCharArray();
			actualInt = ((IntValue) res.f1).getValue();
		}
		else {
			actualStringCharArray = ((String) res.f0).toCharArray();
			actualInt = (Integer) res.f1;
		}

		Arrays.sort(actualStringCharArray);

		char[] expectedStringCharArray = expectedString.toCharArray();
		Arrays.sort(expectedStringCharArray);

		Assert.assertArrayEquals(expectedStringCharArray, actualStringCharArray);
		Assert.assertEquals(expectedInt, actualInt);
	}

	// --------------------------------------------------------------------------------------------
	//  Test UDFs
	// --------------------------------------------------------------------------------------------

	public static final class ConcatSumFirstReducer extends ReduceFunction<Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
			value1.f0 = value1.f0 + value2.f0;
			value1.f1 = value1.f1 + value2.f1;
			return value1;
		}
	}

	public static final class ConcatSumSecondReducer extends ReduceFunction<Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
			value2.f0 = value1.f0 + value2.f0;
			value2.f1 = value1.f1 + value2.f1;
			return value2;
		}
	}

	public static final class ConcatSumFirstMutableReducer extends ReduceFunction<Tuple2<StringValue, IntValue>> {

		@Override
		public Tuple2<StringValue, IntValue> reduce(Tuple2<StringValue, IntValue> value1, Tuple2<StringValue, IntValue> value2) {
			value1.f0.setValue(value1.f0.getValue() + value2.f0.getValue());
			value1.f1.setValue(value1.f1.getValue() + value2.f1.getValue());
			return value1;
		}
	}

	public static final class ConcatSumSecondMutableReducer extends ReduceFunction<Tuple2<StringValue, IntValue>> {

		@Override
		public Tuple2<StringValue, IntValue> reduce(Tuple2<StringValue, IntValue> value1, Tuple2<StringValue, IntValue> value2) {
			value2.f0.setValue(value1.f0.getValue() + value2.f0.getValue());
			value2.f1.setValue(value1.f1.getValue() + value2.f1.getValue());
			return value2;
		}
	}
}
