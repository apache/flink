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

import java.util.List;

import org.apache.flink.api.common.functions.GenericReduce;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.ReduceDriver;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.runtime.util.RegularToMutableObjectIterator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("serial")
public class ReduceDriverTest {

	@Test
	public void testReduceDriverImmutableEmpty() {
		try {
			TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>> context =
					new TestTaskContext<GenericReduce<Tuple2<String, Integer>>, Tuple2<String, Integer>>();

			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();

			TupleTypeInfo<Tuple2<String, Integer>> typeInfo =
					(TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(data.get(0));

			MutableObjectIterator<Tuple2<String, Integer>> input = EmptyMutableObjectIterator.get();

			TypeComparator<Tuple2<String, Integer>> comparator =
					typeInfo.createComparator(new int[]{0}, new boolean[]{true});

			GatheringCollector<Tuple2<String, Integer>> result =
					new GatheringCollector<Tuple2<String, Integer>>(typeInfo.createSerializer());

			context.setDriverStrategy(DriverStrategy.SORTED_REDUCE);
			context.setInput1(input, typeInfo.createSerializer());
			context.setComparator1(comparator);
			context.setCollector(result);

			ReduceDriver<Tuple2<String, Integer>> driver = new ReduceDriver<Tuple2<String, Integer>>();
			driver.setup(context);
			driver.prepare();
			driver.run();

			Assert.assertEquals(0, result.getList().size());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testReduceDriverImmutable() {
		try {
			List<Tuple2<String, Integer>> data = DriverTestData.createReduceImmutableData();
			TupleTypeInfo<Tuple2<String, Integer>> typeInfo =
					(TupleTypeInfo<Tuple2<String, Integer>>) TypeExtractor.getForObject(data.get(0));

			ConcatSumFirstReducer udf1 = new ConcatSumFirstReducer();
			ConcatSumSecondReducer udf2 = new ConcatSumSecondReducer();

			Object[] expected = DriverTestData.createReduceImmutableDataGroupedResult().toArray();

			verifyReduceDriver(data, null, typeInfo, udf1, expected);
			verifyReduceDriver(data, null, typeInfo, udf2, expected);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testReduceDriverMutable() {
		try {
			List<Tuple2<StringValue, IntValue>> data = DriverTestData.createReduceMutableData();
			TupleTypeInfo<Tuple2<StringValue, IntValue>> typeInfo =
					(TupleTypeInfo<Tuple2<StringValue, IntValue>>) TypeExtractor.getForObject(data.get(0));

			ConcatSumFirstMutableReducer udf1 = new ConcatSumFirstMutableReducer();
			ConcatSumSecondMutableReducer udf2 = new ConcatSumSecondMutableReducer();

			Object[] expected = DriverTestData.createReduceMutableDataGroupedResult().toArray();

			verifyReduceDriver(data, null, typeInfo, udf1, expected);
			verifyReduceDriver(data, null, typeInfo, udf2, expected);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private <S, T> void verifyReduceDriver(List<Tuple2<S, T>> data, Tuple2<S, T> initialValue,
										   TupleTypeInfo<Tuple2<S, T>> typeInfo, ReduceFunction<Tuple2<S, T>> udf,
										   Object[] expected) throws Exception {

		TestTaskContext<GenericReduce<Tuple2<S, T>>, Tuple2<S, T>> context =
				new TestTaskContext<GenericReduce<Tuple2<S, T>>, Tuple2<S, T>>();

		TypeComparator<Tuple2<S, T>> comparator = typeInfo.createComparator(new int[]{0}, new boolean[]{true});
		TypeSerializer<Tuple2<S, T>> serializer = typeInfo.createSerializer();

		MutableObjectIterator<Tuple2<S, T>> input = new RegularToMutableObjectIterator<Tuple2<S, T>>(data.iterator(), serializer);
		GatheringCollector<Tuple2<S, T>> result = new GatheringCollector<Tuple2<S, T>>(serializer);

		context.setDriverStrategy(DriverStrategy.SORTED_REDUCE);
		context.setInput1(input, typeInfo.createSerializer());
		context.setComparator1(comparator);
		context.setCollector(result);
		context.setUdf(udf);

		if (initialValue != null) {
			context.getTaskConfig().getConfiguration().setBytes(
					ReduceOperatorBase.INITIAL_VALUE_KEY,
					InstantiationUtil.serializeToByteArray(serializer, initialValue));
		}

		ReduceDriver<Tuple2<S, T>> driver = new ReduceDriver<Tuple2<S, T>>();
		driver.setup(context);
		driver.prepare();
		driver.run();

		Object[] res = result.getList().toArray();

		DriverTestData.compareTupleArrays(expected, res);
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
