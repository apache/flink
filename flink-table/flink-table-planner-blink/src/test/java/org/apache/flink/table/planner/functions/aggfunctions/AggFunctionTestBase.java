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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.MaxWithRetractAccumulator;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.MinWithRetractAccumulator;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.BinaryGenericSerializer;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.utils.BinaryGenericAsserter.equivalent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Base class for aggregate function test.
 *
 * @param <T> the type for the aggregation result
 * @param <ACC> accumulate type
 */
public abstract class AggFunctionTestBase<T, ACC> {

	/**
	 * Spec for parameterized aggregate function tests.
	 */
	protected static class AggFunctionTestSpec<T, ACC> {
		final AggregateFunction<T, ACC> aggregator;
		final List<List<T>> inputValueSets;
		final List<T> expectedResults;

		public AggFunctionTestSpec(
				AggregateFunction<T, ACC> aggregator,
				List<List<T>> inputValueSets,
				List<T> expectedResults) {
			this.aggregator = aggregator;
			this.inputValueSets = inputValueSets;
			this.expectedResults = expectedResults;
		}

		@Override
		public String toString() {
			return aggregator.getClass().getSimpleName();
		}
	}

	protected abstract List<List<T>> getInputValueSets();

	protected abstract List<T> getExpectedResults();

	protected abstract AggregateFunction<T, ACC> getAggregator();

	protected abstract Class<?> getAccClass();

	protected Method getAccumulateFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("accumulate", getAccClass(), Object.class);
	}

	protected Method getRetractFunc() throws NoSuchMethodException {
		throw new UnsupportedOperationException("retract is not supported");
	}

	@Test
	// test aggregate and retract functions without partial merge
	public void testAccumulateAndRetractWithoutMerge()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		// iterate over input sets
		List<List<T>> inputValueSets = getInputValueSets();
		List<T> expectedResults = getExpectedResults();
		Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
		AggregateFunction<T, ACC> aggregator = getAggregator();
		int size = getInputValueSets().size();
		// iterate over input sets
		for (int i = 0; i < size; ++i) {
			List<T> inputValues = inputValueSets.get(i);
			T expected = expectedResults.get(i);
			ACC acc = accumulateValues(inputValues);
			T result = aggregator.getValue(acc);
			validateResult(expected, result, aggregator.getAccumulatorType());

			if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
				retractValues(acc, inputValues);
				ACC expectedAcc = aggregator.createAccumulator();
				// The two accumulators should be exactly same
				validateResult(expectedAcc, acc, aggregator.getAccumulatorType());
			}
		}
	}

	@Test
	public void testAggregateWithMerge()
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<T, ACC> aggregator = getAggregator();
		if (UserDefinedFunctionUtils.ifMethodExistInFunction("merge", aggregator)) {
			Method mergeFunc = aggregator.getClass().getMethod("merge", getAccClass(), Iterable.class);
			List<List<T>> inputValueSets = getInputValueSets();
			List<T> expectedResults = getExpectedResults();
			Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
			int size = getInputValueSets().size();
			// iterate over input sets
			for (int i = 0; i < size; ++i) {
				List<T> inputValues = inputValueSets.get(i);
				T expected = expectedResults.get(i);
				// equally split the vals sequence into two sequences
				Tuple2<List<T>, List<T>> splitValues = splitValues(inputValues);
				List<T> firstValues = splitValues.f0;
				List<T> secondValues = splitValues.f1;
				// 1. verify merge with accumulate
				List<ACC> accumulators = new ArrayList<>();
				accumulators.add(accumulateValues(secondValues));

				ACC acc = accumulateValues(firstValues);

				mergeFunc.invoke(aggregator, (Object) acc, accumulators);

				T result = aggregator.getValue(acc);
				validateResult(expected, result, aggregator.getResultType());

				// 2. verify merge with accumulate & retract
				if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
					retractValues(acc, inputValues);
					ACC expectedAcc = aggregator.createAccumulator();
					// The two accumulators should be exactly same
					validateResult(expectedAcc, acc, aggregator.getAccumulatorType());
				}
			}

			// iterate over input sets
			for (int i = 0; i < size; ++i) {
				List<T> inputValues = inputValueSets.get(i);
				T expected = expectedResults.get(i);
				// 3. test partial merge with an empty accumulator
				List<ACC> accumulators = new ArrayList<>();
				accumulators.add(aggregator.createAccumulator());

				ACC acc = accumulateValues(inputValues);
				mergeFunc.invoke(aggregator, (Object) acc, accumulators);

				T result = aggregator.getValue(acc);
				validateResult(expected, result, aggregator.getResultType());
			}
		}
	}

	@Test
	public void testMergeReservedAccumulator()
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<T, ACC> aggregator = getAggregator();
		boolean hasMerge = UserDefinedFunctionUtils.ifMethodExistInFunction("merge", aggregator);
		boolean hasRetract = UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator);
		if (!hasMerge || !hasRetract) {
			// this test only verify AggregateFunctions which has merge() and retract() method
			return;
		}

		Method mergeFunc = aggregator.getClass().getMethod("merge", getAccClass(), Iterable.class);
		List<List<T>> inputValueSets = getInputValueSets();
		int size = getInputValueSets().size();

		// iterate over input sets
		for (int i = 0; i < size; ++i) {
			List<T> inputValues = inputValueSets.get(i);
			List<ACC> accumulators = new ArrayList<>();
			List<ACC> reversedAccumulators = new ArrayList<>();
			// prepare accumulators
			accumulators.add(accumulateValues(inputValues));
			// prepare reversed accumulators
			ACC retractedAcc = aggregator.createAccumulator();
			retractValues(retractedAcc, inputValues);
			reversedAccumulators.add(retractedAcc);
			// prepare accumulator only contain two elements
			ACC accWithSubset = accumulateValues(inputValues.subList(0, 2));
			T expectedValue = aggregator.getValue(accWithSubset);

			// merge
			ACC acc = aggregator.createAccumulator();
			mergeFunc.invoke(aggregator, acc, accumulators);
			mergeFunc.invoke(aggregator, acc, reversedAccumulators);
			mergeFunc.invoke(aggregator, accWithSubset, Collections.singleton(acc));

			// getValue
			T result = aggregator.getValue(accWithSubset);
			validateResult(expectedValue, result, aggregator.getResultType());
		}
	}

	@Test
	public void testResetAccumulator() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<T, ACC> aggregator = getAggregator();
		if (UserDefinedFunctionUtils.ifMethodExistInFunction("resetAccumulator", aggregator)) {
			Method resetAccFunc = aggregator.getClass().getMethod("resetAccumulator", getAccClass());

			List<List<T>> inputValueSets = getInputValueSets();
			List<T> expectedResults = getExpectedResults();
			Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
			int size = getInputValueSets().size();
			// iterate over input sets
			for (int i = 0; i < size; ++i) {
				List<T> inputValues = inputValueSets.get(i);
				T expected = expectedResults.get(i);
				ACC acc = accumulateValues(inputValues);
				resetAccFunc.invoke(aggregator, (Object) acc);
				ACC expectedAcc = aggregator.createAccumulator();
				//The accumulator after reset should be exactly same as the new accumulator
				validateResult(expectedAcc, acc, aggregator.getAccumulatorType());
			}
		}
	}

	protected <E> void validateResult(E expected, E result, TypeInformation<?> typeInfo) {
		if (expected instanceof BigDecimal && result instanceof BigDecimal) {
			// BigDecimal.equals() value and scale but we are only interested in value.
			assertEquals(0, ((BigDecimal) expected).compareTo((BigDecimal) result));
		} else if (expected instanceof MinWithRetractAccumulator &&
				result instanceof MinWithRetractAccumulator) {
			MinWithRetractAccumulator e = (MinWithRetractAccumulator) expected;
			MinWithRetractAccumulator r = (MinWithRetractAccumulator) result;
			assertEquals(e.min, r.min);
			assertEquals(e.mapSize, r.mapSize);
		} else if (expected instanceof MaxWithRetractAccumulator &&
				result instanceof MaxWithRetractAccumulator) {
			MaxWithRetractAccumulator e = (MaxWithRetractAccumulator) expected;
			MaxWithRetractAccumulator r = (MaxWithRetractAccumulator) result;
			assertEquals(e.max, r.max);
			assertEquals(e.mapSize, r.mapSize);
		} else if (expected instanceof BinaryGeneric && result instanceof BinaryGeneric) {
			TypeSerializer<?> serializer = typeInfo.createSerializer(new ExecutionConfig());
			assertThat(
					(BinaryGeneric) result,
					equivalent((BinaryGeneric) expected, new BinaryGenericSerializer(serializer)));
		} else if (expected instanceof GenericRow && result instanceof GenericRow) {
			validateGenericRow((GenericRow) expected, (GenericRow) result, (BaseRowTypeInfo) typeInfo);
		} else {
			assertEquals(expected, result);
		}
	}

	private void validateGenericRow(GenericRow expected, GenericRow result, BaseRowTypeInfo typeInfo) {
		assertEquals(expected.getArity(), result.getArity());

		for (int i = 0; i < expected.getArity(); ++i) {
			Object expectedObj = expected.getField(i);
			Object resultObj = result.getField(i);
			validateResult(expectedObj, resultObj, typeInfo.getTypeAt(i));
		}
	}

	protected ACC accumulateValues(List<T> values)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<T, ACC> aggregator = getAggregator();
		ACC accumulator = getAggregator().createAccumulator();
		Method accumulateFunc = getAccumulateFunc();
		for (T value : values) {
			if (accumulateFunc.getParameterCount() == 1) {
				accumulateFunc.invoke(aggregator, (Object) accumulator);
			} else if (accumulateFunc.getParameterCount() == 2) {
				accumulateFunc.invoke(aggregator, (Object) accumulator, (Object) value);
			} else {
				throw new TableException("Unsupported now");
			}
		}
		return accumulator;
	}

	protected void retractValues(ACC accumulator, List<T> values)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<T, ACC> aggregator = getAggregator();
		Method retractFunc = getRetractFunc();
		for (T value : values) {
			if (retractFunc.getParameterCount() == 1) {
				retractFunc.invoke(aggregator, (Object) accumulator);
			} else if (retractFunc.getParameterCount() == 2) {
				retractFunc.invoke(aggregator, (Object) accumulator, (Object) value);
			} else {
				throw new TableException("Unsupported now");
			}
		}
	}

	protected Tuple2<List<T>, List<T>> splitValues(List<T> values) {
		return splitValues(values, values.size() / 2);
	}

	protected Tuple2<List<T>, List<T>> splitValues(List<T> values, int index) {
		List<T> firstValues = new ArrayList<>();
		List<T> secondValues = new ArrayList<>();
		int i;
		for (i = 0; i < values.size(); ++i) {
			if (i < index) {
				firstValues.add(values.get(i));
			} else {
				break;
			}
		}
		if (i < values.size()) {
			secondValues.addAll(values.subList(i, values.size()));
		}
		return new Tuple2<>(firstValues, secondValues);
	}

}
