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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Base test case for built-in FirstValue and LastValue (with retreat) aggregate function.
 * This class tests `accumulate` method with order argument.
 */
public abstract class FirstLastValueAggFunctionWithOrderTestBase<T> extends AggFunctionTestBase<T, GenericRowData> {

	protected Method getAccumulateFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("accumulate", getAccClass(), Object.class, Long.class);
	}

	@Override
	protected Class<?> getAccClass() {
		return GenericRowData.class;
	}

	protected abstract List<List<Long>> getInputOrderSets();

	@Test
	@Override
	public void testAccumulateAndRetractWithoutMerge()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		// iterate over input sets
		List<List<T>> inputValueSets = getInputValueSets();
		List<List<Long>> inputOrderSets = getInputOrderSets();
		List<T> expectedResults = getExpectedResults();
		Preconditions.checkArgument(inputValueSets.size() == inputOrderSets.size(),
				"The number of inputValueSets is not same with the number of inputOrderSets");
		Preconditions.checkArgument(inputValueSets.size() == expectedResults.size(),
				"The number of inputValueSets is not same with the number of expectedResults");
		AggregateFunction<T, GenericRowData> aggregator = getAggregator();
		int size = getInputValueSets().size();
		// iterate over input sets
		for (int i = 0; i < size; ++i) {
			List<T> inputValues = inputValueSets.get(i);
			List<Long> inputOrders = inputOrderSets.get(i);
			T expected = expectedResults.get(i);
			GenericRowData acc = accumulateValues(inputValues, inputOrders);
			T result = aggregator.getValue(acc);
			validateResult(expected, result, aggregator.getResultType());

			if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
				retractValues(acc, inputValues, inputOrders);
				GenericRowData expectedAcc = aggregator.createAccumulator();
				// The two accumulators should be exactly same
				validateResult(expectedAcc, acc, aggregator.getAccumulatorType());
			}
		}
	}

	@Test
	@Override
	public void testResetAccumulator() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<T, GenericRowData> aggregator = getAggregator();
		if (UserDefinedFunctionUtils.ifMethodExistInFunction("resetAccumulator", aggregator)) {
			Method resetAccFunc = aggregator.getClass().getMethod("resetAccumulator", getAccClass());

			List<List<T>> inputValueSets = getInputValueSets();
			List<List<Long>> inputOrderSets = getInputOrderSets();
			List<T> expectedResults = getExpectedResults();
			Preconditions.checkArgument(inputValueSets.size() == inputOrderSets.size(),
					"The number of inputValueSets is not same with the number of inputOrderSets");
			Preconditions.checkArgument(inputValueSets.size() == expectedResults.size(),
					"The number of inputValueSets is not same with the number of expectedResults");
			int size = getInputValueSets().size();
			// iterate over input sets
			for (int i = 0; i < size; ++i) {
				List<T> inputValues = inputValueSets.get(i);
				List<Long> inputOrders = inputOrderSets.get(i);
				T expected = expectedResults.get(i);
				GenericRowData acc = accumulateValues(inputValues, inputOrders);
				resetAccFunc.invoke(aggregator, (Object) acc);
				GenericRowData expectedAcc = aggregator.createAccumulator();
				//The accumulator after reset should be exactly same as the new accumulator
				validateResult(expectedAcc, acc, aggregator.getAccumulatorType());
			}
		}
	}

	protected GenericRowData accumulateValues(List<T> values, List<Long> orders)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Preconditions.checkArgument(values.size() == orders.size(),
				"The number of values is not same with the number of orders, " +
						"\nvalues: " + values + "\norders: " + orders);
		AggregateFunction<T, GenericRowData> aggregator = getAggregator();
		GenericRowData accumulator = getAggregator().createAccumulator();
		Method accumulateFunc = getAccumulateFunc();
		for (int i = 0; i < values.size(); ++i) {
			accumulateFunc.invoke(aggregator, (Object) accumulator, (Object) values.get(i), orders.get(i));
		}
		return accumulator;
	}

	@Override
	protected GenericRowData accumulateValues(List<T> values) {
		throw new TableException("Should not call this method");
	}

	protected void retractValues(GenericRowData accumulator, List<T> values, List<Long> orders)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Preconditions.checkArgument(values.size() == orders.size(),
				"The number of values is not same with the number of orders, " +
						"\nvalues: " + values + "\norders: " + orders);
		AggregateFunction<T, GenericRowData> aggregator = getAggregator();
		Method retractFunc = getRetractFunc();
		for (int i = 0; i < values.size(); ++i) {
			retractFunc.invoke(aggregator, (Object) accumulator, (Object) values.get(i), orders.get(i));
		}
	}

	@Override
	protected void retractValues(GenericRowData accumulator, List<T> values) {
		throw new TableException("Should not call this method");
	}

}
