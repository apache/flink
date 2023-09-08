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
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Base test case for built-in FIRST_VALUE and LAST_VALUE (with retract) aggregate function. This
 * class tests `accumulate` method with order argument.
 */
abstract class FirstLastValueAggFunctionWithOrderTestBase<T, ACC>
        extends AggFunctionTestBase<T, T, ACC> {

    protected Method getAccumulateFunc() throws NoSuchMethodException {
        return getAggregator()
                .getClass()
                .getMethod("accumulate", getAccClass(), Object.class, Long.class);
    }

    protected abstract List<List<Long>> getInputOrderSets();

    @Test
    @Override
    void testAccumulateAndRetractWithoutMerge()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        // iterate over input sets
        List<List<T>> inputValueSets = getInputValueSets();
        List<List<Long>> inputOrderSets = getInputOrderSets();
        List<T> expectedResults = getExpectedResults();
        Preconditions.checkArgument(
                inputValueSets.size() == inputOrderSets.size(),
                "The number of inputValueSets is not same with the number of inputOrderSets");
        Preconditions.checkArgument(
                inputValueSets.size() == expectedResults.size(),
                "The number of inputValueSets is not same with the number of expectedResults");
        AggregateFunction<T, ACC> aggregator = getAggregator();
        int size = getInputValueSets().size();
        // iterate over input sets
        for (int i = 0; i < size; ++i) {
            List<T> inputValues = inputValueSets.get(i);
            List<Long> inputOrders = inputOrderSets.get(i);
            T expected = expectedResults.get(i);
            ACC acc = accumulateValues(inputValues, inputOrders);
            T result = aggregator.getValue(acc);
            validateResult(expected, result);

            if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
                retractValues(acc, inputValues, inputOrders);
                ACC expectedAcc = aggregator.createAccumulator();
                // The two accumulators should be exactly same
                validateResult(expectedAcc, acc);
            }
        }
    }

    @Test
    @Override
    void testResetAccumulator()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        AggregateFunction<T, ACC> aggregator = getAggregator();
        if (UserDefinedFunctionUtils.ifMethodExistInFunction("resetAccumulator", aggregator)) {
            Method resetAccFunc =
                    aggregator.getClass().getMethod("resetAccumulator", getAccClass());

            List<List<T>> inputValueSets = getInputValueSets();
            List<List<Long>> inputOrderSets = getInputOrderSets();
            List<T> expectedResults = getExpectedResults();
            Preconditions.checkArgument(
                    inputValueSets.size() == inputOrderSets.size(),
                    "The number of inputValueSets is not same with the number of inputOrderSets");
            Preconditions.checkArgument(
                    inputValueSets.size() == expectedResults.size(),
                    "The number of inputValueSets is not same with the number of expectedResults");
            int size = getInputValueSets().size();
            // iterate over input sets
            for (int i = 0; i < size; ++i) {
                List<T> inputValues = inputValueSets.get(i);
                List<Long> inputOrders = inputOrderSets.get(i);
                ACC acc = accumulateValues(inputValues, inputOrders);
                resetAccFunc.invoke(aggregator, acc);
                ACC expectedAcc = aggregator.createAccumulator();
                // The accumulator after reset should be exactly same as the new accumulator
                validateResult(expectedAcc, acc);
            }
        }
    }

    protected ACC accumulateValues(List<T> values, List<Long> orders)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Preconditions.checkArgument(
                values.size() == orders.size(),
                "The number of values is not same with the number of orders, "
                        + "\nvalues: "
                        + values
                        + "\norders: "
                        + orders);
        AggregateFunction<T, ACC> aggregator = getAggregator();
        ACC accumulator = getAggregator().createAccumulator();
        Method accumulateFunc = getAccumulateFunc();
        for (int i = 0; i < values.size(); ++i) {
            accumulateFunc.invoke(aggregator, accumulator, values.get(i), orders.get(i));
        }
        return accumulator;
    }

    @Override
    protected void accumulateValues(
            AggregateFunction<T, ACC> aggregator, ACC accumulator, List<T> values) {
        throw new TableException("Should not call this method");
    }

    protected void retractValues(ACC accumulator, List<T> values, List<Long> orders)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Preconditions.checkArgument(
                values.size() == orders.size(),
                "The number of values is not same with the number of orders, "
                        + "\nvalues: "
                        + values
                        + "\norders: "
                        + orders);
        AggregateFunction<T, ACC> aggregator = getAggregator();
        Method retractFunc = getRetractFunc();
        for (int i = 0; i < values.size(); ++i) {
            retractFunc.invoke(aggregator, accumulator, values.get(i), orders.get(i));
        }
    }

    @Override
    protected void retractValues(ACC accumulator, List<T> values) {
        throw new TableException("Should not call this method");
    }
}
