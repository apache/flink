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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for aggregate function test.
 *
 * @param <IN> the type for the aggregation input
 * @param <T> the type for the aggregation result
 * @param <ACC> accumulate type
 */
abstract class AggFunctionTestBase<IN, T, ACC> {

    protected abstract List<List<IN>> getInputValueSets();

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
    void testAccumulateAndRetractWithoutMerge()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        // iterate over input sets
        List<List<IN>> inputValueSets = getInputValueSets();
        List<T> expectedResults = getExpectedResults();
        Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
        AggregateFunction<T, ACC> aggregator = getAggregator();
        int size = getInputValueSets().size();
        // iterate over input sets
        for (int i = 0; i < size; ++i) {
            List<IN> inputValues = inputValueSets.get(i);
            T expected = expectedResults.get(i);
            ACC acc = aggregator.createAccumulator();
            accumulateValues(aggregator, acc, inputValues);
            T result = aggregator.getValue(acc);
            validateResult(expected, result);

            if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
                retractValues(acc, inputValues);
                ACC expectedAcc = aggregator.createAccumulator();
                // The two accumulators should be exactly same
                validateResult(expectedAcc, acc);
            }
        }
    }

    @Test
    void testAggregateWithMerge()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        AggregateFunction<T, ACC> aggregator = getAggregator();
        if (UserDefinedFunctionUtils.ifMethodExistInFunction("merge", aggregator)) {
            Method mergeFunc =
                    aggregator.getClass().getMethod("merge", getAccClass(), Iterable.class);
            List<List<IN>> inputValueSets = getInputValueSets();
            List<T> expectedResults = getExpectedResults();
            Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
            int size = getInputValueSets().size();
            // iterate over input sets
            for (int i = 0; i < size; ++i) {
                List<IN> inputValues = inputValueSets.get(i);
                T expected = expectedResults.get(i);
                // equally split the vals sequence into two sequences
                Tuple2<List<IN>, List<IN>> splitValues = splitValues(inputValues);
                List<IN> firstValues = splitValues.f0;
                List<IN> secondValues = splitValues.f1;
                // 1. verify merge with accumulate
                List<ACC> accumulators = new ArrayList<>();
                ACC secondAcc = aggregator.createAccumulator();
                accumulateValues(aggregator, secondAcc, secondValues);
                accumulators.add(secondAcc);

                ACC firstAcc = aggregator.createAccumulator();
                accumulateValues(aggregator, firstAcc, firstValues);

                mergeFunc.invoke(aggregator, firstAcc, accumulators);

                T result = aggregator.getValue(firstAcc);
                validateResult(expected, result);

                // 2. verify merge with accumulate & retract
                if (UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator)) {
                    retractValues(firstAcc, inputValues);
                    ACC expectedAcc = aggregator.createAccumulator();
                    // The two accumulators should be exactly same
                    validateResult(expectedAcc, firstAcc);
                }
            }

            // iterate over input sets
            for (int i = 0; i < size; ++i) {
                List<IN> inputValues = inputValueSets.get(i);
                T expected = expectedResults.get(i);
                // 3. test partial merge with an empty accumulator
                List<ACC> accumulators = new ArrayList<>();
                accumulators.add(aggregator.createAccumulator());

                ACC acc = aggregator.createAccumulator();
                accumulateValues(aggregator, acc, inputValues);
                mergeFunc.invoke(aggregator, acc, accumulators);

                T result = aggregator.getValue(acc);
                validateResult(expected, result);
            }
        }
    }

    @Test
    void testMergeReservedAccumulator()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        AggregateFunction<T, ACC> aggregator = getAggregator();
        boolean hasMerge = UserDefinedFunctionUtils.ifMethodExistInFunction("merge", aggregator);
        boolean hasRetract =
                UserDefinedFunctionUtils.ifMethodExistInFunction("retract", aggregator);
        if (!hasMerge || !hasRetract) {
            // this test only verify AggregateFunctions which has merge() and retract() method
            return;
        }

        Method mergeFunc = aggregator.getClass().getMethod("merge", getAccClass(), Iterable.class);
        List<List<IN>> inputValueSets = getInputValueSets();
        int size = getInputValueSets().size();

        // iterate over input sets
        for (int i = 0; i < size; ++i) {
            List<IN> inputValues = inputValueSets.get(i);
            List<ACC> accumulators = new ArrayList<>();
            List<ACC> reversedAccumulators = new ArrayList<>();
            // prepare accumulators
            ACC firstAcc = aggregator.createAccumulator();
            accumulateValues(aggregator, firstAcc, inputValues);
            accumulators.add(firstAcc);
            // prepare reversed accumulators
            ACC retractedAcc = aggregator.createAccumulator();
            retractValues(retractedAcc, inputValues);
            reversedAccumulators.add(retractedAcc);
            // prepare accumulator only contain two elements
            ACC accWithSubset = aggregator.createAccumulator();
            accumulateValues(aggregator, accWithSubset, inputValues.subList(0, 2));
            T expectedValue = aggregator.getValue(accWithSubset);

            // merge
            ACC secondAcc = aggregator.createAccumulator();
            mergeFunc.invoke(aggregator, secondAcc, accumulators);
            mergeFunc.invoke(aggregator, secondAcc, reversedAccumulators);
            mergeFunc.invoke(aggregator, accWithSubset, Collections.singleton(secondAcc));

            // getValue
            T result = aggregator.getValue(accWithSubset);
            validateResult(expectedValue, result);
        }
    }

    @Test
    void testResetAccumulator()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        AggregateFunction<T, ACC> aggregator = getAggregator();
        if (UserDefinedFunctionUtils.ifMethodExistInFunction("resetAccumulator", aggregator)) {
            Method resetAccFunc =
                    aggregator.getClass().getMethod("resetAccumulator", getAccClass());

            List<List<IN>> inputValueSets = getInputValueSets();
            List<T> expectedResults = getExpectedResults();
            Preconditions.checkArgument(inputValueSets.size() == expectedResults.size());
            int size = getInputValueSets().size();
            // iterate over input sets
            for (int i = 0; i < size; ++i) {
                List<IN> inputValues = inputValueSets.get(i);
                ACC acc = aggregator.createAccumulator();
                accumulateValues(aggregator, acc, inputValues);
                resetAccFunc.invoke(aggregator, acc);
                ACC expectedAcc = aggregator.createAccumulator();
                // The accumulator after reset should be exactly same as the new accumulator
                validateResult(expectedAcc, acc);
            }
        }
    }

    protected <E> void validateResult(E expected, E result) {
        assertThat(result).isEqualTo(expected);
    }

    protected void accumulateValues(
            AggregateFunction<T, ACC> aggregator, ACC accumulator, List<IN> values)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method accumulateFunc = getAccumulateFunc();
        for (IN value : values) {
            if (accumulateFunc.getParameterCount() == 1) {
                accumulateFunc.invoke(aggregator, accumulator);
            } else if (accumulateFunc.getParameterCount() == 2) {
                accumulateFunc.invoke(aggregator, accumulator, value);
            } else {
                throw new TableException("Unsupported now");
            }
        }
    }

    protected void retractValues(ACC accumulator, List<IN> values)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        AggregateFunction<T, ACC> aggregator = getAggregator();
        Method retractFunc = getRetractFunc();
        for (IN value : values) {
            if (retractFunc.getParameterCount() == 1) {
                retractFunc.invoke(aggregator, accumulator);
            } else if (retractFunc.getParameterCount() == 2) {
                retractFunc.invoke(aggregator, accumulator, value);
            } else {
                throw new TableException("Unsupported now");
            }
        }
    }

    protected Tuple2<List<IN>, List<IN>> splitValues(List<IN> values) {
        return splitValues(values, values.size() / 2);
    }

    protected Tuple2<List<IN>, List<IN>> splitValues(List<IN> values, int index) {
        List<IN> firstValues = new ArrayList<>();
        List<IN> secondValues = new ArrayList<>();
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
