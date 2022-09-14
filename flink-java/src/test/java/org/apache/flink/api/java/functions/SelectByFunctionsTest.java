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

package org.apache.flink.api.java.functions;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link SelectByMaxFunction} and {@link SelectByMinFunction}. */
class SelectByFunctionsTest {

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private final Tuple5<Integer, Long, String, Long, Integer> bigger =
            new Tuple5<>(10, 100L, "HelloWorld", 200L, 20);
    private final Tuple5<Integer, Long, String, Long, Integer> smaller =
            new Tuple5<>(5, 50L, "Hello", 50L, 15);

    // Special case where only the last value determines if bigger or smaller
    private final Tuple5<Integer, Long, String, Long, Integer> specialCaseBigger =
            new Tuple5<>(10, 100L, "HelloWorld", 200L, 17);
    private final Tuple5<Integer, Long, String, Long, Integer> specialCaseSmaller =
            new Tuple5<>(5, 50L, "Hello", 50L, 17);

    /**
     * This test validates whether the order of tuples has any impact on the outcome and if the
     * bigger tuple is returned.
     */
    @Test
    void testMaxByComparison() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<>(tupleTypeInfo, 0);

        try {
            assertThat(maxByTuple.reduce(smaller, bigger))
                    .as("SelectByMax must return bigger tuple")
                    .isSameAs(bigger);
            assertThat(maxByTuple.reduce(bigger, smaller))
                    .as("SelectByMax must return bigger tuple")
                    .isSameAs(bigger);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }

    // ----------------------- MAXIMUM FUNCTION TEST BELOW --------------------------

    /**
     * This test cases checks when two tuples only differ in one value, but this value is not in the
     * fields list. In that case it should be seen as equal and then the first given tuple (value1)
     * should be returned by reduce().
     */
    @Test
    void testMaxByComparisonSpecialCase1() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<>(tupleTypeInfo, 0, 3);

        try {
            assertThat(maxByTuple.reduce(specialCaseBigger, bigger))
                    .as("SelectByMax must return the first given tuple")
                    .isSameAs(specialCaseBigger);
            assertThat(maxByTuple.reduce(bigger, specialCaseBigger))
                    .as("SelectByMax must return the first given tuple")
                    .isSameAs(bigger);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }

    /** This test cases checks when two tuples only differ in one value. */
    @Test
    void testMaxByComparisonSpecialCase2() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<>(tupleTypeInfo, 0, 2, 1, 4, 3);

        try {
            assertThat(maxByTuple.reduce(specialCaseBigger, bigger))
                    .as("SelectByMax must return bigger tuple")
                    .isSameAs(bigger);
            assertThat(maxByTuple.reduce(bigger, specialCaseBigger))
                    .as("SelectByMax must return bigger tuple")
                    .isSameAs(bigger);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }

    /** This test validates that equality is independent of the amount of used indices. */
    @Test
    void testMaxByComparisonMultiple() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<>(tupleTypeInfo, 0, 1, 2, 3, 4);

        try {
            assertThat(maxByTuple.reduce(smaller, bigger))
                    .as("SelectByMax must return bigger tuple")
                    .isSameAs(bigger);
            assertThat(maxByTuple.reduce(bigger, smaller))
                    .as("SelectByMax must return bigger tuple")
                    .isSameAs(bigger);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }

    /** Checks whether reduce does behave as expected if both values are the same object. */
    @Test
    void testMaxByComparisonMustReturnATuple() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<>(tupleTypeInfo, 0);

        try {
            assertThat(maxByTuple.reduce(bigger, bigger))
                    .as("SelectByMax must return bigger tuple")
                    .isSameAs(bigger);
            assertThat(maxByTuple.reduce(smaller, smaller))
                    .as("SelectByMax must return smaller tuple")
                    .isSameAs(smaller);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }

    // ----------------------- MINIMUM FUNCTION TEST BELOW --------------------------

    /**
     * This test validates whether the order of tuples has any impact on the outcome and if the
     * smaller tuple is returned.
     */
    @Test
    void testMinByComparison() {
        SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>> minByTuple =
                new SelectByMinFunction<>(tupleTypeInfo, 0);

        try {
            assertThat(minByTuple.reduce(smaller, bigger))
                    .as("SelectByMin must return smaller tuple")
                    .isSameAs(smaller);
            assertThat(minByTuple.reduce(bigger, smaller))
                    .as("SelectByMin must return smaller tuple")
                    .isSameAs(smaller);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }

    /**
     * This test cases checks when two tuples only differ in one value, but this value is not in the
     * fields list. In that case it should be seen as equal and then the first given tuple (value1)
     * should be returned by reduce().
     */
    @Test
    void testMinByComparisonSpecialCase1() {
        SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>> minByTuple =
                new SelectByMinFunction<>(tupleTypeInfo, 0, 3);

        try {
            assertThat(minByTuple.reduce(specialCaseBigger, bigger))
                    .as("SelectByMin must return the first given tuple")
                    .isSameAs(specialCaseBigger);
            assertThat(minByTuple.reduce(bigger, specialCaseBigger))
                    .as("SelectByMin must return the first given tuple")
                    .isSameAs(bigger);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }

    /**
     * This test validates that when two tuples only differ in one value and that value's index is
     * given at construction time. The smaller tuple must be returned then.
     */
    @Test
    void testMinByComparisonSpecialCase2() {
        SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>> minByTuple =
                new SelectByMinFunction<>(tupleTypeInfo, 0, 2, 1, 4, 3);

        try {
            assertThat(minByTuple.reduce(specialCaseSmaller, smaller))
                    .as("SelectByMin must return smaller tuple")
                    .isSameAs(smaller);
            assertThat(minByTuple.reduce(smaller, specialCaseSmaller))
                    .as("SelectByMin must return smaller tuple")
                    .isSameAs(smaller);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }

    /** Checks whether reduce does behave as expected if both values are the same object. */
    @Test
    void testMinByComparisonMultiple() {
        SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>> minByTuple =
                new SelectByMinFunction<>(tupleTypeInfo, 0, 1, 2, 3, 4);

        try {
            assertThat(minByTuple.reduce(smaller, bigger))
                    .as("SelectByMin must return smaller tuple")
                    .isSameAs(smaller);
            assertThat(minByTuple.reduce(bigger, smaller))
                    .as("SelectByMin must return smaller tuple")
                    .isSameAs(smaller);
        } catch (Exception e) {
            fail("No exception should be thrown while comparing both tuples");
        }
    }
}
