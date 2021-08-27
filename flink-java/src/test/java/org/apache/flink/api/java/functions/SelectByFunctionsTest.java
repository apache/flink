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

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link SelectByMaxFunction} and {@link SelectByMinFunction}. */
public class SelectByFunctionsTest {

    private final TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>> tupleTypeInfo =
            new TupleTypeInfo<Tuple5<Integer, Long, String, Long, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private final Tuple5<Integer, Long, String, Long, Integer> bigger =
            new Tuple5<Integer, Long, String, Long, Integer>(10, 100L, "HelloWorld", 200L, 20);
    private final Tuple5<Integer, Long, String, Long, Integer> smaller =
            new Tuple5<Integer, Long, String, Long, Integer>(5, 50L, "Hello", 50L, 15);

    // Special case where only the last value determines if bigger or smaller
    private final Tuple5<Integer, Long, String, Long, Integer> specialCaseBigger =
            new Tuple5<Integer, Long, String, Long, Integer>(10, 100L, "HelloWorld", 200L, 17);
    private final Tuple5<Integer, Long, String, Long, Integer> specialCaseSmaller =
            new Tuple5<Integer, Long, String, Long, Integer>(5, 50L, "Hello", 50L, 17);

    /**
     * This test validates whether the order of tuples has any impact on the outcome and if the
     * bigger tuple is returned.
     */
    @Test
    public void testMaxByComparison() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0});

        try {
            Assert.assertSame(
                    "SelectByMax must return bigger tuple",
                    bigger,
                    maxByTuple.reduce(smaller, bigger));
            Assert.assertSame(
                    "SelectByMax must return bigger tuple",
                    bigger,
                    maxByTuple.reduce(bigger, smaller));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }

    // ----------------------- MAXIMUM FUNCTION TEST BELOW --------------------------

    /**
     * This test cases checks when two tuples only differ in one value, but this value is not in the
     * fields list. In that case it should be seen as equal and then the first given tuple (value1)
     * should be returned by reduce().
     */
    @Test
    public void testMaxByComparisonSpecialCase1() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0, 3});

        try {
            Assert.assertSame(
                    "SelectByMax must return the first given tuple",
                    specialCaseBigger,
                    maxByTuple.reduce(specialCaseBigger, bigger));
            Assert.assertSame(
                    "SelectByMax must return the first given tuple",
                    bigger,
                    maxByTuple.reduce(bigger, specialCaseBigger));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }

    /** This test cases checks when two tuples only differ in one value. */
    @Test
    public void testMaxByComparisonSpecialCase2() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0, 2, 1, 4, 3});

        try {
            Assert.assertSame(
                    "SelectByMax must return bigger tuple",
                    bigger,
                    maxByTuple.reduce(specialCaseBigger, bigger));
            Assert.assertSame(
                    "SelectByMax must return bigger tuple",
                    bigger,
                    maxByTuple.reduce(bigger, specialCaseBigger));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }

    /** This test validates that equality is independent of the amount of used indices. */
    @Test
    public void testMaxByComparisonMultiple() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0, 1, 2, 3, 4});

        try {
            Assert.assertSame(
                    "SelectByMax must return bigger tuple",
                    bigger,
                    maxByTuple.reduce(smaller, bigger));
            Assert.assertSame(
                    "SelectByMax must return bigger tuple",
                    bigger,
                    maxByTuple.reduce(bigger, smaller));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }

    /** Checks whether reduce does behave as expected if both values are the same object. */
    @Test
    public void testMaxByComparisonMustReturnATuple() {
        SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>> maxByTuple =
                new SelectByMaxFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0});

        try {
            Assert.assertSame(
                    "SelectByMax must return bigger tuple",
                    bigger,
                    maxByTuple.reduce(bigger, bigger));
            Assert.assertSame(
                    "SelectByMax must return smaller tuple",
                    smaller,
                    maxByTuple.reduce(smaller, smaller));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }

    // ----------------------- MINIMUM FUNCTION TEST BELOW --------------------------

    /**
     * This test validates whether the order of tuples has any impact on the outcome and if the
     * smaller tuple is returned.
     */
    @Test
    public void testMinByComparison() {
        SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>> minByTuple =
                new SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0});

        try {
            Assert.assertSame(
                    "SelectByMin must return smaller tuple",
                    smaller,
                    minByTuple.reduce(smaller, bigger));
            Assert.assertSame(
                    "SelectByMin must return smaller tuple",
                    smaller,
                    minByTuple.reduce(bigger, smaller));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }

    /**
     * This test cases checks when two tuples only differ in one value, but this value is not in the
     * fields list. In that case it should be seen as equal and then the first given tuple (value1)
     * should be returned by reduce().
     */
    @Test
    public void testMinByComparisonSpecialCase1() {
        SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>> minByTuple =
                new SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0, 3});

        try {
            Assert.assertSame(
                    "SelectByMin must return the first given tuple",
                    specialCaseBigger,
                    minByTuple.reduce(specialCaseBigger, bigger));
            Assert.assertSame(
                    "SelectByMin must return the first given tuple",
                    bigger,
                    minByTuple.reduce(bigger, specialCaseBigger));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }

    /**
     * This test validates that when two tuples only differ in one value and that value's index is
     * given at construction time. The smaller tuple must be returned then.
     */
    @Test
    public void testMinByComparisonSpecialCase2() {
        SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>> minByTuple =
                new SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0, 2, 1, 4, 3});

        try {
            Assert.assertSame(
                    "SelectByMin must return smaller tuple",
                    smaller,
                    minByTuple.reduce(specialCaseSmaller, smaller));
            Assert.assertSame(
                    "SelectByMin must return smaller tuple",
                    smaller,
                    minByTuple.reduce(smaller, specialCaseSmaller));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }

    /** Checks whether reduce does behave as expected if both values are the same object. */
    @Test
    public void testMinByComparisonMultiple() {
        SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>> minByTuple =
                new SelectByMinFunction<Tuple5<Integer, Long, String, Long, Integer>>(
                        tupleTypeInfo, new int[] {0, 1, 2, 3, 4});

        try {
            Assert.assertSame(
                    "SelectByMin must return smaller tuple",
                    smaller,
                    minByTuple.reduce(smaller, bigger));
            Assert.assertSame(
                    "SelectByMin must return smaller tuple",
                    smaller,
                    minByTuple.reduce(bigger, smaller));
        } catch (Exception e) {
            Assert.fail("No exception should be thrown while comparing both tuples");
        }
    }
}
