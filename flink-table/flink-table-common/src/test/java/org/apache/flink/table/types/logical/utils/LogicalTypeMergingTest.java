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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link LogicalTypeMerging} for finding the result decimal type for the various
 * operations, e.g.: {@link LogicalTypeMerging#findSumAggType(LogicalType)}, {@link
 * LogicalTypeMerging#findAdditionDecimalType(int, int, int, int)}, etc.
 *
 * <p>For {@link LogicalTypeMerging#findCommonType(List)} tests please check {@link
 * org.apache.flink.table.types.LogicalCommonTypeTest}
 */
public class LogicalTypeMergingTest {

    @Test
    public void testFindDivisionDecimalType() {
        assertThat(
                LogicalTypeMerging.findDivisionDecimalType(32, 8, 38, 8), equalTo(decimal(38, 6)));
        assertThat(
                LogicalTypeMerging.findDivisionDecimalType(30, 20, 30, 20),
                equalTo(decimal(38, 8)));
    }

    @Test
    public void testFindMultiplicationDecimalType() {
        assertThat(
                LogicalTypeMerging.findMultiplicationDecimalType(30, 10, 30, 10),
                equalTo(decimal(38, 6)));
        assertThat(
                LogicalTypeMerging.findMultiplicationDecimalType(30, 20, 30, 20),
                equalTo(decimal(38, 17)));
        assertThat(
                LogicalTypeMerging.findMultiplicationDecimalType(38, 2, 38, 3),
                equalTo(decimal(38, 5)));
    }

    @Test
    public void testFindModuloDecimalType() {
        assertThat(
                LogicalTypeMerging.findModuloDecimalType(30, 10, 30, 10), equalTo(decimal(30, 10)));
        assertThat(
                LogicalTypeMerging.findModuloDecimalType(30, 20, 25, 20), equalTo(decimal(25, 20)));
        assertThat(
                LogicalTypeMerging.findModuloDecimalType(10, 10, 10, 10), equalTo(decimal(10, 10)));
    }

    @Test
    public void testFindAdditionDecimalType() {
        assertThat(
                LogicalTypeMerging.findAdditionDecimalType(38, 8, 32, 8), equalTo(decimal(38, 7)));
        assertThat(
                LogicalTypeMerging.findAdditionDecimalType(32, 8, 38, 8), equalTo(decimal(38, 7)));
        assertThat(
                LogicalTypeMerging.findAdditionDecimalType(30, 20, 28, 20),
                equalTo(decimal(31, 20)));
        assertThat(
                LogicalTypeMerging.findAdditionDecimalType(10, 10, 10, 10),
                equalTo(decimal(11, 10)));
        assertThat(
                LogicalTypeMerging.findAdditionDecimalType(38, 5, 38, 4), equalTo(decimal(38, 5)));
    }

    @Test
    public void testFindRoundingDecimalType() {
        assertThat(LogicalTypeMerging.findRoundDecimalType(32, 8, 5), equalTo(decimal(30, 5)));
        assertThat(LogicalTypeMerging.findRoundDecimalType(32, 8, 10), equalTo(decimal(32, 8)));
        assertThat(LogicalTypeMerging.findRoundDecimalType(30, 20, 18), equalTo(decimal(29, 18)));
        assertThat(LogicalTypeMerging.findRoundDecimalType(10, 10, 2), equalTo(decimal(3, 2)));
    }

    @Test
    public void testFindAvgAggType() {
        assertThat(LogicalTypeMerging.findAvgAggType(decimal(38, 20)), equalTo(decimal(38, 20)));
        assertThat(LogicalTypeMerging.findAvgAggType(decimal(38, 2)), equalTo(decimal(38, 6)));
        assertThat(LogicalTypeMerging.findAvgAggType(decimal(38, 8)), equalTo(decimal(38, 8)));
        assertThat(LogicalTypeMerging.findAvgAggType(decimal(30, 20)), equalTo(decimal(38, 20)));
        assertThat(LogicalTypeMerging.findAvgAggType(decimal(10, 10)), equalTo(decimal(38, 10)));
    }

    private static final DecimalType decimal(int precision, int scale) {
        return new DecimalType(false, precision, scale);
    }
}
