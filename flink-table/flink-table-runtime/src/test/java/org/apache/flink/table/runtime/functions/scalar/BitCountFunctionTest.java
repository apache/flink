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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests Spark-compatible 64-bit population counts. */
class BitCountFunctionTest {

    @ParameterizedTest
    @MethodSource("integerCases")
    void testIntegers(Number value, int expected) {
        assertThat(function(DataTypes.BIGINT()).eval(value)).isEqualTo(expected);
    }

    private static Stream<Arguments> integerCases() {
        return Stream.of(
                Arguments.of((byte) 0, 0),
                Arguments.of((byte) 3, 2),
                Arguments.of((byte) -1, 64),
                Arguments.of(Byte.MIN_VALUE, 57),
                Arguments.of(Byte.MAX_VALUE, 7),
                Arguments.of((short) 0, 0),
                Arguments.of((short) 3, 2),
                Arguments.of((short) -1, 64),
                Arguments.of(Short.MIN_VALUE, 49),
                Arguments.of(Short.MAX_VALUE, 15),
                Arguments.of(0, 0),
                Arguments.of(3, 2),
                Arguments.of(-1, 64),
                Arguments.of(Integer.MIN_VALUE, 33),
                Arguments.of(Integer.MAX_VALUE, 31),
                Arguments.of(0L, 0),
                Arguments.of(3L, 2),
                Arguments.of(-1L, 64),
                Arguments.of(Long.MIN_VALUE, 1),
                Arguments.of(Long.MAX_VALUE, 63),
                Arguments.of(0xAAAAAAAAAAAAAAAAL, 32),
                Arguments.of(0x5555555555555555L, 32));
    }

    @Test
    void testEveryBitPosition() {
        final BitCountFunction function = function(DataTypes.BIGINT());
        for (int bit = 0; bit < Long.SIZE; bit++) {
            assertThat(function.eval(1L << bit)).as("bit %s", bit).isEqualTo(1);
            assertThat(function.eval(~(1L << bit))).as("unset bit %s", bit).isEqualTo(63);
        }
    }

    @Test
    void testAllTinyInts() {
        final BitCountFunction function = function(DataTypes.TINYINT());
        for (int value = Byte.MIN_VALUE; value <= Byte.MAX_VALUE; value++) {
            // Count the low eight bits independently, then account for sign extension.
            int expected = value < 0 ? 56 : 0;
            for (int bit = 0; bit < Byte.SIZE; bit++) {
                expected += (value >>> bit) & 1;
            }
            assertThat(function.eval((byte) value)).as("value %s", value).isEqualTo(expected);
        }
    }

    @Test
    void testNullInteger() {
        assertThat(function(DataTypes.BIGINT()).eval((Number) null)).isNull();
    }

    @Test
    void testBooleans() {
        final BitCountFunction function = function(DataTypes.BOOLEAN());
        assertThat(function.eval(true)).isEqualTo(1);
        assertThat(function.eval(false)).isZero();
        assertThat(function.eval((Boolean) null)).isNull();
    }

    private static BitCountFunction function(DataType inputType) {
        final CallContextMock callContext = new CallContextMock();
        callContext.argumentDataTypes = Collections.singletonList(inputType);
        callContext.outputDataType = Optional.of(DataTypes.INT());
        return (BitCountFunction)
                UserDefinedFunctionHelper.createSpecializedFunction(
                        "BIT_COUNT",
                        BuiltInFunctionDefinitions.BIT_COUNT,
                        callContext,
                        BitCountFunctionTest.class.getClassLoader(),
                        null,
                        null);
    }
}
