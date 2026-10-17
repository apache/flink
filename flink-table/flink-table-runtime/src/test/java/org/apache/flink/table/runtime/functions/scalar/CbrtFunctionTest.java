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
import org.apache.flink.table.types.inference.utils.CallContextMock;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Contract and numerical boundary tests for {@link CbrtFunction}. */
class CbrtFunctionTest {

    @ParameterizedTest
    @MethodSource("values")
    void testContract(Double value, Double expected) {
        final Double actual = function().eval(value);
        if (expected == null) {
            assertThat(actual).isNull();
        } else if (expected.isNaN()) {
            assertThat(actual).isNaN();
        } else if (expected.isInfinite() || expected == 0.0) {
            assertThat(actual).isNotNull();
            assertThat(Double.doubleToLongBits(actual))
                    .isEqualTo(Double.doubleToLongBits(expected));
        } else {
            assertThat(actual).isCloseTo(expected, within(Math.ulp(expected) * 2));
        }
    }

    private static Stream<Arguments> values() {
        return Stream.of(
                Arguments.of(null, null),
                Arguments.of(0.0, 0.0),
                Arguments.of(-0.0, -0.0),
                Arguments.of(27.0, 3.0),
                Arguments.of(-27.0, -3.0),
                Arguments.of(8.0, 2.0),
                Arguments.of(-8.0, -2.0),
                Arguments.of(5e-324, 1.7031839360032603e-108),
                Arguments.of(1.7976931348623157e+308, 5.643803094122362e+102),
                Arguments.of(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
                Arguments.of(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY),
                Arguments.of(Double.NaN, Double.NaN));
    }

    private static CbrtFunction function() {
        final CallContextMock context = new CallContextMock();
        context.argumentDataTypes = Collections.singletonList(DataTypes.DOUBLE());
        context.outputDataType = Optional.of(DataTypes.DOUBLE());
        return (CbrtFunction)
                UserDefinedFunctionHelper.createSpecializedFunction(
                        "CBRT",
                        BuiltInFunctionDefinitions.CBRT,
                        context,
                        CbrtFunctionTest.class.getClassLoader(),
                        null,
                        null);
    }
}
