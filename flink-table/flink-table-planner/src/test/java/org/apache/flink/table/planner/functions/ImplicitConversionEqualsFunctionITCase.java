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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;

/**
 * Tests for type conversions in '='. These tests are only for SQL API. We temporarily forbid "="
 * between numeric and (var)char fields and throw an exception because it will produce wrong result.
 * SEE [FLINK-24914].
 */
public class ImplicitConversionEqualsFunctionITCase extends BuiltInFunctionTestBase {

    // numeric data
    private static final byte TINY_INT_DATA = (byte) 1;
    private static final short SMALL_INT_DATA = (short) 1;
    private static final int INT_DATA = 1;
    private static final long BIG_INT_DATA = 1L;
    private static final float FLOAT_DATA = 1.0f;
    private static final double DOUBLE_DATA = 1.0d;
    private static final BigDecimal DECIMAL_DATA = new BigDecimal(1);

    // time data
    private static final String DATE_DATA = "2001-01-01";
    private static final String TIME_DATA = "00:00:00";
    private static final String TIMESTAMP_DATA = ("2001-01-01 00:00:00");

    // string data
    private static final String STRING_DATA_EQUALS_NUMERIC = "1";
    private static final String STRING_DATA_EQUALS_DATE = "2001-01-01";
    private static final String STRING_DATA_EQUALS_TIME = "00:00:00";
    private static final String STRING_DATA_EQUALS_TIMESTAMP = "2001-01-01 00:00:00";

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        final List<TestSpec> specs = new ArrayList<>();
        specs.addAll(implicitConversionBetweenNumeric());
        specs.addAll(implicitConversionBetweenTimeAndString());
        specs.addAll(unsupportedImplicitConversionBetweenNumericAndString());
        return specs;
    }

    private static List<TestSpec> implicitConversionBetweenNumeric() {
        return Arrays.asList(
                TypeConversionTestBuilder.left(TINYINT(), TINY_INT_DATA)
                        .right(TINYINT(), TINY_INT_DATA)
                        .right(SMALLINT(), SMALL_INT_DATA)
                        .right(INT(), INT_DATA)
                        .right(BIGINT(), BIG_INT_DATA)
                        .right(FLOAT(), FLOAT_DATA)
                        .right(DOUBLE(), DOUBLE_DATA)
                        .right(DECIMAL(1, 0), DECIMAL_DATA)
                        .build(),
                TypeConversionTestBuilder.left(SMALLINT(), SMALL_INT_DATA)
                        .right(SMALLINT(), SMALL_INT_DATA)
                        .right(INT(), INT_DATA)
                        .right(BIGINT(), BIG_INT_DATA)
                        .right(FLOAT(), FLOAT_DATA)
                        .right(DOUBLE(), DOUBLE_DATA)
                        .right(DECIMAL(1, 0), DECIMAL_DATA)
                        .build(),
                TypeConversionTestBuilder.left(INT(), INT_DATA)
                        .right(INT(), INT_DATA)
                        .right(BIGINT(), BIG_INT_DATA)
                        .right(FLOAT(), FLOAT_DATA)
                        .right(DOUBLE(), DOUBLE_DATA)
                        .right(DECIMAL(1, 0), DECIMAL_DATA)
                        .build(),
                TypeConversionTestBuilder.left(BIGINT(), BIG_INT_DATA)
                        .right(BIGINT(), BIG_INT_DATA)
                        .right(FLOAT(), FLOAT_DATA)
                        .right(DOUBLE(), DOUBLE_DATA)
                        .right(DECIMAL(1, 0), DECIMAL_DATA)
                        .build(),
                TypeConversionTestBuilder.left(FLOAT(), FLOAT_DATA)
                        .right(FLOAT(), FLOAT_DATA)
                        .right(DOUBLE(), DOUBLE_DATA)
                        .right(DECIMAL(1, 0), DECIMAL_DATA)
                        .build(),
                TypeConversionTestBuilder.left(DOUBLE(), DOUBLE_DATA)
                        .right(DOUBLE(), DOUBLE_DATA)
                        .right(DECIMAL(1, 0), DECIMAL_DATA)
                        .build());
    }

    private static List<TestSpec> implicitConversionBetweenTimeAndString() {
        return Arrays.asList(
                TypeConversionTestBuilder.left(DATE(), DATE_DATA)
                        .right(DATE(), DATE_DATA)
                        .right(STRING(), STRING_DATA_EQUALS_DATE)
                        .build(),
                TypeConversionTestBuilder.left(TIME(), TIME_DATA)
                        .right(STRING(), STRING_DATA_EQUALS_TIME)
                        .build(),
                TypeConversionTestBuilder.left(TIMESTAMP(), TIMESTAMP_DATA)
                        .right(STRING(), STRING_DATA_EQUALS_TIMESTAMP)
                        .build());
    }

    // unsupported temporarily
    private static List<TestSpec> unsupportedImplicitConversionBetweenNumericAndString() {
        return Collections.singletonList(
                TypeConversionTestBuilder.left(STRING(), STRING_DATA_EQUALS_NUMERIC)
                        .right(STRING(), STRING_DATA_EQUALS_NUMERIC)
                        .fail(TINYINT(), TINY_INT_DATA)
                        .fail(SMALLINT(), SMALL_INT_DATA)
                        .fail(INT(), INT_DATA)
                        .fail(BIGINT(), BIG_INT_DATA)
                        .fail(FLOAT(), FLOAT_DATA)
                        .fail(DOUBLE(), DOUBLE_DATA)
                        .fail(DECIMAL(1, 0), DECIMAL_DATA)
                        .build());
    }

    static class TypeConversionTestBuilder {
        private DataType leftType;
        private Object leftValue;
        private final List<Object> rightDataOnSuccess = new ArrayList<>();
        private final List<DataType> rightTypesOnSuccess = new ArrayList<>();
        private final List<Object> rightDataOnFailure = new ArrayList<>();
        private final List<DataType> rightTypesOnFailure = new ArrayList<>();

        private static TypeConversionTestBuilder left(DataType leftType, Object leftValue) {
            TypeConversionTestBuilder builder = new TypeConversionTestBuilder();
            builder.leftType = leftType;
            builder.leftValue = leftValue;
            return builder;
        }

        private TypeConversionTestBuilder right(DataType rightType, Object rightValue) {
            this.rightTypesOnSuccess.add(rightType);
            this.rightDataOnSuccess.add(rightValue);
            return this;
        }

        private TypeConversionTestBuilder fail(DataType rightType, Object rightValue) {
            this.rightTypesOnFailure.add(rightType);
            this.rightDataOnFailure.add(rightValue);
            return this;
        }

        private TestSpec build() {
            int columnBaseIdx = 0;
            String leftColumnName = "f" + columnBaseIdx;
            columnBaseIdx++;

            TestSpec testSpec =
                    TestSpec.forFunction(
                            BuiltInFunctionDefinitions.EQUALS, "left: " + leftType.toString());

            final List<Object> allData = new ArrayList<>();
            allData.add(leftValue);
            allData.addAll(rightDataOnSuccess);
            allData.addAll(rightDataOnFailure);

            final List<Object> allTypes = new ArrayList<>();
            allTypes.add(leftType);
            allTypes.addAll(rightTypesOnSuccess);
            allTypes.addAll(rightTypesOnFailure);

            testSpec.onFieldsWithData(allData.toArray())
                    .andDataTypes(allTypes.toArray(new AbstractDataType<?>[] {}));

            // test successful cases
            for (int i = 0; i < rightTypesOnSuccess.size(); i++) {
                String rightColumnName = "f" + (i + columnBaseIdx);
                DataType rightType = rightTypesOnSuccess.get(i);
                testSpec.testSqlResult(
                        String.format(
                                "CAST(%s AS %s) = CAST(%s AS %s)",
                                leftColumnName, leftType.toString(), rightColumnName, rightType),
                        true,
                        BOOLEAN());
            }

            columnBaseIdx = columnBaseIdx + rightTypesOnSuccess.size();
            // test failed cases
            for (int i = 0; i < rightTypesOnFailure.size(); i++) {
                String rightColumnName = "f" + (i + columnBaseIdx);
                DataType rightType = rightTypesOnFailure.get(i);
                String exceptionMsg =
                        getImplicitConversionFromStringExceptionMsg(
                                rightType.getLogicalType().getTypeRoot());
                testSpec.testSqlRuntimeError(
                        String.format(
                                "CAST(%s AS %s) = CAST(%s AS %s)",
                                leftColumnName, leftType.toString(), rightColumnName, rightType),
                        exceptionMsg);
            }
            return testSpec;
        }

        private String getImplicitConversionFromStringExceptionMsg(LogicalTypeRoot rightType) {
            return String.format(
                    "implicit type conversion between VARCHAR and %s is not supported now",
                    rightType.toString());
        }
    }
}
