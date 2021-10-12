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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.planner.functions.CastFunctionITCase;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This class runs unit tests of {@link CastRule} implementations. For IT test cases, check out the
 * {@link CastFunctionITCase}
 */
class CastRulesTest {

    Stream<CastTestSpecBuilder> testCases() {
        return Stream.of(
                CastTestSpecBuilder.testCastTo(BIGINT())
                        .fromCase(BIGINT(), 10L, 10L)
                        .fromCase(INT(), 10, 10L)
                        .fromCase(SMALLINT(), (short) 10, 10L)
                        .fromCase(TINYINT(), (byte) 10, 10L),
                CastTestSpecBuilder.testCastTo(STRING())
                        .fromCase(
                                STRING(),
                                StringData.fromString("Hello"),
                                StringData.fromString("Hello"))
                        .fromCase(
                                TIMESTAMP(),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2021-09-24T12:34:56.123456")),
                                StringData.fromString("2021-09-24 12:34:56.123456"))
                        .fromCase(
                                TIMESTAMP_LTZ(),
                                CastRule.Context.create(
                                        ZoneId.of("CET"),
                                        Thread.currentThread().getContextClassLoader()),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.parse("2021-09-24T12:34:56.123456")),
                                StringData.fromString("2021-09-24 14:34:56.123456")),
                CastTestSpecBuilder.testCastTo(ARRAY(STRING().nullable()))
                        .fromCase(
                                ARRAY(TIMESTAMP().nullable()),
                                new GenericArrayData(
                                        new Object[] {
                                            TimestampData.fromLocalDateTime(
                                                    LocalDateTime.parse(
                                                            "2021-09-24T12:34:56.123456")),
                                            null,
                                            TimestampData.fromLocalDateTime(
                                                    LocalDateTime.parse(
                                                            "2021-09-24T14:34:56.123456"))
                                        }),
                                new GenericArrayData(
                                        new Object[] {
                                            StringData.fromString("2021-09-24 12:34:56.123456"),
                                            null,
                                            StringData.fromString("2021-09-24 14:34:56.123456")
                                        })),
                CastTestSpecBuilder.testCastTo(ARRAY(BIGINT().nullable()))
                        .fromCase(
                                ARRAY(INT().nullable()),
                                new GenericArrayData(new Object[] {1, null, 2}),
                                new GenericArrayData(new Object[] {1L, null, 2L})),
                CastTestSpecBuilder.testCastTo(ARRAY(BIGINT().notNull()))
                        .fromCase(
                                ARRAY(INT().notNull()),
                                new GenericArrayData(new int[] {1, 2}),
                                new GenericArrayData(new long[] {1L, 2L})));
    }

    @TestFactory
    Stream<DynamicTest> castTests() {
        return DynamicTest.stream(
                testCases().flatMap(CastTestSpecBuilder::toSpecs),
                CastTestSpec::toString,
                CastTestSpec::run);
    }

    private static class CastTestSpec {
        private final DataType inputType;
        private final DataType targetType;
        private final Object inputData;
        private final Object expectedData;
        private final CastRule.Context castContext;

        public CastTestSpec(
                DataType inputType,
                DataType targetType,
                Object inputData,
                Object expectedData,
                CastRule.Context castContext) {
            this.inputType = inputType;
            this.targetType = targetType;
            this.inputData = inputData;
            this.expectedData = expectedData;
            this.castContext = castContext;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public void run() throws Exception {
            CastExecutor executor =
                    CastRuleProvider.create(
                            this.castContext,
                            this.inputType.getLogicalType(),
                            this.targetType.getLogicalType());
            assertNotNull(executor);

            assertEquals(this.expectedData, executor.cast(this.inputData));
        }

        @Override
        public String toString() {
            return inputType + " => " + targetType + " {" + inputData + " => " + expectedData + '}';
        }
    }

    private static class CastTestSpecBuilder {
        private DataType targetType;
        private final List<Object> inputData = new ArrayList<>();
        private final List<DataType> inputTypes = new ArrayList<>();
        private final List<Object> expectedValues = new ArrayList<>();
        private final List<CastRule.Context> castContexts = new ArrayList<>();

        private static CastTestSpecBuilder testCastTo(DataType targetType) {
            CastTestSpecBuilder tsb = new CastTestSpecBuilder();
            tsb.targetType = targetType;
            return tsb;
        }

        private CastTestSpecBuilder fromCase(DataType dataType, Object src, Object target) {
            this.inputTypes.add(dataType);
            this.inputData.add(src);
            this.expectedValues.add(target);
            this.castContexts.add(
                    CastRule.Context.create(
                            ZoneId.systemDefault(),
                            Thread.currentThread().getContextClassLoader()));
            return this;
        }

        private CastTestSpecBuilder fromCase(
                DataType dataType, CastRule.Context castContext, Object src, Object target) {
            this.inputTypes.add(dataType);
            this.inputData.add(src);
            this.expectedValues.add(target);
            this.castContexts.add(castContext);
            return this;
        }

        private Stream<CastTestSpec> toSpecs() {
            CastTestSpec[] testSpecs = new CastTestSpec[inputData.size()];
            for (int i = 0; i < inputData.size(); i++) {
                testSpecs[i] =
                        new CastTestSpec(
                                inputTypes.get(i),
                                targetType,
                                inputData.get(i),
                                expectedValues.get(i),
                                castContexts.get(i));
            }
            return Arrays.stream(testSpecs);
        }
    }
}
