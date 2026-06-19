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

package org.apache.flink.table.types.extraction;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.extraction.DataTypeExtractorTest.TestSpec;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.types.extraction.DataTypeExtractorTest.runExtraction;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link DataTypeExtractor} on Java record classes.
 *
 * <p>Move this to {@link DataTypeExtractorTest} once we support Java 17 language level.
 */
class DataTypeExtractorJava17Test {

    private static Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forType(PojoByRecord.class)
                        .expectDataType(
                                DataTypes.STRUCTURED(
                                        PojoByRecord.class,
                                        DataTypes.FIELD(
                                                "record1",
                                                DataTypes.INT().notNull().bridgedTo(int.class)),
                                        DataTypes.FIELD("record2", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "record3",
                                                DataTypes.DOUBLE()
                                                        .notNull()
                                                        .bridgedTo(double.class)),
                                        DataTypes.FIELD(
                                                "nestedRecord",
                                                DataTypes.STRUCTURED(
                                                        PojoByRecordNested.class,
                                                        DataTypes.FIELD(
                                                                "nested1",
                                                                DataTypes.INT()
                                                                        .notNull()
                                                                        .bridgedTo(int.class)),
                                                        DataTypes.FIELD(
                                                                "nested2",
                                                                DataTypes.VARCHAR(200)))))));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testExtraction(TestSpec testSpec) {
        if (testSpec.expectedErrorMessage != null) {
            assertThatThrownBy(() -> runExtraction(testSpec))
                    .isInstanceOf(ValidationException.class)
                    .satisfies(
                            anyCauseMatches(
                                    ValidationException.class, testSpec.expectedErrorMessage));
        } else {
            runExtraction(testSpec);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    /** Basic test record. */
    public record PojoByRecord(
            int record1, String record2, double record3, PojoByRecordNested nestedRecord) {}

    /** Complex test record. */
    public record PojoByRecordNested(
            int nested1,
            // Record signature has precedence over the
            // custom constructor below
            @DataTypeHint("VARCHAR(200)") String nested2) {

        public PojoByRecordNested(int nested1, String nested2) {
            this.nested1 = nested1;
            this.nested2 = nested2;
        }

        public PojoByRecordNested(int nested1) {
            this(nested1, "UNKNOWN");
        }
    }
}
