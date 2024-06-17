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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.PlannerMocks;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for {@link FlinkCalciteSqlValidator}. */
class FlinkCalciteSqlValidatorTest {

    private final PlannerMocks plannerMocks =
            PlannerMocks.create()
                    .registerTemporaryTable(
                            "t1", Schema.newBuilder().column("a", DataTypes.INT()).build())
                    .registerTemporaryTable(
                            "t2",
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .column("b", DataTypes.INT())
                                    .build());

    @Test
    void testUpsertInto() {
        assertThatThrownBy(() -> plannerMocks.getParser().parse("UPSERT INTO t1 VALUES(1)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "UPSERT INTO statement is not supported. Please use INSERT INTO instead.");
    }

    @Test
    void testInsertIntoShouldColumnMismatchWithValues() {
        assertThatThrownBy(() -> plannerMocks.getParser().parse("INSERT INTO t2 (a,b) VALUES(1)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(" Number of columns must match number of query columns");
    }

    @Test
    void testInsertIntoShouldColumnMismatchWithSelect() {
        assertThatThrownBy(() -> plannerMocks.getParser().parse("INSERT INTO t2 (a,b) SELECT 1"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(" Number of columns must match number of query columns");
    }

    @Test
    void testInsertIntoShouldColumnMismatchWithLastValue() {
        assertThatThrownBy(
                        () ->
                                plannerMocks
                                        .getParser()
                                        .parse("INSERT INTO t2 (a,b) VALUES (1,2), (3)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(" Number of columns must match number of query columns");
    }

    @Test
    void testInsertIntoShouldColumnMismatchWithFirstValue() {
        assertThatThrownBy(
                        () ->
                                plannerMocks
                                        .getParser()
                                        .parse("INSERT INTO t2 (a,b) VALUES (1), (2,3)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(" Number of columns must match number of query columns");
    }

    @Test
    void testInsertIntoShouldColumnMismatchWithMultiFieldValues() {
        assertThatThrownBy(
                        () ->
                                plannerMocks
                                        .getParser()
                                        .parse("INSERT INTO t2 (a,b) VALUES (1,2), (3,4,5)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(" Number of columns must match number of query columns");
    }

    @Test
    void testInsertIntoShouldNotColumnMismatchWithValues() {
        assertDoesNotThrow(
                () -> {
                    plannerMocks.getParser().parse("INSERT INTO t2 (a,b) VALUES (1,2), (3,4)");
                });
    }

    @Test
    void testInsertIntoShouldNotColumnMismatchWithSelect() {
        assertDoesNotThrow(
                () -> {
                    plannerMocks.getParser().parse("INSERT INTO t2 (a,b) Select 1, 2");
                });
    }

    @Test
    void testInsertIntoShouldNotColumnMismatchWithSingleColValues() {
        assertDoesNotThrow(
                () -> {
                    plannerMocks.getParser().parse("INSERT INTO t2 (a) VALUES (1), (3)");
                });
    }

    @Test
    void testExplainUpsertInto() {
        assertThatThrownBy(() -> plannerMocks.getParser().parse("EXPLAIN UPSERT INTO t1 VALUES(1)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "UPSERT INTO statement is not supported. Please use INSERT INTO instead.");
    }
}
