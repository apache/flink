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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.List;

import static org.apache.flink.table.catalog.Column.computed;
import static org.apache.flink.table.catalog.Column.metadata;
import static org.apache.flink.table.catalog.Column.physical;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MaterializedTableUtils#validateAndExtractColumnChanges}. */
class ValidateAndExtractColumnChangesTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("input")
    void test(TestSpec spec) {
        assertThat(
                        MaterializedTableUtils.validateAndExtractColumnChanges(
                                spec.oldSchema, spec.newSchema, spec.schemaDefinedInQuery))
                .containsExactlyInAnyOrderElementsOf(spec.expected);
    }

    private static Collection<TestSpec> input() {
        return List.of(
                TestSpec.of(
                        "identical schemas",
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("a", DataTypes.INT())),
                        true,
                        List.of()),
                TestSpec.of(
                        "comment added",
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("a", DataTypes.INT()).withComment("hello")),
                        true,
                        List.of(
                                TableChange.modifyColumnComment(
                                        physical("a", DataTypes.INT()), "hello"))),
                TestSpec.of(
                        "comment removed",
                        schema(physical("a", DataTypes.INT()).withComment("hello")),
                        schema(physical("a", DataTypes.INT())),
                        true,
                        List.of(
                                TableChange.modifyColumnComment(
                                        physical("a", DataTypes.INT()).withComment("hello"),
                                        null))),
                TestSpec.of(
                        "comment changed",
                        schema(physical("a", DataTypes.INT()).withComment("old")),
                        schema(physical("a", DataTypes.INT()).withComment("new")),
                        true,
                        List.of(
                                TableChange.modifyColumnComment(
                                        physical("a", DataTypes.INT()).withComment("old"), "new"))),
                TestSpec.of(
                        "single column appended",
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())),
                        true,
                        List.of(TableChange.add(physical("b", DataTypes.STRING())))),
                TestSpec.of(
                        "multiple columns appended",
                        schema(physical("a", DataTypes.INT())),
                        schema(
                                physical("a", DataTypes.INT()),
                                physical("b", DataTypes.STRING()),
                                physical("c", DataTypes.BOOLEAN())),
                        true,
                        List.of(
                                TableChange.add(physical("b", DataTypes.STRING())),
                                TableChange.add(physical("c", DataTypes.BOOLEAN())))),
                TestSpec.of(
                        "nullability differs but schema is not defined in query",
                        schema(physical("a", DataTypes.INT().notNull())),
                        schema(physical("a", DataTypes.INT())),
                        false,
                        List.of()),
                TestSpec.of(
                        "computed columns are ignored in persisted comparison",
                        schema(
                                physical("a", DataTypes.INT()),
                                computed("comp", expr(DataTypes.INT()))),
                        schema(
                                physical("a", DataTypes.INT()),
                                physical("b", DataTypes.STRING()),
                                computed("comp", expr(DataTypes.INT()))),
                        true,
                        List.of(TableChange.add(physical("b", DataTypes.STRING())))),
                TestSpec.of(
                        "virtual metadata column drop emits dropColumn",
                        schema(
                                physical("a", DataTypes.INT()),
                                metadata("v", DataTypes.INT(), null, true)),
                        schema(physical("a", DataTypes.INT())),
                        true,
                        List.of(TableChange.dropColumn("v"))),
                TestSpec.of(
                        "non-virtual metadata column treated as persisted",
                        schema(physical("a", DataTypes.INT())),
                        schema(
                                physical("a", DataTypes.INT()),
                                metadata("m", DataTypes.STRING(), null, false)),
                        true,
                        List.of(TableChange.add(metadata("m", DataTypes.STRING(), null, false)))),
                TestSpec.of(
                        "schemaDefinedInQuery=false makes added column nullable",
                        schema(physical("a", DataTypes.INT())),
                        schema(
                                physical("a", DataTypes.INT()),
                                physical("b", DataTypes.STRING().notNull())),
                        false,
                        List.of(TableChange.add(physical("b", DataTypes.STRING())))),
                TestSpec.of(
                        "drop persisted column emits dropColumn",
                        schema(physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())),
                        schema(physical("a", DataTypes.INT())),
                        true,
                        List.of(TableChange.dropColumn("b"))),
                TestSpec.of(
                        "rename persisted column emits drop + add",
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("b", DataTypes.INT())),
                        true,
                        List.of(
                                TableChange.dropColumn("a"),
                                TableChange.add(physical("b", DataTypes.INT())))),
                TestSpec.of(
                        "persisted type change emits modifyPhysicalColumnType",
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("a", DataTypes.STRING())),
                        true,
                        List.of(
                                TableChange.modifyPhysicalColumnType(
                                        physical("a", DataTypes.INT()), DataTypes.STRING()))),
                TestSpec.of(
                        "nullability change with schemaDefinedInQuery=true emits modifyPhysicalColumnType",
                        schema(physical("a", DataTypes.INT().notNull())),
                        schema(physical("a", DataTypes.INT())),
                        true,
                        List.of(
                                TableChange.modifyPhysicalColumnType(
                                        physical("a", DataTypes.INT().notNull()),
                                        DataTypes.INT()))),
                TestSpec.of(
                        "type change + comment change both emitted",
                        schema(physical("a", DataTypes.INT()).withComment("old")),
                        schema(physical("a", DataTypes.STRING()).withComment("new")),
                        true,
                        List.of(
                                TableChange.modifyPhysicalColumnType(
                                        physical("a", DataTypes.INT()).withComment("old"),
                                        DataTypes.STRING()),
                                TableChange.modifyColumnComment(
                                        physical("a", DataTypes.INT()).withComment("old"), "new"))),
                TestSpec.of(
                        "reorder persisted columns is silent (DDL order is arbitrary)",
                        schema(physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())),
                        schema(physical("b", DataTypes.STRING()), physical("a", DataTypes.INT())),
                        true,
                        List.of()),
                TestSpec.of(
                        "add computed column",
                        schema(physical("a", DataTypes.INT())),
                        schema(
                                physical("a", DataTypes.INT()),
                                computed("comp", expr(DataTypes.INT()))),
                        true,
                        List.of(TableChange.add(computed("comp", expr(DataTypes.INT()))))),
                TestSpec.of(
                        "add virtual metadata column",
                        schema(physical("a", DataTypes.INT())),
                        schema(
                                physical("a", DataTypes.INT()),
                                metadata("v", DataTypes.STRING(), null, true)),
                        true,
                        List.of(TableChange.add(metadata("v", DataTypes.STRING(), null, true)))),
                TestSpec.of(
                        "drop computed column",
                        schema(
                                physical("a", DataTypes.INT()),
                                computed("comp", expr(DataTypes.INT()))),
                        schema(physical("a", DataTypes.INT())),
                        true,
                        List.of(TableChange.dropColumn("comp"))),
                TestSpec.of(
                        "drop virtual metadata column",
                        schema(
                                physical("a", DataTypes.INT()),
                                metadata("v", DataTypes.STRING(), null, true)),
                        schema(physical("a", DataTypes.INT())),
                        true,
                        List.of(TableChange.dropColumn("v"))),
                TestSpec.of(
                        "modify computed column expression emits modifyColumn",
                        schema(
                                physical("a", DataTypes.INT()),
                                computed("comp", expr(DataTypes.INT()))),
                        schema(
                                physical("a", DataTypes.INT()),
                                computed("comp", expr(DataTypes.BIGINT()))),
                        true,
                        List.of(
                                new TableChange.ModifyColumn(
                                        computed("comp", expr(DataTypes.INT())),
                                        computed("comp", expr(DataTypes.BIGINT())),
                                        null))));
    }

    private static ResolvedSchema schema(Column... columns) {
        return ResolvedSchema.of(columns);
    }

    private static ResolvedExpression expr(DataType type) {
        return new ResolvedExpressionMock(type, () -> "1");
    }

    private static class TestSpec {
        private final String name;
        private final ResolvedSchema oldSchema;
        private final ResolvedSchema newSchema;
        private final boolean schemaDefinedInQuery;
        private final List<TableChange> expected;

        TestSpec(
                String name,
                ResolvedSchema oldSchema,
                ResolvedSchema newSchema,
                boolean schemaDefinedInQuery,
                List<TableChange> expected) {
            this.name = name;
            this.oldSchema = oldSchema;
            this.newSchema = newSchema;
            this.schemaDefinedInQuery = schemaDefinedInQuery;
            this.expected = expected;
        }

        static TestSpec of(
                String name,
                ResolvedSchema oldSchema,
                ResolvedSchema newSchema,
                boolean schemaDefinedInQuery,
                List<TableChange> expected) {
            return new TestSpec(name, oldSchema, newSchema, schemaDefinedInQuery, expected);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
