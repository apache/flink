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
import org.apache.flink.table.catalog.TableChange.ColumnPosition;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.List;

import static org.apache.flink.table.catalog.Column.physical;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link MaterializedTableUtils}. */
class MaterializedTableUtilsTest {
    @ParameterizedTest
    @MethodSource("input")
    void test(TestSpec spec) {
        assertThat(MaterializedTableUtils.buildSchemaTableChanges(spec.oldSchema, spec.newSchema))
                .isEqualTo(spec.expected);
    }

    private static Collection<TestSpec> input() {
        return List.of(
                TestSpec.of(
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("a", DataTypes.INT())),
                        List.of()),
                TestSpec.of(
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("a", DataTypes.INT()).withComment("comment")),
                        List.of(
                                TableChange.modifyColumnComment(
                                        physical("a", DataTypes.INT()), "comment"))),
                TestSpec.of(
                        schema(physical("a", DataTypes.INT()).withComment("comment")),
                        schema(physical("a", DataTypes.INT()).withComment("comment")),
                        List.of()),
                TestSpec.of(
                        schema(physical("a", DataTypes.TIMESTAMP()).withComment("comment")),
                        schema(physical("a", DataTypes.TIMESTAMP()).withComment("comment 2")),
                        List.of(
                                TableChange.modifyColumnComment(
                                        physical("a", DataTypes.TIMESTAMP()).withComment("comment"),
                                        "comment 2"))),
                TestSpec.of(
                        schema(physical("a", DataTypes.FLOAT()).withComment("comment")),
                        schema(physical("a", DataTypes.FLOAT())),
                        List.of(
                                TableChange.modifyColumnComment(
                                        physical("a", DataTypes.FLOAT()).withComment("comment"),
                                        null))),
                TestSpec.of(
                        schema(physical("a", DataTypes.INT()).withComment("comment")),
                        schema(physical("a2", DataTypes.STRING()).withComment("comment 2")),
                        List.of(
                                TableChange.add(
                                        physical("a2", DataTypes.STRING())
                                                .withComment("comment 2")),
                                TableChange.dropColumn("a"))),
                TestSpec.of(
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("b", DataTypes.INT())),
                        List.of(
                                TableChange.add(physical("b", DataTypes.INT())),
                                TableChange.dropColumn("a"))),
                TestSpec.of(
                        schema(physical("a", DataTypes.INT()), physical("b", DataTypes.BOOLEAN())),
                        schema(physical("b", DataTypes.BOOLEAN()), physical("a", DataTypes.INT())),
                        List.of(
                                TableChange.modifyColumnPosition(
                                        physical("b", DataTypes.BOOLEAN()), ColumnPosition.first()),
                                TableChange.modifyColumnPosition(
                                        physical("a", DataTypes.INT()),
                                        ColumnPosition.after("b")))),
                TestSpec.of(
                        schema(physical("a", DataTypes.INT())),
                        schema(physical("a", DataTypes.BIGINT())),
                        List.of(
                                TableChange.modifyPhysicalColumnType(
                                        physical("a", DataTypes.INT()), DataTypes.BIGINT()))));
    }

    private static ResolvedSchema schema(Column... columns) {
        return ResolvedSchema.of(columns);
    }

    private static class TestSpec {
        private final ResolvedSchema oldSchema;
        private final ResolvedSchema newSchema;
        private final List<TableChange> expected;

        public TestSpec(
                ResolvedSchema oldSchema, ResolvedSchema newSchema, List<TableChange> expected) {

            this.oldSchema = oldSchema;
            this.newSchema = newSchema;
            this.expected = expected;
        }

        public static TestSpec of(
                ResolvedSchema oldSchema, ResolvedSchema newSchema, List<TableChange> expected) {
            return new TestSpec(oldSchema, newSchema, expected);
        }
    }
}
