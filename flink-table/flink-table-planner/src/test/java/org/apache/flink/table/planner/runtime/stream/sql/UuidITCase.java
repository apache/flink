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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.of;

/** Runtime tests for the {@code UUID} type through the full plan/codegen/execution stack. */
class UuidITCase {

    private static final UUID UUID_A = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    private static final UUID UUID_B = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

    private static Stream<Arguments> uuidQueries() {
        return Stream.of(
                of(
                        "scalar literal",
                        "SELECT UUID '550e8400-e29b-41d4-a716-446655440000'",
                        List.of(Row.of(UUID_A))),
                of(
                        "array",
                        "SELECT ARRAY[UUID '550e8400-e29b-41d4-a716-446655440000', "
                                + "UUID 'f47ac10b-58cc-4372-a567-0e02b2c3d479']",
                        List.of(Row.of((Object) new UUID[] {UUID_A, UUID_B}))),
                of(
                        "multi-row values",
                        "SELECT id FROM (VALUES (UUID '550e8400-e29b-41d4-a716-446655440000'), "
                                + "(UUID 'f47ac10b-58cc-4372-a567-0e02b2c3d479')) AS t(id)",
                        List.of(Row.of(UUID_A), Row.of(UUID_B))),
                of(
                        "union and case",
                        "SELECT CASE WHEN 1 = 1 THEN UUID '550e8400-e29b-41d4-a716-446655440000' "
                                + "ELSE UUID 'f47ac10b-58cc-4372-a567-0e02b2c3d479' END "
                                + "UNION ALL SELECT UUID 'f47ac10b-58cc-4372-a567-0e02b2c3d479'",
                        List.of(Row.of(UUID_A), Row.of(UUID_B))),
                of(
                        "map value",
                        "SELECT MAP['a', UUID '550e8400-e29b-41d4-a716-446655440000']",
                        List.of(Row.of(Map.of("a", UUID_A)))),
                of(
                        "nested row",
                        "SELECT (UUID '550e8400-e29b-41d4-a716-446655440000', 42)",
                        List.of(Row.of(Row.of(UUID_A, 42)))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("uuidQueries")
    void testUuidRuntime(String description, String sql, List<Row> expected) throws Exception {
        assertThat(collect(sql)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testInvalidUuidLiteralFailsValidation() {
        assertThatThrownBy(() -> collect("SELECT UUID 'abcd'"))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Invalid UUID string: abcd");
    }

    private static List<Row> collect(String sql) throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableEnv.executeSql(sql).collect()) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }
}
