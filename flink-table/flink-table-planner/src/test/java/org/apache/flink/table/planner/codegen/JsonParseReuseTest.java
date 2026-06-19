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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.codegen.calls.BuiltInMethods;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that multiple JSON function calls on the same input reuse the parsed JSON. */
class JsonParseReuseTest {

    private static final Pattern JSON_PARSE_PATTERN =
            Pattern.compile("\\b" + Pattern.quote(BuiltInMethods.JSON_PARSE().getName()) + "\\(");

    private StreamTableEnvironment tEnv;

    private static final String JSON_ROW1 =
            "{\"type\":\"account\",\"age\":42,\"address\":{\"city\":\"Munich\"},\"roles\":[\"user\",\"viewer\"]}";
    private static final String JSON_ROW2 =
            "{\"type\":\"admin\",\"age\":30,\"address\":{\"city\":\"Berlin\"},\"roles\":[\"admin\"]}";

    @BeforeEach
    void setUp() {
        tEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.inStreamingMode());
        tEnv.createTemporaryView(
                "json_src",
                tEnv.fromValues(Row.of(JSON_ROW1, "{}"), Row.of(JSON_ROW2, "{\"x\":1}"))
                        .as("json_data", "other_json"));
    }

    private List<Row> collect(final String sql) {
        final TableResult result = tEnv.executeSql(sql);
        final List<Row> rows = new ArrayList<>();
        result.collect().forEachRemaining(rows::add);
        return rows;
    }

    private static int countJsonParse(final String code) {
        final Matcher m = JSON_PARSE_PATTERN.matcher(code);
        int count = 0;
        while (m.find()) {
            count++;
        }
        return count;
    }

    private String extractGeneratedCode(final String sql) {
        final Table table = tEnv.sqlQuery(sql);
        final Transformation<?> root = tEnv.toChangelogStream(table).getTransformation();
        final StringBuilder allCode = new StringBuilder();
        for (final Transformation<?> t : root.getTransitivePredecessors()) {
            if (t instanceof OneInputTransformation
                    && ((OneInputTransformation<?, ?>) t).getOperatorFactory()
                            instanceof CodeGenOperatorFactory) {
                final CodeGenOperatorFactory<?> factory =
                        (CodeGenOperatorFactory<?>)
                                ((OneInputTransformation<?, ?>) t).getOperatorFactory();
                allCode.append(factory.getGeneratedClass().getCode());
            }
        }
        return allCode.toString();
    }

    @Test
    void testTwoJsonValueCalls() {
        final String sql =
                "SELECT JSON_VALUE(json_data, '$.type'), JSON_VALUE(json_data, '$.age') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("account", "42"), Row.of("admin", "30"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("Two JSON_VALUE calls on the same input should parse once")
                .isEqualTo(1);
    }

    @Test
    void testTwoJsonQueryCalls() {
        final String sql =
                "SELECT JSON_QUERY(json_data, '$.address'), "
                        + "JSON_QUERY(json_data, '$.roles' WITH WRAPPER) FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("{\"city\":\"Munich\"}", "[[\"user\",\"viewer\"]]"),
                        Row.of("{\"city\":\"Berlin\"}", "[[\"admin\"]]"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("Two JSON_QUERY calls on the same input should parse once")
                .isEqualTo(1);
    }

    @Test
    void testJsonValueAndJsonQueryMixed() {
        final String sql =
                "SELECT JSON_VALUE(json_data, '$.type'), "
                        + "JSON_QUERY(json_data, '$.address') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("account", "{\"city\":\"Munich\"}"),
                        Row.of("admin", "{\"city\":\"Berlin\"}"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("JSON_VALUE + JSON_QUERY on the same input should parse once")
                .isEqualTo(1);
    }

    @Test
    void testThreeJsonFunctionCalls() {
        final String sql =
                "SELECT JSON_VALUE(json_data, '$.type'), "
                        + "JSON_VALUE(json_data, '$.age'), "
                        + "JSON_QUERY(json_data, '$.address') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("account", "42", "{\"city\":\"Munich\"}"),
                        Row.of("admin", "30", "{\"city\":\"Berlin\"}"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("Three JSON function calls on the same input should parse once")
                .isEqualTo(1);
    }

    @Test
    void testDifferentJsonInputs() {
        final String sql =
                "SELECT JSON_VALUE(json_data, '$.type'), "
                        + "JSON_VALUE(other_json, '$.x') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("account", null), Row.of("admin", "1"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("JSON_VALUE calls on different inputs should parse separately")
                .isEqualTo(2);
    }
}
