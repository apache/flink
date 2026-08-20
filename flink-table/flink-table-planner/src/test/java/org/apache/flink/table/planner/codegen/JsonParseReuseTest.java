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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.codesplit.JavaCodeSplitter;
import org.apache.flink.table.planner.codegen.calls.BuiltInMethods;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private List<String> generatedClassCodes(final String sql) {
        final Table table = tEnv.sqlQuery(sql);
        final Transformation<?> root = tEnv.toChangelogStream(table).getTransformation();
        final List<String> codes = new ArrayList<>();
        for (final Transformation<?> t : root.getTransitivePredecessors()) {
            if (t instanceof OneInputTransformation
                    && ((OneInputTransformation<?, ?>) t).getOperatorFactory()
                            instanceof CodeGenOperatorFactory) {
                final CodeGenOperatorFactory<?> factory =
                        (CodeGenOperatorFactory<?>)
                                ((OneInputTransformation<?, ?>) t).getOperatorFactory();
                codes.add(factory.getGeneratedClass().getCode());
            }
        }
        return codes;
    }

    private String extractGeneratedCode(final String sql) {
        return String.join("", generatedClassCodes(sql));
    }

    @Test
    void testTwoJsonValueCalls() {
        final String sql =
                "SELECT JSON_VALUE(json_data, '$.type'), JSON_VALUE(json_data, '$.age') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("account", "42"), Row.of("admin", "30"));
        final String code = extractGeneratedCode(sql);
        assertThat(countJsonParse(code))
                .as("Two JSON_VALUE calls on the same input should parse once")
                .isOne();
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
        final String code = extractGeneratedCode(sql);
        assertThat(countJsonParse(code))
                .as("Two JSON_QUERY calls on the same input should parse once")
                .isOne();
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
        final String code = extractGeneratedCode(sql);
        assertThat(countJsonParse(code))
                .as("JSON_VALUE + JSON_QUERY on the same input should parse once")
                .isOne();
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
        final String code = extractGeneratedCode(sql);
        assertThat(countJsonParse(code))
                .as("Three JSON function calls on the same input should parse once")
                .isOne();
    }

    @Test
    void testReuseSurvivesCodeSplitting() {
        // an aggressive split must still route the input into each parseJson call site
        tEnv.getConfig().set(TableConfigOptions.MAX_LENGTH_GENERATED_CODE, 1);
        final String sql =
                "SELECT JSON_VALUE(json_data, '$.type'), "
                        + "JSON_VALUE(json_data, '$.age'), "
                        + "JSON_QUERY(json_data, '$.address'), "
                        + "JSON_QUERY(json_data, '$.roles' WITH WRAPPER) FROM json_src";
        // GeneratedClass compiles splitCode, so correct results already prove reuse survives it
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("account", "42", "{\"city\":\"Munich\"}", "[[\"user\",\"viewer\"]]"),
                        Row.of("admin", "30", "{\"city\":\"Berlin\"}", "[[\"admin\"]]"));

        // assert on the split code, not getCode() (unsplit), since only splitCode is compiled
        final int maxLength = tEnv.getConfig().get(TableConfigOptions.MAX_LENGTH_GENERATED_CODE);
        final int maxMembers = tEnv.getConfig().get(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE);
        final List<String> splitCodes =
                generatedClassCodes(sql).stream()
                        .map(code -> JavaCodeSplitter.split(code, maxLength, maxMembers))
                        .collect(Collectors.toList());
        assertThat(generatedClassCodes(sql))
                .as("the aggressive limit must actually split some generated class")
                .anySatisfy(
                        code ->
                                assertThat(JavaCodeSplitter.split(code, maxLength, maxMembers))
                                        .isNotEqualTo(code));
        assertThat(splitCodes.stream().mapToInt(JsonParseReuseTest::countJsonParse).sum())
                .as("Even in the split code the input is parsed once")
                .isOne();
    }

    @Test
    void testComputedColumnInputSharesParse() {
        // calls on the same computed input (TRIM) must still share a parse
        final String sql =
                "SELECT JSON_VALUE(TRIM(json_data), '$.type'), "
                        + "JSON_QUERY(TRIM(json_data), '$.address') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("account", "{\"city\":\"Munich\"}"),
                        Row.of("admin", "{\"city\":\"Berlin\"}"));
        final String code = extractGeneratedCode(sql);
        assertThat(countJsonParse(code))
                .as("Calls on the same computed input should parse once")
                .isOne();
    }

    @Test
    void testFirstCallWithNullArgumentStillParses() {
        // first call's args are null so its result is NULL, but the parse must still happen
        final String sql =
                "SELECT JSON_VALUE(json_data, CAST(NULL AS STRING)), "
                        + "JSON_QUERY(json_data, '$.address') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(null, "{\"city\":\"Munich\"}"),
                        Row.of(null, "{\"city\":\"Berlin\"}"));
        final String code = extractGeneratedCode(sql);
        assertThat(countJsonParse(code))
                .as("JSON_VALUE + JSON_QUERY on the same input should parse once")
                .isOne();
    }

    @Test
    void testFirstCallInsideNotTakenBranchStillParses() {
        // The first JSON call owns the parse but sits in a CASE branch that is never taken.
        final String sql =
                "SELECT CASE WHEN json_data IS NULL "
                        + "THEN JSON_VALUE(json_data, '$.type') ELSE 'fallback' END, "
                        + "JSON_QUERY(json_data, '$.address') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("fallback", "{\"city\":\"Munich\"}"),
                        Row.of("fallback", "{\"city\":\"Berlin\"}"));
    }

    @Test
    void testCallInsideSurvivingBranchSharesWithCallOutside() {
        // branch guarded by an unrelated column survives the optimizer, yet both rows share a parse
        final String sql =
                "SELECT CASE WHEN CHARACTER_LENGTH(other_json) > 3 "
                        + "THEN JSON_VALUE(json_data, '$.type') ELSE 'fb' END, "
                        + "JSON_QUERY(json_data, '$.address') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("fb", "{\"city\":\"Munich\"}"),
                        Row.of("admin", "{\"city\":\"Berlin\"}"));
        final String code = extractGeneratedCode(sql);
        assertThat(countJsonParse(code))
                .as("Calls on the same input share one parse even across a CASE boundary")
                .isOne();
    }

    @Test
    void testSingleJsonTypeCall() {
        final String sql = "SELECT JSON_TYPE(json_data) FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("object"), Row.of("object"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("A single JSON_TYPE call should parse once")
                .isOne();
    }

    @Test
    void testTwoJsonTypeCalls() {
        final String sql = "SELECT JSON_TYPE(json_data), JSON_TYPE(json_data) FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of("object", "object"), Row.of("object", "object"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("Identical JSON_TYPE calls are one expression, so they parse once")
                .isOne();
    }

    @Test
    void testJsonTypeAndJsonValueMixed() {
        final String sql =
                "SELECT JSON_VALUE(json_data, '$.type'), JSON_TYPE(json_data) FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of("account", "object"), Row.of("admin", "object"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("JSON_VALUE + JSON_TYPE on the same input should parse once")
                .isOne();
    }

    @Test
    void testJsonTypeWithJsonValueAndJsonQuery() {
        final String sql =
                "SELECT JSON_VALUE(json_data, '$.type'), "
                        + "JSON_QUERY(json_data, '$.address'), "
                        + "JSON_TYPE(json_data) FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of("account", "{\"city\":\"Munich\"}", "object"),
                        Row.of("admin", "{\"city\":\"Berlin\"}", "object"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("JSON_VALUE + JSON_QUERY + JSON_TYPE on the same input should parse once")
                .isOne();
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

    @Test
    void testReuseInOverWindowQueryIsResetPerRow() {
        // JSON scalars run in a Calc ahead of the OverAggregate; each row must get its own parse
        final TableEnvironment bEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        bEnv.createTemporaryView(
                "over_src",
                bEnv.fromValues(Row.of(1, JSON_ROW1), Row.of(2, JSON_ROW2)).as("id", "j"));
        final String sql =
                "SELECT id, "
                        + "MAX(JSON_VALUE(j, '$.type')) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW), "
                        + "MAX(JSON_QUERY(j, '$.address')) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) "
                        + "FROM over_src";
        final List<Row> rows = new ArrayList<>();
        bEnv.executeSql(sql).collect().forEachRemaining(rows::add);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "account", "{\"city\":\"Munich\"}"),
                        Row.of(2, "admin", "{\"city\":\"Berlin\"}"));
    }

    @Test
    void testFilterAndProjectionShareParse() {
        // JSON_VALUE in WHERE and JSON_QUERY in SELECT on the same input share one parse
        final String sql =
                "SELECT JSON_QUERY(json_data, '$.address') FROM json_src "
                        + "WHERE JSON_VALUE(json_data, '$.type') = 'admin'";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactly(Row.of("{\"city\":\"Berlin\"}"));
        final String code = extractGeneratedCode(sql);
        assertThat(countJsonParse(code))
                .as("Filter and projection on the same input should parse once")
                .isOne();
    }

    @Test
    void testReuseIsResetPerRowInMatchRecognize() {
        // A matches the first three rows; the per-row parse must give SUM 1+2+3, not 1+1+1
        final List<Row> data =
                Arrays.asList(
                        Row.of(1000, "{\"n\":1}", Instant.ofEpochMilli(1000L)),
                        Row.of(2000, "{\"n\":2}", Instant.ofEpochMilli(2000L)),
                        Row.of(3000, "{\"n\":3}", Instant.ofEpochMilli(3000L)),
                        Row.of(9000, "{\"n\":9}", Instant.ofEpochMilli(9000L)));
        final String dataId = TestValuesTableFactory.registerData(data);
        tEnv.executeSql(
                "CREATE TABLE events ("
                        + "  f0 INT,"
                        + "  f1 STRING,"
                        + "  ts TIMESTAMP_LTZ(3),"
                        + "  WATERMARK FOR ts AS ts"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'data-id' = '"
                        + dataId
                        + "',"
                        + "  'bounded' = 'true')");
        final String sql =
                "SELECT total FROM events MATCH_RECOGNIZE ("
                        + " ORDER BY ts"
                        + " MEASURES SUM(CAST(JSON_VALUE(A.f1, '$.n') AS INT)) AS total"
                        + " AFTER MATCH SKIP PAST LAST ROW"
                        + " PATTERN (A+ B)"
                        + " DEFINE A AS A.f0 < 9000, B AS B.f0 >= 9000)";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactly(Row.of(6));
    }

    @Test
    void testReuseIsResetPerRowInBatchFusion() {
        // a projection Calc is only fused on top of a HashJoin, so force one (disable the rest)
        final TableEnvironment bEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        bEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, true)
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "NestedLoopJoin,SortMergeJoin")
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        bEnv.createTemporaryView(
                "src", bEnv.fromValues(Row.of(1, JSON_ROW1), Row.of(2, JSON_ROW2)).as("id", "j"));
        bEnv.createTemporaryView("dim", bEnv.fromValues(Row.of(1), Row.of(2)).as("id"));
        final String sql =
                "SELECT JSON_VALUE(src.j, '$.type'), JSON_VALUE(src.j, '$.age') "
                        + "FROM src JOIN dim ON src.id = dim.id";
        final List<Row> rows = new ArrayList<>();
        bEnv.executeSql(sql).collect().forEachRemaining(rows::add);
        assertThat(rows).containsExactlyInAnyOrder(Row.of("account", "42"), Row.of("admin", "30"));
    }

    @Test
    void testTwoJsonLengthCalls() {
        final String sql =
                "SELECT JSON_LENGTH(json_data), JSON_LENGTH(json_data, '$.address') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(4, 1), Row.of(4, 1));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("Two JSON_LENGTH calls on the same input should parse once")
                .isOne();
    }

    @Test
    void testJsonLengthAndJsonValueMixed() {
        final String sql =
                "SELECT JSON_LENGTH(json_data), JSON_VALUE(json_data, '$.type') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(4, "account"), Row.of(4, "admin"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("JSON_LENGTH + JSON_VALUE on the same input should parse once")
                .isOne();
    }

    @Test
    void testJsonLengthAndJsonQueryMixed() {
        final String sql =
                "SELECT JSON_LENGTH(json_data), JSON_QUERY(json_data, '$.address') FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(4, "{\"city\":\"Munich\"}"), Row.of(4, "{\"city\":\"Berlin\"}"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("JSON_LENGTH + JSON_QUERY on the same input should parse once")
                .isOne();
    }

    @Test
    void testJsonLengthAndJsonTypeMixed() {
        final String sql =
                "SELECT JSON_LENGTH(json_data, '$.roles'), JSON_TYPE(json_data, '$.age') "
                        + "FROM json_src";
        final List<Row> rows = collect(sql);
        assertThat(rows).containsExactlyInAnyOrder(Row.of(2, "number"), Row.of(1, "number"));
        assertThat(countJsonParse(extractGeneratedCode(sql)))
                .as("JSON_LENGTH + JSON_TYPE on the same input should parse once")
                .isOne();
    }
}
