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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.codesplit.JavaCodeSplitter;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.runtime.functions.SqlXmlUtils;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that the {@code PARSE_XML} and {@code TRY_PARSE_XML} calls on the same input share it. */
class XmlReadReuseTest {

    private static final Pattern READ_PATTERN =
            Pattern.compile(Pattern.quote(SqlXmlUtils.class.getCanonicalName() + ".read("));
    private static final Pattern PARSE_XML_PATTERN =
            Pattern.compile(Pattern.quote(SqlXmlUtils.class.getCanonicalName() + ".parseXml("));

    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.inStreamingMode());
        tEnv.createTemporaryView(
                "xml_src",
                tEnv.fromValues(
                                Row.of(
                                        "<book><title>Dune</title></book>",
                                        "<a>1</a>",
                                        "{\"a\":1}",
                                        "<book>"),
                                Row.of(
                                        "<book><title>Emma</title></book>",
                                        "<a>2</a>",
                                        "<a/>",
                                        "<a>"))
                        .as("xml", "other_xml", "json", "invalid_xml"));
    }

    @Test
    void testCallsOnTheSameInputReadItOnce() {
        final String sql =
                "SELECT CAST(PARSE_XML(xml)['book']['title'] AS STRING), "
                        + "JSON_STRING(PARSE_XML(xml, TRUE)), "
                        + "JSON_STRING(TRY_PARSE_XML(xml, TRUE)) FROM xml_src";
        assertThat(collect(sql))
                .containsExactlyInAnyOrder(
                        Row.of(
                                "Dune",
                                "{\"book\":{\"title\":[\"Dune\"]}}",
                                "{\"book\":{\"title\":[\"Dune\"]}}"),
                        Row.of(
                                "Emma",
                                "{\"book\":{\"title\":[\"Emma\"]}}",
                                "{\"book\":{\"title\":[\"Emma\"]}}"));
        assertThat(countReads(sql)).isOne();
    }

    @Test
    void testCallsOnDifferentInputsReadEachOnce() {
        final String sql =
                "SELECT JSON_STRING(PARSE_XML(xml)), JSON_STRING(TRY_PARSE_XML(xml, TRUE)), "
                        + "JSON_STRING(PARSE_XML(other_xml)), "
                        + "JSON_STRING(TRY_PARSE_XML(other_xml, TRUE)) FROM xml_src";
        assertThat(collect(sql))
                .containsExactlyInAnyOrder(
                        Row.of(
                                "{\"book\":{\"title\":\"Dune\"}}",
                                "{\"book\":{\"title\":[\"Dune\"]}}",
                                "{\"a\":\"1\"}",
                                "{\"a\":\"1\"}"),
                        Row.of(
                                "{\"book\":{\"title\":\"Emma\"}}",
                                "{\"book\":{\"title\":[\"Emma\"]}}",
                                "{\"a\":\"2\"}",
                                "{\"a\":\"2\"}"));
        assertThat(countReads(sql)).isEqualTo(2);
    }

    @Test
    void testFirstCallInBranchThatIsNotTaken() {
        // The first call is never taken, so the second one has to read the document.
        final String sql =
                "SELECT CASE WHEN CHARACTER_LENGTH(other_xml) > 100 "
                        + "THEN CAST(PARSE_XML(xml)['book']['title'] AS STRING) ELSE 'none' END, "
                        + "CAST(PARSE_XML(xml, TRUE)['book']['title'][1] AS STRING) FROM xml_src";
        assertThat(collect(sql))
                .containsExactlyInAnyOrder(Row.of("none", "Dune"), Row.of("none", "Emma"));
        assertThat(countCalls(sql, PARSE_XML_PATTERN))
                .as("the branch with the first call must survive the optimizer")
                .isEqualTo(2);
        assertThat(countReads(sql)).isOne();
    }

    @Test
    void testInvalidDocumentIsReadOnce() {
        final String sql =
                "SELECT JSON_STRING(TRY_PARSE_XML(invalid_xml)), "
                        + "JSON_STRING(TRY_PARSE_XML(invalid_xml, TRUE)) FROM xml_src";
        assertThat(collect(sql)).containsExactlyInAnyOrder(Row.of(null, null), Row.of(null, null));
        assertThat(countReads(sql)).isOne();
    }

    @Test
    void testJsonFunctionOnTheSameInput() {
        // The JSON functions share their parsed input as well, which must not be mixed up.
        final String sql =
                "SELECT JSON_VALUE(json, '$.a'), JSON_STRING(TRY_PARSE_XML(json)) FROM xml_src";
        assertThat(collect(sql))
                .containsExactlyInAnyOrder(Row.of("1", null), Row.of(null, "{\"a\":\"\"}"));
        assertThat(countReads(sql)).isOne();
    }

    @Test
    void testReuseSurvivesCodeSplitting() {
        tEnv.getConfig()
                .set(TableConfigOptions.MAX_LENGTH_GENERATED_CODE, 1)
                .set(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE, 1);
        final String sql =
                "SELECT CAST(PARSE_XML(xml)['book']['title'] AS STRING), "
                        + "JSON_STRING(PARSE_XML(xml, TRUE)), "
                        + "JSON_STRING(TRY_PARSE_XML(xml, TRUE)) FROM xml_src";
        // Only the split code is compiled, so correct results show that the reuse survives it.
        assertThat(collect(sql))
                .containsExactlyInAnyOrder(
                        Row.of(
                                "Dune",
                                "{\"book\":{\"title\":[\"Dune\"]}}",
                                "{\"book\":{\"title\":[\"Dune\"]}}"),
                        Row.of(
                                "Emma",
                                "{\"book\":{\"title\":[\"Emma\"]}}",
                                "{\"book\":{\"title\":[\"Emma\"]}}"));
        final List<String> splitCodes =
                GeneratedCodeTestUtils.generatedClassCodes(tEnv, sql).stream()
                        .map(code -> JavaCodeSplitter.split(code, 1, 1))
                        .collect(Collectors.toList());
        assertThat(GeneratedCodeTestUtils.generatedClassCodes(tEnv, sql))
                .as("the limits must actually split a generated class")
                .anySatisfy(
                        code -> assertThat(JavaCodeSplitter.split(code, 1, 1)).isNotEqualTo(code));
        assertThat(
                        splitCodes.stream()
                                .mapToInt(
                                        code ->
                                                GeneratedCodeTestUtils.countMatches(
                                                        READ_PATTERN, code))
                                .sum())
                .isOne();
    }

    @Test
    void testDocumentIsReadForEachRowInMatchRecognize() {
        // A matches the first three rows, so a read for each row gives 1 + 2 + 3.
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(1000, "<n>1</n>", Instant.ofEpochMilli(1000L)),
                                Row.of(2000, "<n>2</n>", Instant.ofEpochMilli(2000L)),
                                Row.of(3000, "<n>3</n>", Instant.ofEpochMilli(3000L)),
                                Row.of(9000, "<n>9</n>", Instant.ofEpochMilli(9000L))));
        tEnv.executeSql(
                "CREATE TABLE events (f0 INT, f1 STRING, ts TIMESTAMP_LTZ(3), "
                        + "WATERMARK FOR ts AS ts) WITH ('connector' = 'values', 'data-id' = '"
                        + dataId
                        + "', 'bounded' = 'true')");
        final String sql =
                "SELECT total FROM events MATCH_RECOGNIZE ("
                        + " ORDER BY ts"
                        + " MEASURES SUM(CAST(CAST(PARSE_XML(A.f1)['n'] AS STRING) AS INT)) AS total"
                        + " AFTER MATCH SKIP PAST LAST ROW"
                        + " PATTERN (A+ B)"
                        + " DEFINE A AS A.f0 < 9000, B AS B.f0 >= 9000)";
        assertThat(collect(sql)).containsExactly(Row.of(6));
    }

    @Test
    void testCallsInFusedOperators() {
        // The join and the projection above it are fused into one class, and both call PARSE_XML.
        // Both rows reach the projection, so a document kept from the previous row would show.
        final TableEnvironment bEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        bEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, true)
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "NestedLoopJoin,SortMergeJoin")
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        bEnv.createTemporaryView(
                "src",
                bEnv.fromValues(
                                Row.of(1, "<book><title>Dune</title></book>"),
                                Row.of(2, "<book><title>Emma</title></book>"))
                        .as("id", "xml"));
        // Different values, so that the planner can't push the join condition below the join.
        bEnv.createTemporaryView(
                "dim", bEnv.fromValues(Row.of(1, 0), Row.of(2, 1)).as("id", "min_length"));
        final String sql =
                "SELECT JSON_STRING(PARSE_XML(src.xml, TRUE)) FROM src JOIN dim "
                        + "ON src.id = dim.id "
                        + "AND CHARACTER_LENGTH(JSON_STRING(PARSE_XML(src.xml))) > dim.min_length";
        assertThat(GeneratedCodeTestUtils.collect(bEnv, sql))
                .containsExactlyInAnyOrder(
                        Row.of("{\"book\":{\"title\":[\"Dune\"]}}"),
                        Row.of("{\"book\":{\"title\":[\"Emma\"]}}"));
    }

    private int countCalls(String sql, Pattern pattern) {
        return GeneratedCodeTestUtils.generatedClassCodes(tEnv, sql).stream()
                .mapToInt(code -> GeneratedCodeTestUtils.countMatches(pattern, code))
                .sum();
    }

    private List<Row> collect(String sql) {
        return GeneratedCodeTestUtils.collect(tEnv, sql);
    }

    private int countReads(String sql) {
        return countCalls(sql, READ_PATTERN);
    }
}
