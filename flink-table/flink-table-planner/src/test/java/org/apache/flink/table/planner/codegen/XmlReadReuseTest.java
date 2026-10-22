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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.functions.SqlXmlUtils;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

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
                                Row.of("<book><title>Dune</title></book>", "<a>1</a>"),
                                Row.of("<book><title>Emma</title></book>", "<a>2</a>"))
                        .as("xml", "other_xml"));
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
    void testFilterAndProjectionReadOnce() {
        final String sql =
                "SELECT CAST(PARSE_XML(xml)['book']['title'] AS STRING) FROM xml_src "
                        + "WHERE CAST(PARSE_XML(xml, TRUE)['book']['title'][1] AS STRING) <> 'Emma'";
        assertThat(collect(sql)).containsExactly(Row.of("Dune"));
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
