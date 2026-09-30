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

package org.apache.flink.table.planner.parse;

import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.utils.PlannerMocks;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the exception conversion in {@link CalciteParser}. */
class CalciteParserTest {

    private static final String NON_QUERY = "Non-query expression encountered in illegal context";

    private static final SqlParser.Config CONFIG =
            SqlParser.config()
                    .withParserFactory(FlinkSqlParserImpl.FACTORY)
                    .withConformance(FlinkSqlConformance.DEFAULT)
                    .withLex(Lex.JAVA)
                    .withIdentifierMaxLength(256);

    private final CalciteParser parser = new CalciteParser(CONFIG, List.of());

    /**
     * Errors that the grammar raises through {@code SqlUtil.newContextException}. Calcite keeps
     * their position only in {@code SqlParseException#getPos()}, so the message has to name it and
     * show the line.
     */
    private static Stream<Arguments> statementsWithContextErrors() {
        return Stream.of(
                Arguments.of(
                        "misspelled keyword",
                        "ELECT 1",
                        NON_QUERY
                                + ". From line 1, column 1 to line 1, column 5:\n"
                                + "    ELECT 1\n"
                                + "    ^^^^^"),
                Arguments.of(
                        "non-query INSERT body",
                        "INSERT INTO t 1",
                        NON_QUERY
                                + ". At line 1, column 15:\n"
                                + "    INSERT INTO t 1\n"
                                + "                  ^"),
                Arguments.of(
                        "non-query UNION operand",
                        "SELECT a,\n  b\nFROM t\nUNION\n  foo_bar_baz",
                        NON_QUERY
                                + ". From line 5, column 3 to line 5, column 13:\n"
                                + "      foo_bar_baz\n"
                                + "      ^^^^^^^^^^^"),
                Arguments.of(
                        "leading block comment",
                        "/*\ncomment\n*/  ELECT 1",
                        NON_QUERY
                                + ". From line 3, column 5 to line 3, column 9:\n"
                                + "    */  ELECT 1\n"
                                + "        ^^^^^"),
                Arguments.of(
                        "CRLF line breaks",
                        "SELECT 1\r\nFROM t\r\nUNION\r\nfoo_bar",
                        NON_QUERY
                                + ". From line 4, column 1 to line 4, column 7:\n"
                                + "    foo_bar\n"
                                + "    ^^^^^^^"),
                Arguments.of(
                        "CRLF before the next line",
                        "SELECT 1\r\nUNION\r\nfoo_bar\r\nUNION\r\nSELECT 2",
                        NON_QUERY
                                + ". From line 3, column 1 to line 3, column 7:\n"
                                + "    foo_bar\n"
                                + "    ^^^^^^^"),
                Arguments.of(
                        "CR line breaks",
                        "SELECT 1\rUNION\rfoo_bar",
                        NON_QUERY
                                + ". From line 3, column 1 to line 3, column 7:\n"
                                + "    foo_bar\n"
                                + "    ^^^^^^^"),
                Arguments.of(
                        "tab counts as one column",
                        "SELECT\t1\tUNION\tfoo",
                        NON_QUERY
                                + ". From line 1, column 16 to line 1, column 18:\n"
                                + "    SELECT 1 UNION foo\n"
                                + "                   ^^^"),
                Arguments.of(
                        "expected query or join",
                        "SELECT * FROM (t)",
                        "Expected query or join. At line 1, column 16:\n"
                                + "    SELECT * FROM (t)\n"
                                + "                   ^"),
                Arguments.of(
                        "Flink grammar extension",
                        "CREATE SYSTEM CONNECTION c WITH ('a'='b')",
                        "CREATE SYSTEM CONNECTION is not"
                                + " supported, system connections can only be registered as"
                                + " temporary connections, you can use CREATE TEMPORARY SYSTEM"
                                + " CONNECTION instead."
                                + " From line 1, column 8 to line 1, column 13:\n"
                                + "    CREATE SYSTEM CONNECTION c WITH ('a'='b')\n"
                                + "           ^^^^^^"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("statementsWithContextErrors")
    void testStatementErrorShowsTheLine(String name, String sql, String expectedMessage) {
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(SqlParserException.class)
                .hasMessage("SQL parse failed. " + expectedMessage);
        assertThatThrownBy(() -> parser.parseSqlList(sql))
                .isInstanceOf(SqlParserException.class)
                .hasMessage("SQL parse failed. " + expectedMessage);
    }

    @Test
    void testStatementListErrorShowsTheLineOfTheScript() {
        final String script =
                IntStream.rangeClosed(1, 18)
                                .mapToObj(i -> "-- line " + i + "\n")
                                .collect(Collectors.joining())
                        + "SELECT 1;\n  SELCT 2;";
        assertThatThrownBy(() -> parser.parseSqlList(script))
                .isInstanceOf(SqlParserException.class)
                .hasMessage(
                        "SQL parse failed. "
                                + NON_QUERY
                                + ". From line 20, column 3 to line 20, column 7:\n"
                                + "      SELCT 2;\n"
                                + "      ^^^^^");
    }

    @Test
    void testExpressionErrorShowsTheLine() {
        assertThatThrownBy(() -> parser.parseExpression("x IN (SELECT 1 UNION 2)"))
                .isInstanceOf(SqlParserException.class)
                .hasMessage(
                        "SQL parse failed. "
                                + NON_QUERY
                                + ". At line 1, column 22:\n"
                                + "    x IN (SELECT 1 UNION 2)\n"
                                + "                         ^");
    }

    /** JavaCC names the position itself; the line goes before its token list. */
    @Test
    void testSyntaxErrorShowsTheLineBeforeTheExpectedTokens() {
        assertThatThrownBy(() -> parser.parse("SELECT 1; -- x"))
                .isInstanceOf(SqlParserException.class)
                .hasMessageStartingWith(
                        "SQL parse failed. Encountered \";\" at line 1, column 9:\n"
                                + "    SELECT 1; -- x\n"
                                + "            ^\n"
                                + "Was expecting one of:\n");
    }

    @Test
    void testKeywordErrorShowsTheLine() {
        assertThatThrownBy(() -> parser.parse("SELECT FROM t"))
                .isInstanceOf(SqlParserException.class)
                .hasMessageStartingWith(
                        "SQL parse failed. Incorrect syntax near the keyword 'FROM' at line 1, column 8:\n"
                                + "    SELECT FROM t\n"
                                + "           ^^^^\n"
                                + "Was expecting one of:\n");
    }

    /** The lexer stops after the last character here, so the caret sits behind the line. */
    @Test
    void testLexicalErrorShowsTheLine() {
        assertThatThrownBy(() -> parser.parse("SELECT 1 #"))
                .isInstanceOf(SqlParserException.class)
                .hasMessageStartingWith("SQL parse failed. Lexical error at line 1, column 11.")
                .hasMessageEndingWith(":\n" + "    SELECT 1 #\n" + "              ^");
    }

    @Test
    void testLongLineIsCutAfterTheError() {
        final String sql = "ELECT 1 FROM " + "a".repeat(100);
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(SqlParserException.class)
                .hasMessage(
                        "SQL parse failed. "
                                + NON_QUERY
                                + ". From line 1, column 1 to line 1, column 5:\n"
                                + "    ELECT 1 FROM "
                                + "a".repeat(60)
                                + "...\n"
                                + "    ^^^^^");
    }

    @Test
    void testLongLineIsCutBeforeTheError() {
        final String sql = "SELECT " + "a".repeat(100) + " FROM t UNION foo_bar";
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(SqlParserException.class)
                .hasMessage(
                        "SQL parse failed. "
                                + NON_QUERY
                                + ". From line 1, column 122 to line 1, column 128:\n"
                                + "    ..."
                                + "a".repeat(52)
                                + " FROM t UNION foo_bar\n"
                                + "    "
                                + " ".repeat(69)
                                + "^^^^^^^");
    }

    @Test
    void testLongLineIsCutOnBothSidesOfTheError() {
        final String sql = "SELECT " + "a".repeat(100) + " UNION foo_bar UNION " + "b".repeat(100);
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(SqlParserException.class)
                .hasMessage(
                        "SQL parse failed. "
                                + NON_QUERY
                                + ". From line 1, column 115 to line 1, column 121:\n"
                                + "    ..."
                                + "a".repeat(52)
                                + " UNION foo_bar UNI...\n"
                                + "    "
                                + " ".repeat(62)
                                + "^^^^^^^");
    }

    @Test
    void testSensitiveOptionValuesAreMaskedInTheLine() {
        assertThatThrownBy(
                        () ->
                                parser.parse(
                                        "CREATE TABLE t (a INT) WITH ('password' = 'hunter2') 1"))
                .isInstanceOf(SqlParserException.class)
                .hasMessageStartingWith(
                        "SQL parse failed. Encountered \"1\" at line 1, column 54:\n"
                                + "    CREATE TABLE t (a INT) WITH ('password' = '*******') 1\n"
                                + "                                                         ^\n")
                .message()
                .doesNotContain("hunter2");
    }

    @Test
    void testSensitiveValueOfAnUnquotedOptionIsMasked() {
        assertThatThrownBy(() -> parser.parse("SET s3.secret-key=abc"))
                .isInstanceOf(SqlParserException.class)
                .hasMessageStartingWith(
                        "SQL parse failed. Encountered \"s3\" at line 1, column 5:\n"
                                + "    SET s3.secret-key=***\n"
                                + "        ^^\n")
                .message()
                .doesNotContain("abc");
    }

    @Test
    void testAdditionalSensitiveKeysAreMasked() {
        final CalciteParser masking = new CalciteParser(CONFIG, List.of("hunter"));
        assertThatThrownBy(
                        () ->
                                masking.parse(
                                        "CREATE TABLE t (a INT) WITH ('my.hunter.opt' = 'x') 1"))
                .isInstanceOf(SqlParserException.class)
                .message()
                .contains("    CREATE TABLE t (a INT) WITH ('my.hunter.opt' = '*') 1\n")
                .doesNotContain("'x'");
    }

    /** The Calcite text stays in the message, only its headline gains the echoed line. */
    @Test
    void testMessageKeepsTheParserText() {
        for (String sql : new String[] {"ELECT 1", "SELECT 1; -- x", "SELECT 1 #"}) {
            assertThatThrownBy(() -> parser.parse(sql))
                    .isInstanceOf(SqlParserException.class)
                    .satisfies(
                            e -> {
                                final String[] cause = e.getCause().getMessage().split("\n", 2);
                                assertThat(e.getMessage())
                                        .contains(cause[0].replaceAll("\\.$", ""));
                                if (cause.length > 1) {
                                    assertThat(e.getMessage()).endsWith(cause[1]);
                                }
                            });
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedOptionPairs")
    void testSensitiveValueIsMaskedWhateverBrokeThePair(String name, String sql, String line) {
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(SqlParserException.class)
                .message()
                .contains("\n    " + line + "\n")
                .doesNotContain("hunter2");
    }

    private static Stream<Arguments> malformedOptionPairs() {
        return Stream.of(
                Arguments.of(
                        "colon",
                        "CREATE TABLE t (a INT) WITH ('password': 'hunter2')",
                        "CREATE TABLE t (a INT) WITH ('password': '*******')"),
                Arguments.of(
                        "arrow",
                        "CREATE TABLE t (a INT) WITH ('password' => 'hunter2')",
                        "CREATE TABLE t (a INT) WITH ('password' => '*******')"),
                Arguments.of(
                        "missing equals",
                        "CREATE TABLE t (a INT) WITH ('password' 'hunter2')",
                        "CREATE TABLE t (a INT) WITH ('password' '*******')"),
                Arguments.of(
                        "unterminated value",
                        "CREATE TABLE t (a INT) WITH ('password' = 'hunter2",
                        "CREATE TABLE t (a INT) WITH ('password' = '*******"),
                Arguments.of(
                        "double-quoted key",
                        "CREATE TABLE t (a INT) WITH (\"password\" = 'hunter2') 1",
                        "CREATE TABLE t (a INT) WITH (\"password\" = '*******') 1"),
                Arguments.of(
                        "pair in a trailing comment",
                        "SELECT 1 UNION foo_bar -- 'password' = 'hunter2'",
                        "SELECT 1 UNION foo_bar -- 'password' = '*******'"));
    }

    @Test
    void testPlainIdentifiersAreNotMasked() {
        assertThatThrownBy(() -> parser.parse("SELECT * FROM t WHERE token_count=5 UNION foo_bar"))
                .isInstanceOf(SqlParserException.class)
                .message()
                .contains("    SELECT * FROM t WHERE token_count=5 UNION foo_bar\n");
    }

    @Test
    void testExpressionParserOfThePlannerHonorsAdditionalSensitiveKeys() {
        final TableConfig config = TableConfig.getDefault();
        config.set(SecurityOptions.ADDITIONAL_SENSITIVE_KEYS, List.of("hunter"));
        final FlinkPlannerImpl planner =
                PlannerMocks.newBuilder()
                        .withTableConfig(config)
                        .build()
                        .getPlannerContext()
                        .createFlinkPlanner();
        assertThatThrownBy(() -> planner.parser().parseExpression("f('my.hunter.opt' = 'x' 1)"))
                .isInstanceOf(SqlParserException.class)
                .message()
                .contains("    f('my.hunter.opt' = '*' 1)\n")
                .doesNotContain("'x'");
    }

    @Test
    void testLongInputsStillProduceAParserException() {
        assertThatThrownBy(() -> parser.parse("ELECT " + "a".repeat(100_000)))
                .isInstanceOf(SqlParserException.class);
        assertThatThrownBy(
                        () ->
                                parser.parse(
                                        "CREATE TABLE t (a INT) WITH ('format' = '"
                                                + "a".repeat(20_000)
                                                + "') 1"))
                .isInstanceOf(SqlParserException.class);
    }

    @Test
    void testErrorWithoutPositionIsUnchanged() {
        assertThatThrownBy(() -> parser.parse("show databases in db.t"))
                .isInstanceOf(SqlParserException.class)
                .hasMessage(
                        "SQL parse failed. Show databases from/in identifier [ db.t ] format"
                                + " error, catalog must be a single part identifier.");
    }

    @Test
    void testIncompleteStatementIsReportedAsEof() {
        assertThatThrownBy(() -> parser.parse("SELECT a FROM t WHERE"))
                .isInstanceOf(SqlParserEOFException.class)
                .hasMessageStartingWith("Encountered \"<EOF>\" at line 1, column 21.");
        assertThatThrownBy(() -> parser.parseSqlList("SELECT a FROM t WHERE"))
                .isInstanceOf(SqlParserEOFException.class)
                .hasMessageStartingWith("Encountered \"<EOF>\" at line 1, column 21.");
    }

    @Test
    void testIncompleteExpressionIsNotReportedAsEof() {
        assertThatThrownBy(() -> parser.parseExpression("a +"))
                .isExactlyInstanceOf(SqlParserException.class)
                .hasMessageStartingWith(
                        "SQL parse failed. Encountered \"+ <EOF>\" at line 1, column 3:\n"
                                + "    a +\n"
                                + "      ^\n");
    }
}
