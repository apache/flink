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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ParseErrorSnippet}. */
class ParseErrorSnippetTest {

    /** Longer than the 76-character window. */
    private static final String LONG_LINE = "SELECT " + "a".repeat(100) + " FROM t";

    @Test
    void testPointAndSpan() {
        assertThat(render("SELECT a FROM t", new SqlParserPos(1, 8)))
                .contains("    SELECT a FROM t\n           ^");
        assertThat(render("SELECT a FROM t", new SqlParserPos(1, 10, 1, 13)))
                .contains("    SELECT a FROM t\n             ^^^^");
    }

    @Test
    void testLineOfAMultiLineText() {
        assertThat(render("SELECT a\r\nFROM t\r\nWHERE x", new SqlParserPos(2, 6)))
                .contains("    FROM t\n         ^");
        assertThat(render("SELECT a\nFROM t\nWHERE x", new SqlParserPos(3, 1, 3, 5)))
                .contains("    WHERE x\n    ^^^^^");
        assertThat(render("SELECT a\rFROM t\rWHERE x", new SqlParserPos(3, 1, 3, 5)))
                .contains("    WHERE x\n    ^^^^^");
    }

    @Test
    void testSpanAcrossLinesEndsAtTheLineEnd() {
        assertThat(render("SELECT a +\n  b FROM t", new SqlParserPos(1, 8, 2, 3)))
                .contains("    SELECT a +\n           ^^^");
    }

    @Test
    void testTrailingWhitespaceIsKeptUpToTheColumn() {
        assertThat(render("SELECT a   ", new SqlParserPos(1, 11)))
                .contains("    SELECT a  \n              ^");
        assertThat(render("SELECT a   ", new SqlParserPos(1, 8)))
                .contains("    SELECT a\n           ^");
    }

    @Test
    void testUnknownOrMissingLineRendersNothing() {
        assertThat(render("SELECT a", SqlParserPos.ZERO)).isEmpty();
        assertThat(render("SELECT a", new SqlParserPos(2, 1))).isEmpty();
        assertThat(render("SELECT a\n", new SqlParserPos(3, 1))).isEmpty();
    }

    @Test
    void testTabIsOneColumn() {
        assertThat(render("\tSELECT\ta", new SqlParserPos(1, 9)))
                .contains("     SELECT a\n            ^");
    }

    @Test
    void testWideCharactersTakeTwoCellsBeforeTheCaret() {
        assertThat(render("SELECT '日本語' UNION foo_bar", new SqlParserPos(1, 20, 1, 26)))
                .contains("    SELECT '日本語' UNION foo_bar\n" + " ".repeat(26) + "^^^^^^^");
        assertThat(render("SELECT 'é' UNION foo_bar", new SqlParserPos(1, 19, 1, 25)))
                .contains("    SELECT 'é' UNION foo_bar\n" + " ".repeat(21) + "^^^^^^^");
        assertThat(render("SELECT '日本' UNION foo", new SqlParserPos(1, 9, 1, 10)))
                .contains("    SELECT '日本' UNION foo\n            ^^^^");
    }

    @Test
    void testLongLineKeepsItsStartWhenTheSpanIsThere() {
        assertThat(render(LONG_LINE, new SqlParserPos(1, 1, 1, 6)))
                .contains("    SELECT " + "a".repeat(66) + "...\n    ^^^^^^");
    }

    @Test
    void testLongLineKeepsItsEndWhenTheSpanIsThere() {
        assertThat(render(LONG_LINE, new SqlParserPos(1, 109, 1, 112)))
                .contains("    ..." + "a".repeat(66) + " FROM t\n    " + " ".repeat(70) + "^^^^");
    }

    @Test
    void testLongLineKeepsTenCharactersAfterTheSpanStart() {
        assertThat(render(LONG_LINE, new SqlParserPos(1, 81, 1, 83)))
                .contains("    ..." + "a".repeat(70) + "...\n    " + " ".repeat(62) + "^^^");
    }

    @Test
    void testCaretPastTheEndOfALongLineStaysWithinTheWindow() {
        assertThat(render("x".repeat(76), new SqlParserPos(1, 77)))
                .contains("    ..." + "x".repeat(72) + "\n    " + " ".repeat(75) + "^");
    }

    @Test
    void testWindowIsMeasuredInCells() {
        assertThat(render("日".repeat(100), new SqlParserPos(1, 90)))
                .contains("    ..." + "日".repeat(34) + "...\n    " + " ".repeat(61) + "^^");
        assertThat(render("😀".repeat(100), new SqlParserPos(1, 99, 1, 100)))
                .contains("    ..." + "😀".repeat(34) + "...\n    " + " ".repeat(61) + "^^");
    }

    @Test
    void testSpanIsCutWithTheLine() {
        assertThat(render(LONG_LINE, new SqlParserPos(1, 1, 1, 100)))
                .contains("    SELECT " + "a".repeat(66) + "...\n    " + "^".repeat(73));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(
            delimiter = '|',
            quoteCharacter = '"',
            value = {
                "quoted pairs|WITH ('connector' = 'kafka', 'PASSWORD'='hunter2')|WITH ('connector' = 'kafka', 'PASSWORD'='*******')",
                "doubled quote in the value|WITH ('a.secret.b' =  'it''s')|WITH ('a.secret.b' =  '*****')",
                "bare option key|SET s3.secret-key=abc, a=b|SET s3.secret-key=***, a=b",
                "colon instead of equals|WITH ('password': 'hunter2')|WITH ('password': '*******')",
                "arrow instead of equals|WITH ('password' => 'hunter2')|WITH ('password' => '*******')",
                "no separator|WITH ('password' 'hunter2')|WITH ('password' '*******')",
                "unterminated value|WITH ('password' = 'hun|WITH ('password' = '***",
                "value quoted after a bare key|SET security.token='abc'|SET security.token='***'",
                "double-quoted key|WITH (\"password\" = 'hunter2')|WITH (\"password\" = '*******')",
                "back-quoted key and double-quoted value|WITH (`password` = \"hunter2\")|WITH (`password` = \"*******\")",
                "long separator|WITH ('password' ==== 'hunter2')|WITH ('password' ==== '*******')",
                "pair after a comment marker|SELECT 1 -- 'password' = 'hunter2'|SELECT 1 -- 'password' = '*******'",
                "plain identifier is not a key|SELECT * FROM t WHERE token_count=5|SELECT * FROM t WHERE token_count=5",
                "quoted literal without a value|SELECT 'password' FROM t|SELECT 'password' FROM t",
                "non-sensitive pairs|WITH ('url' = 'jdbc:x', 'a-b' = 'c')|WITH ('url' = 'jdbc:x', 'a-b' = 'c')",
            })
    void testSensitiveOptionValuesAreMasked(String name, String line, String expected) {
        assertThat(render(line, new SqlParserPos(1, 1))).contains("    " + expected + "\n    ^");
    }

    @Test
    void testAdditionalSensitiveKeysAreMasked() {
        final String line = "WITH ('my.custom' = 'x', 'other' = 'y')";
        assertThat(ParseErrorSnippet.render(line, new SqlParserPos(1, 1), List.of("custom")))
                .contains("    WITH ('my.custom' = '*', 'other' = 'y')\n    ^");
    }

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void testLongLinesRenderInLinearTime() {
        assertThat(render("ELECT " + "a".repeat(1_000_000), new SqlParserPos(1, 1, 1, 5)))
                .isPresent();
        assertThat(render("x '" + "a".repeat(1_000_000) + "'", new SqlParserPos(1, 1))).isPresent();
        assertThat(
                        render(
                                "WITH ('password' = '" + "a".repeat(1_000_000) + "') 1",
                                new SqlParserPos(1, 1)))
                .isPresent();
    }

    private static Optional<String> render(String sql, SqlParserPos pos) {
        return ParseErrorSnippet.render(sql, pos, List.of());
    }
}
