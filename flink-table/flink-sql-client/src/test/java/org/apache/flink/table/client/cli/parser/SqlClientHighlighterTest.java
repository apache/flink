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

package org.apache.flink.table.client.cli.parser;

import org.apache.flink.table.api.SqlDialect;

import org.jline.utils.AttributedStringBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.table.client.cli.parser.SqlClientHighlighterTest.AttributedStringTestSpecBuilder.withStyle;
import static org.apache.flink.table.client.cli.parser.SqlClientHighlighterTest.SqlClientHighlighterTestSpec.forSql;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SqlClientSyntaxHighlighter}. */
class SqlClientHighlighterTest {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "select",
                "join",
                "match_recognize",
                "Select",
                "wHeRe",
                "FroM",
                "view",
                "temporary"
            })
    void keywordsTest(String keyword) {
        applyTestFor(AttributedStringTestSpecBuilder::appendKeyword, keyword, null);
        applyTestFor(AttributedStringTestSpecBuilder::appendKeyword, keyword, SqlDialect.HIVE);
        applyTestFor(AttributedStringTestSpecBuilder::appendKeyword, keyword, SqlDialect.DEFAULT);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "-- one line comment",
                "--select",
                "/* \"''\"",
                "/*",
                "/*/*/",
                "/*/ this is a comment",
                "--",
                "--\n/*",
                "/* hello\n'wor'ld*/",
                "/*\"-- \"values*/",
                "/*\"--;\n FROM*/",
                "/*SELECT'''a\n'AS\n`````b``c`\nFROM t*/"
            })
    void commentsTest(String comment) {
        applyTestFor(AttributedStringTestSpecBuilder::appendComment, comment, null);
        applyTestFor(AttributedStringTestSpecBuilder::appendComment, comment, SqlDialect.HIVE);
        applyTestFor(AttributedStringTestSpecBuilder::appendComment, comment, SqlDialect.DEFAULT);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "/*+ this is a hint*/",
                "/*+ select*/",
                "/*+ \"''\"*/",
                "/*+",
                "/*+/ this is a part of a hint",
                "/*+ hello\n'wor'ld*/",
                "/*+\"-- \"values*/",
                "/*+\"--;\n FROM*/"
            })
    void hintsTest(String hint) {
        applyTestFor(AttributedStringTestSpecBuilder::appendHint, hint, null);
        applyTestFor(AttributedStringTestSpecBuilder::appendHint, hint, SqlDialect.HIVE);
        applyTestFor(AttributedStringTestSpecBuilder::appendHint, hint, SqlDialect.DEFAULT);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "'",
                "''",
                "'''",
                "''''",
                "'\\'",
                "'\\\\'",
                "'from'",
                "'''from'",
                "'''''where'",
                "'''''where'''",
                "'test ` \" \n''select'",
                "'/*   '",
                "'''--   '",
                "'\n  \n'"
            })
    void quotedTest(String quotedText) {
        applyTestFor(AttributedStringTestSpecBuilder::appendQuoted, quotedText, null);
        applyTestFor(AttributedStringTestSpecBuilder::appendQuoted, quotedText, SqlDialect.HIVE);
        applyTestFor(AttributedStringTestSpecBuilder::appendQuoted, quotedText, SqlDialect.DEFAULT);
    }

    @ParameterizedTest
    @ValueSource(strings = {"select1", "test", "_from", "12", "hello world!", "", " ", "\t", "\n"})
    void sqlNonKeywordTest(String nonKeyword) {
        applyTestFor(AttributedStringTestSpecBuilder::append, nonKeyword, SqlDialect.HIVE);
        applyTestFor(AttributedStringTestSpecBuilder::append, nonKeyword, SqlDialect.DEFAULT);
        applyTestFor(AttributedStringTestSpecBuilder::append, nonKeyword, null);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "\"",
                "\"\"",
                "\"\"\"test\"",
                "\"\"\"\"",
                "\"\"\"\"\"\"",
                "\"\\\"",
                "\"\\\\\"",
                "\"from\"",
                "\"''\"",
                "\"test '' \n\"\"select\"",
                "\"/*   \"",
                "\"--   \"",
                "\"\n  \n\""
            })
    void hiveSqlIdentifierTest(String sqlIdentifier) {
        // For non Hive dialect this is not sql identifier style
        applyTestFor(
                AttributedStringTestSpecBuilder::appendSqlIdentifier,
                sqlIdentifier,
                SqlDialect.HIVE);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "`",
                "``",
                "```",
                "````",
                "`\\`",
                "`\\\\`",
                "`select * from`",
                "`''`",
                "`hello '' ''select`",
                "`/*   `",
                "`--   `",
                "`\n  \n`"
            })
    void sqlIdentifierTest(String sqlIdentifier) {
        // For Hive dialect this is not sql identifier style
        applyTestFor(
                AttributedStringTestSpecBuilder::appendSqlIdentifier,
                sqlIdentifier,
                SqlDialect.DEFAULT);
        applyTestFor(AttributedStringTestSpecBuilder::appendSqlIdentifier, sqlIdentifier, null);
    }

    private void applyTestFor(
            BiFunction<AttributedStringTestSpecBuilder, String, AttributedStringTestSpecBuilder>
                    biFunction,
            String sql,
            SqlDialect dialect) {
        for (SyntaxHighlightStyle.BuiltInStyle style : SyntaxHighlightStyle.BuiltInStyle.values()) {
            SqlClientHighlighterTestSpec spec =
                    forSql(
                            sql,
                            style1 -> biFunction.apply(withStyle(style1.getHighlightStyle()), sql));
            assertThat(
                            SqlClientSyntaxHighlighter.getHighlightedOutput(
                                            spec.sql, style.getHighlightStyle(), dialect)
                                    .toAnsi())
                    .as("sql: " + spec.sql + ", style: " + style + ", dialect: " + dialect)
                    .isEqualTo(spec.getExpected(style));
        }
    }

    @ParameterizedTest
    @MethodSource("allDialectsSpecFunctionProvider")
    void complexTestForAllDialects(SqlClientHighlighterTestSpec spec) {
        for (SyntaxHighlightStyle.BuiltInStyle style : SyntaxHighlightStyle.BuiltInStyle.values()) {
            for (SqlDialect dialect : SqlDialect.values()) {
                verifyHighlighting(spec.sql, style, dialect, spec.getExpected(style));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("hiveDialectsSpecFunctionProvider")
    void complexTestForHiveDialect(SqlClientHighlighterTestSpec spec) {
        for (SyntaxHighlightStyle.BuiltInStyle style : SyntaxHighlightStyle.BuiltInStyle.values()) {
            verifyHighlighting(spec.sql, style, SqlDialect.HIVE, spec.getExpected(style));
        }
    }

    @ParameterizedTest
    @MethodSource("defaultDialectsSpecFunctionProvider")
    void complexTestForDefaultDialect(SqlClientHighlighterTestSpec spec) {
        for (SyntaxHighlightStyle.BuiltInStyle style : SyntaxHighlightStyle.BuiltInStyle.values()) {
            verifyHighlighting(spec.sql, style, SqlDialect.DEFAULT, spec.getExpected(style));
        }
    }

    private static void verifyHighlighting(
            String sql,
            SyntaxHighlightStyle.BuiltInStyle style,
            SqlDialect dialect,
            String expected) {
        assertThat(
                        SqlClientSyntaxHighlighter.getHighlightedOutput(
                                        sql, style.getHighlightStyle(), dialect)
                                .toAnsi())
                .as("SQL: " + sql + "\nDialect: " + dialect + "\nStyle: " + style)
                .isEqualTo(expected);
    }

    static Stream<SqlClientHighlighterTestSpec> allDialectsSpecFunctionProvider() {
        return Stream.of(
                forSql(
                        "SELECT '\\';",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .append(" ")
                                        .appendQuoted("'\\'")
                                        .append(";")),
                forSql(
                        "SELECT.FROM%JOIN;",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .append(".")
                                        .appendKeyword("FROM")
                                        .append("%")
                                        .appendKeyword("JOIN")
                                        .append(";")),
                forSql(
                        "SELECT '''';",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .append(" ")
                                        .appendQuoted("''''")
                                        .append(";")));
    }

    static Stream<SqlClientHighlighterTestSpec> defaultDialectsSpecFunctionProvider() {
        return Stream.of(
                forSql(
                        "SELECT 123 AS `\\`;",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .append(" 123 ")
                                        .appendKeyword("AS")
                                        .append(" ")
                                        .appendSqlIdentifier("`\\`")
                                        .append(";")),
                forSql(
                        "SELECT 1 AS ````;",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .append(" 1 ")
                                        .appendKeyword("AS")
                                        .append(" ")
                                        .appendSqlIdentifier("````")
                                        .append(";")),
                forSql(
                        "SELECT'''''1''from'''AS```1``where``group`;",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .appendQuoted("'''''1''from'''")
                                        .appendKeyword("AS")
                                        .appendSqlIdentifier("```1``where``group`")
                                        .append(";")),
                forSql(
                        // query without spaces
                        "SELECT/*+hint*/'abc'--one-line-comment\nAS`field`/*\ncomment\n*/;",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .appendHint("/*+hint*/")
                                        .appendQuoted("'abc'")
                                        .appendComment("--one-line-comment\n")
                                        .appendKeyword("AS")
                                        .appendSqlIdentifier("`field`")
                                        .appendComment("/*\ncomment\n*/")
                                        .append(";")),
                forSql(
                        // query without spaces with double quotes
                        "SELECT/*+hint*/'abc'--one-line-comment\nAS\"field\"/*\ncomment\n*/;",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .appendHint("/*+hint*/")
                                        .appendQuoted("'abc'")
                                        .appendComment("--one-line-comment\n")
                                        .appendKeyword("AS")
                                        .append("\"field\"")
                                        .appendComment("/*\ncomment\n*/")
                                        .append(";")),
                forSql(
                        // invalid query however highlighting should keep working
                        "SELECT/*\n * / \n \"q\" \nfrom dual\n where\n 1 = 1",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .appendComment(
                                                "/*\n * / \n \"q\" \nfrom dual\n where\n 1 = 1")),
                forSql(
                        // invalid query (wrong symbols at the end)
                        // however highlighting should keep working
                        "SELECT 1 AS`one`FROM mytable.tfrom//",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .append(" 1 ")
                                        .appendKeyword("AS")
                                        .appendSqlIdentifier("`one`")
                                        .appendKeyword("FROM")
                                        .append(" mytable.tfrom//")));
    }

    static Stream<SqlClientHighlighterTestSpec> hiveDialectsSpecFunctionProvider() {
        return Stream.of(
                forSql(
                        "SELECT 1 AS \"\"\"\";",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .append(" 1 ")
                                        .appendKeyword("AS")
                                        .append(" ")
                                        .appendSqlIdentifier("\"\"\"\"")
                                        .append(";")),
                forSql(
                        // query without spaces
                        "SELECT/*+hint*/'abc'--one-line-comment\nAS\"field\"/*\ncomment\n*/;",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .appendHint("/*+hint*/")
                                        .appendQuoted("'abc'")
                                        .appendComment("--one-line-comment\n")
                                        .appendKeyword("AS")
                                        .appendSqlIdentifier("\"field\"")
                                        .appendComment("/*\ncomment\n*/")
                                        .append(";")),
                forSql(
                        // query without spaces and ticks
                        "SELECT/*+hint*/'abc'--one-line-comment\nAS`joinq`.afrom/*\ncomment\n*/;",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .appendHint("/*+hint*/")
                                        .appendQuoted("'abc'")
                                        .appendComment("--one-line-comment\n")
                                        .appendKeyword("AS")
                                        .append("`joinq`")
                                        .append(".afrom")
                                        .appendComment("/*\ncomment\n*/")
                                        .append(";")),
                forSql(
                        // invalid query however highlighting should keep working
                        "SELECT/*\n * / \n \"q\" \nfrom dual\n where\n 1 = 1",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .appendComment(
                                                "/*\n * / \n \"q\" \nfrom dual\n where\n 1 = 1")),
                forSql(
                        // invalid query (wrong symbols at the end)
                        // however highlighting should keep working
                        "SELECT 1 AS\"one\"FROM mytable.tfrom//",
                        style ->
                                withStyle(style.getHighlightStyle())
                                        .appendKeyword("SELECT")
                                        .append(" 1 ")
                                        .appendKeyword("AS")
                                        .appendSqlIdentifier("\"one\"")
                                        .appendKeyword("FROM")
                                        .append(" mytable.tfrom//")));
    }

    static class SqlClientHighlighterTestSpec {
        private final String sql;
        private final Function<SyntaxHighlightStyle.BuiltInStyle, AttributedStringTestSpecBuilder>
                function;

        private SqlClientHighlighterTestSpec(
                String sql,
                Function<SyntaxHighlightStyle.BuiltInStyle, AttributedStringTestSpecBuilder>
                        function) {
            this.sql = sql;
            this.function = function;
        }

        public static SqlClientHighlighterTestSpec forSql(
                String sql,
                Function<SyntaxHighlightStyle.BuiltInStyle, AttributedStringTestSpecBuilder>
                        expectedBuilder) {
            return new SqlClientHighlighterTestSpec(sql, expectedBuilder);
        }

        String getExpected(SyntaxHighlightStyle.BuiltInStyle style) {
            return function.apply(style).asb.toAnsi();
        }

        @Override
        public String toString() {
            return sql;
        }
    }

    static class AttributedStringTestSpecBuilder {
        private final AttributedStringBuilder asb = new AttributedStringBuilder();
        private final SyntaxHighlightStyle style;

        AttributedStringTestSpecBuilder(SyntaxHighlightStyle style) {
            this.style = style;
        }

        public static AttributedStringTestSpecBuilder withStyle(SyntaxHighlightStyle style) {
            return new AttributedStringTestSpecBuilder(style);
        }

        AttributedStringTestSpecBuilder appendKeyword(String keyword) {
            asb.style(style.getKeywordStyle()).append(keyword);
            return this;
        }

        AttributedStringTestSpecBuilder append(String word) {
            asb.style(style.getDefaultStyle()).append(word);
            return this;
        }

        AttributedStringTestSpecBuilder appendQuoted(String quoted) {
            asb.style(style.getQuotedStyle()).append(quoted);
            return this;
        }

        AttributedStringTestSpecBuilder appendComment(String comment) {
            asb.style(style.getCommentStyle()).append(comment);
            return this;
        }

        AttributedStringTestSpecBuilder appendHint(String hint) {
            asb.style(style.getHintStyle()).append(hint);
            return this;
        }

        AttributedStringTestSpecBuilder appendSqlIdentifier(String hint) {
            asb.style(style.getSqlIdentifierStyle()).append(hint);
            return this;
        }
    }
}
