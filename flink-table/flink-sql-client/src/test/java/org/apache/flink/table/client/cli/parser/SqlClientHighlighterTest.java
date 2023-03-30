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

import java.util.stream.Stream;

import static org.apache.flink.table.client.cli.parser.SyntaxHighlightStyle.BuiltInStyle.DARK;
import static org.apache.flink.table.client.cli.parser.SyntaxHighlightStyle.BuiltInStyle.LIGHT;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SqlClientSyntaxHighlighter}. */
class SqlClientHighlighterTest {
    @ParameterizedTest
    @MethodSource("specProvider")
    void test(SqlClientHighlighterTestSpec spec) {
        assertThat(
                        SqlClientSyntaxHighlighter.getHighlightedOutput(
                                        spec.sql, spec.style, spec.dialect)
                                .toAnsi())
                .isEqualTo(spec.getExpected());
    }

    static Stream<SqlClientHighlighterTestSpec> specProvider() {
        return Stream.of(
                SqlClientHighlighterTestSpec.of(
                        "select",
                        new AttributedStringTestSpecBuilder(DARK.getHighlightStyle())
                                .appendKeyword("select")),
                SqlClientHighlighterTestSpec.of(
                        "default_style",
                        new AttributedStringTestSpecBuilder(DARK.getHighlightStyle())
                                .append("default_style")),
                SqlClientHighlighterTestSpec.of(
                        "SELECT '\\';",
                        new AttributedStringTestSpecBuilder(DARK.getHighlightStyle())
                                .appendKeyword("SELECT")
                                .append(" ")
                                .appendQuoted("'\\'")
                                .append(";")),
                SqlClientHighlighterTestSpec.of(
                        "SELECT '\\';",
                        new AttributedStringTestSpecBuilder(DARK.getHighlightStyle())
                                .appendKeyword("SELECT")
                                .append(" ")
                                .appendQuoted("'\\'")
                                .append(";")),
                SqlClientHighlighterTestSpec.of(
                        "SELECT 123 AS `\\`;",
                        new AttributedStringTestSpecBuilder(DARK.getHighlightStyle())
                                .appendKeyword("SELECT")
                                .append(" 123 ")
                                .appendKeyword("AS")
                                .append(" ")
                                .appendSqlIdentifier("`\\`")
                                .append(";")),
                SqlClientHighlighterTestSpec.of(
                        "SELECT '''';",
                        new AttributedStringTestSpecBuilder(DARK.getHighlightStyle())
                                .appendKeyword("SELECT")
                                .append(" ")
                                .appendQuoted("''''")
                                .append(";")),
                SqlClientHighlighterTestSpec.of(
                        "SELECT 1 AS ````;",
                        new AttributedStringTestSpecBuilder(DARK.getHighlightStyle())
                                .appendKeyword("SELECT")
                                .append(" 1 ")
                                .appendKeyword("AS")
                                .append(" ")
                                .appendSqlIdentifier("````")
                                .append(";")),
                SqlClientHighlighterTestSpec.of(
                        "'quoted'",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendQuoted("'quoted'")),
                SqlClientHighlighterTestSpec.of(
                        "`sqlQuoteIdentifier`",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendSqlIdentifier("`sqlQuoteIdentifier`")),
                SqlClientHighlighterTestSpec.of(
                        "/*\nmultiline\n comment\n*/",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendComment("/*\nmultiline\n comment\n*/")),
                new SqlClientHighlighterTestSpec(
                        "/*\nnot finished\nmultiline\n comment\n",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendComment("/*\nnot finished\nmultiline\n comment\n")),
                SqlClientHighlighterTestSpec.of(
                        "/*+hint*/",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendHint("/*+hint*/")),
                SqlClientHighlighterTestSpec.of(
                        "'`not a sql quote`''/*not a comment*/''--not a comment'",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendQuoted(
                                        "'`not a sql quote`''/*not a comment*/''--not a comment'")),
                SqlClientHighlighterTestSpec.of(
                        "`'not a quote'``/*not a comment*/``--not a comment`",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendSqlIdentifier(
                                        "`'not a quote'``/*not a comment*/``--not a comment`")),
                SqlClientHighlighterTestSpec.of(
                        "/*'not a quote'`not a sql quote``` /*+ not a hint*/",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendComment(
                                        "/*'not a quote'`not a sql quote``` /*+ not a hint*/")),
                SqlClientHighlighterTestSpec.of(
                        "select/*+ hint*/'1'as`one`/*comment*/from--\ndual;",
                        new AttributedStringTestSpecBuilder(LIGHT.getHighlightStyle())
                                .appendKeyword("select")
                                .appendHint("/*+ hint*/")
                                .appendQuoted("'1'")
                                .appendKeyword("as")
                                .appendSqlIdentifier("`one`")
                                .appendComment("/*comment*/")
                                .appendKeyword("from")
                                .appendComment("--\n")
                                .append("dual;")));
    }

    static class SqlClientHighlighterTestSpec {
        private String sql;
        private SqlDialect dialect;
        private SyntaxHighlightStyle style;
        private AttributedStringTestSpecBuilder expectedBuilder;

        private SqlClientHighlighterTestSpec(
                String sql, AttributedStringTestSpecBuilder expectedBuilder) {
            this.sql = sql;
            this.expectedBuilder = expectedBuilder;
            this.style = expectedBuilder.style;
        }

        public static SqlClientHighlighterTestSpec of(String sql, AttributedStringTestSpecBuilder expectedBuilder) {
            return new SqlClientHighlighterTestSpec(sql, expectedBuilder);
        }

        SqlClientHighlighterTestSpec dialect(SqlDialect dialect) {
            this.dialect = dialect;
            return this;
        }

        String getExpected() {
            return expectedBuilder.asb.toAnsi();
        }
    }

    static class AttributedStringTestSpecBuilder {
        private final AttributedStringBuilder asb = new AttributedStringBuilder();
        private final SyntaxHighlightStyle style;

        AttributedStringTestSpecBuilder(SyntaxHighlightStyle style) {
            this.style = style;
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
