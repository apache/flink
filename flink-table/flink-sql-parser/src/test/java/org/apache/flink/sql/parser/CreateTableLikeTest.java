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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableLike.FeatureOption;
import org.apache.flink.sql.parser.ddl.SqlTableLike.MergingStrategy;
import org.apache.flink.sql.parser.ddl.SqlTableLike.SqlTableLikeOption;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

/** Tests for parsing and validating {@link SqlTableLike} clause. */
public class CreateTableLikeTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testNoOptions() throws Exception {
        SqlNode actualNode =
                createFlinkParser("CREATE TABLE t (\n" + "   a STRING\n" + ")\n" + "LIKE b")
                        .parseStmt();

        assertThat(actualNode, hasLikeClause(allOf(pointsTo("b"), hasNoOptions())));
    }

    @Test
    public void testCreateTableLike() throws Exception {
        SqlNode actualNode =
                createFlinkParser(
                                "CREATE TABLE t (\n"
                                        + "   a STRING\n"
                                        + ")\n"
                                        + "LIKE b (\n"
                                        + "   EXCLUDING PARTITIONS\n"
                                        + "   EXCLUDING CONSTRAINTS\n"
                                        + "   EXCLUDING WATERMARKS\n"
                                        + "   OVERWRITING GENERATED\n"
                                        + "   OVERWRITING OPTIONS\n"
                                        + ")")
                        .parseStmt();

        assertThat(
                actualNode,
                hasLikeClause(
                        allOf(
                                pointsTo("b"),
                                hasOptions(
                                        option(MergingStrategy.EXCLUDING, FeatureOption.PARTITIONS),
                                        option(
                                                MergingStrategy.EXCLUDING,
                                                FeatureOption.CONSTRAINTS),
                                        option(MergingStrategy.EXCLUDING, FeatureOption.WATERMARKS),
                                        option(
                                                MergingStrategy.OVERWRITING,
                                                FeatureOption.GENERATED),
                                        option(
                                                MergingStrategy.OVERWRITING,
                                                FeatureOption.OPTIONS)))));
    }

    @Test
    public void testCreateTableLikeCannotDuplicateOptions() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                        "CREATE TABLE t (\n"
                                                + "   a STRING\n"
                                                + ")\n"
                                                + "LIKE b (\n"
                                                + "   EXCLUDING PARTITIONS\n"
                                                + "   INCLUDING PARTITIONS\n"
                                                + ")")
                                .parseStmt();

        thrown.expect(SqlValidateException.class);
        thrown.expectMessage("Each like option feature can be declared only once.");
        extendedSqlNode.validate();
    }

    @Test
    public void testInvalidOverwritingForPartition() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                        "CREATE TABLE t (\n"
                                                + "   a STRING\n"
                                                + ")\n"
                                                + "LIKE b (\n"
                                                + "   OVERWRITING PARTITIONS"
                                                + ")")
                                .parseStmt();

        thrown.expect(SqlValidateException.class);
        thrown.expectMessage("Illegal merging strategy 'OVERWRITING' for 'PARTITIONS' option.");
        extendedSqlNode.validate();
    }

    @Test
    public void testInvalidOverwritingForAll() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                        "CREATE TABLE t (\n"
                                                + "   a STRING\n"
                                                + ")\n"
                                                + "LIKE b (\n"
                                                + "   OVERWRITING ALL"
                                                + ")")
                                .parseStmt();

        thrown.expect(SqlValidateException.class);
        thrown.expectMessage("Illegal merging strategy 'OVERWRITING' for 'ALL' option.");
        extendedSqlNode.validate();
    }

    @Test
    public void testInvalidOverwritingForConstraints() throws Exception {
        ExtendedSqlNode extendedSqlNode =
                (ExtendedSqlNode)
                        createFlinkParser(
                                        "CREATE TABLE t (\n"
                                                + "   a STRING\n"
                                                + ")\n"
                                                + "LIKE b (\n"
                                                + "   OVERWRITING CONSTRAINTS"
                                                + ")")
                                .parseStmt();

        thrown.expect(SqlValidateException.class);
        thrown.expectMessage("Illegal merging strategy 'OVERWRITING' for 'CONSTRAINTS' option.");
        extendedSqlNode.validate();
    }

    @Test
    public void testInvalidNoOptions() throws SqlParseException {
        thrown.expect(SqlParseException.class);
        thrown.expectMessage(
                "Encountered \")\" at line 4, column 9.\n"
                        + "Was expecting one of:\n"
                        + "    \"EXCLUDING\" ...\n"
                        + "    \"INCLUDING\" ...\n"
                        + "    \"OVERWRITING\" ...");
        createFlinkParser("CREATE TABLE t (\n" + "   a STRING\n" + ")\n" + "LIKE b ()").parseStmt();
    }

    @Test
    public void testInvalidNoSourceTable() throws SqlParseException {
        thrown.expect(SqlParseException.class);
        thrown.expectMessage(
                "Encountered \"(\" at line 4, column 6.\n"
                        + "Was expecting one of:\n"
                        + "    <BRACKET_QUOTED_IDENTIFIER> ...\n"
                        + "    <QUOTED_IDENTIFIER> ...\n"
                        + "    <BACK_QUOTED_IDENTIFIER> ...\n"
                        + "    <HYPHENATED_IDENTIFIER> ...\n"
                        + "    <IDENTIFIER> ...\n"
                        + "    <UNICODE_QUOTED_IDENTIFIER> ...\n");
        createFlinkParser(
                        "CREATE TABLE t (\n"
                                + "   a STRING\n"
                                + ")\n"
                                + "LIKE ("
                                + "   INCLUDING ALL"
                                + ")")
                .parseStmt();
    }

    public static SqlTableLikeOption option(
            MergingStrategy mergingStrategy, FeatureOption featureOption) {
        return new SqlTableLikeOption(mergingStrategy, featureOption);
    }

    private static Matcher<SqlTableLike> hasOptions(SqlTableLikeOption... optionMatchers) {
        return new FeatureMatcher<SqlTableLike, List<SqlTableLikeOption>>(
                equalTo(Arrays.asList(optionMatchers)), "like options equal to", "like options") {
            @Override
            protected List<SqlTableLikeOption> featureValueOf(SqlTableLike actual) {
                return actual.getOptions();
            }
        };
    }

    private static Matcher<SqlTableLike> hasNoOptions() {
        return new FeatureMatcher<SqlTableLike, List<SqlTableLikeOption>>(
                empty(), "like options are empty", "like options") {
            @Override
            protected List<SqlTableLikeOption> featureValueOf(SqlTableLike actual) {
                return actual.getOptions();
            }
        };
    }

    private static Matcher<SqlTableLike> pointsTo(String... table) {
        return new FeatureMatcher<SqlTableLike, String[]>(
                equalTo(table), "source table identifier pointing to", "source table identifier") {

            @Override
            protected String[] featureValueOf(SqlTableLike actual) {
                return actual.getSourceTable().names.toArray(new String[0]);
            }
        };
    }

    private static Matcher<SqlNode> hasLikeClause(Matcher<SqlTableLike> likeMatcher) {
        return new FeatureMatcher<SqlNode, SqlTableLike>(
                likeMatcher, "create table statement has like clause", "like clause") {

            @Override
            protected SqlTableLike featureValueOf(SqlNode actual) {
                if (!(actual instanceof SqlCreateTable)) {
                    throw new AssertionError("Node is not a CREATE TABLE stmt.");
                }
                return ((SqlCreateTable) actual).getTableLike().orElse(null);
            }
        };
    }

    private SqlParser createFlinkParser(String expr) {
        SqlParser.Config parserConfig =
                SqlParser.configBuilder()
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setLex(Lex.JAVA)
                        .setIdentifierMaxLength(256)
                        .build();

        return SqlParser.create(expr, parserConfig);
    }
}
