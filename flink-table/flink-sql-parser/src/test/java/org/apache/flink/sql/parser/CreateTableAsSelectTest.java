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
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for parsing and validating CREATE TABLE AS SELECT(CTAS) clause in {@link SqlCreateTable}.
 */
class CreateTableAsSelectTest {

    @Test
    void testNoOptions() throws Exception {
        SqlNode actualNode = createFlinkParser("CREATE TABLE t AS SELECT * FROM b").parseStmt();

        assertThat(actualNode.toString()).isEqualTo("CREATE TABLE `t`\nAS\nSELECT *\nFROM `b`");
    }

    @Test
    void testWithOptions() throws Exception {
        SqlNode actualNode =
                createFlinkParser("CREATE TABLE t WITH ('test' = 'zm') AS SELECT * FROM b")
                        .parseStmt();

        assertThat(actualNode.toString())
                .isEqualTo("CREATE TABLE `t` WITH (\n  'test' = 'zm'\n)\nAS\nSELECT *\nFROM `b`");
    }

    @Test
    void testWithColumns() throws Exception {
        SqlNode actualNode =
                createFlinkParser(
                                "CREATE TABLE t (col1 string) WITH ('test' = 'zm') AS SELECT col1 FROM b")
                        .parseStmt();

        assertThat(actualNode.toString())
                .isEqualTo(
                        "CREATE TABLE `t` (\n"
                                + "  `col1` STRING\n"
                                + ") WITH (\n"
                                + "  'test' = 'zm'\n"
                                + ")\n"
                                + "AS\nSELECT `col1`\n"
                                + "FROM `b`");
        assertThat(actualNode).isInstanceOf(SqlCreateTable.class);
        assertThatThrownBy(() -> ((SqlCreateTable) actualNode).validate())
                .isInstanceOf(SqlValidateException.class)
                .hasMessage(
                        "CREATE TABLE AS SELECT syntax does not yet support to specific Column/Partition/Constraints.");
    }

    @Test
    void testCtasAndLike() throws Exception {
        SqlParser parser =
                createFlinkParser(
                        "CREATE TABLE t (col1 string) WITH ('test' = 'zm') like b AS SELECT col1 FROM b");

        assertThatThrownBy(parser::parseStmt)
                .isInstanceOf(SqlParseException.class)
                .hasMessageStartingWith(
                        "Encountered \"AS\" at line 1, column 58.\n"
                                + "Was expecting one of:\n"
                                + "    <EOF> \n"
                                + "    \"(\" ...\n"
                                + "    \".\" ...");
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
