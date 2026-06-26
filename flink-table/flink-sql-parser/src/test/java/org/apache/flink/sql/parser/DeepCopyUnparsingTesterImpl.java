/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unparsing tester that, on top of the regular parse/unparse round-trips performed by {@link
 * SqlParserTest.UnparsingTesterImpl}, also makes a deep copy of every parsed {@link SqlNode} and
 * asserts that the copy unparses to the same SQL as the original.
 *
 * <p>The class should be dropped after upgrading to Calcite 1.42.0 since similar logic is there.
 * More details are at CALCITE-7301.
 */
class DeepCopyUnparsingTesterImpl extends SqlParserTest.UnparsingTesterImpl {

    private static final String FLINK_PARSER_PACKAGE = "org.apache.flink.sql.parser";

    /** Whether {@code node} or any node in its subtree is a Flink parser node. */
    private static boolean containsFlinkNode(SqlNode node) {
        if (isFlinkNode(node)) {
            return true;
        }
        final boolean[] found = {false};
        node.accept(
                new SqlShuttle() {
                    @Override
                    public SqlNode visit(final SqlCall call) {
                        if (isFlinkNode(call)) {
                            found[0] = true;
                        }
                        // Default traversal copies nothing (alwaysCopy=false), so it never
                        // invokes createCall and is safe even for unfixed core operators.
                        return super.visit(call);
                    }
                });
        return found[0];
    }

    private static boolean isFlinkNode(SqlNode node) {
        return node.getClass().getName().startsWith(FLINK_PARSER_PACKAGE);
    }

    private static SqlNode deepCopy(SqlNode sqlNode) {
        return sqlNode.accept(
                new SqlShuttle() {
                    @Override
                    public SqlNode visit(final SqlCall call) {
                        // Handler always creates a new copy of 'call'.
                        CallCopyingArgHandler argHandler = new CallCopyingArgHandler(call, true);
                        call.getOperator().acceptCall(this, call, false, argHandler);
                        return argHandler.result();
                    }
                });
    }

    private static UnaryOperator<SqlWriterConfig> simple() {
        return c ->
                c.withSelectListItemsOnSeparateLines(false)
                        .withUpdateSetListNewline(false)
                        .withIndentation(0)
                        .withFromFolding(SqlWriterConfig.LineFolding.TALL);
    }

    private static SqlWriterConfig simpleWithParens(SqlWriterConfig c) {
        return simple().apply(c).withAlwaysUseParentheses(true);
    }

    private static String toSqlString(
            SqlNodeList sqlNodeList, UnaryOperator<SqlWriterConfig> transform) {
        return sqlNodeList.stream()
                .map(node -> node.toSqlString(transform).getSql())
                .collect(Collectors.joining(";"));
    }

    @Override
    public void checkList(
            SqlTestFactory factory,
            StringAndPos sap,
            SqlDialect dialect,
            UnaryOperator<String> converter,
            List<String> expected) {
        super.checkList(factory, sap, dialect, converter, expected);

        final SqlNodeList sqlNodeList = parseStmtsAndHandleEx(factory, sap.sql);
        if (!containsFlinkNode(sqlNodeList)) {
            return;
        }
        final String sql1 = toSqlString(sqlNodeList, simple());

        // Make a deep copy of the SqlNodeList, unparse it.
        final SqlNodeList sqlNodeList3 = (SqlNodeList) deepCopy(sqlNodeList);
        final String sql4 = toSqlString(sqlNodeList3, simple());
        // Should be the same as we started with.
        assertThat(sql4).isEqualTo(sql1);
    }

    @Override
    public void check(
            SqlTestFactory factory,
            StringAndPos sap,
            SqlDialect dialect,
            UnaryOperator<String> converter,
            String expected,
            Consumer<SqlParser> parserChecker) {
        super.check(factory, sap, dialect, converter, expected, parserChecker);

        final SqlNode sqlNode = parseStmtAndHandleEx(factory, sap.sql, parserChecker);
        if (!containsFlinkNode(sqlNode)) {
            return;
        }
        final SqlDialect dialect2 = Util.first(dialect, AnsiSqlDialect.DEFAULT);
        final UnaryOperator<SqlWriterConfig> writerTransform =
                c -> simpleWithParens(c).withDialect(dialect2);

        // Make a deep copy of the original SqlNode, unparse it.
        final SqlNode sqlNode5 = deepCopy(sqlNode);
        final String actual5 = sqlNode5.toSqlString(writerTransform).getSql();
        assertThat(converter.apply(actual5)).isEqualTo(expected);
    }
}
