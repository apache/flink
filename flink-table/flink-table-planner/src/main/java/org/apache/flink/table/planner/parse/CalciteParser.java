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

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.api.SqlParserException;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.SourceStringReader;

import java.io.Reader;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Thin wrapper around {@link SqlParser} that does exception conversion and {@link SqlNode} casting.
 */
public class CalciteParser {
    private final SqlParser.Config config;
    private final List<String> additionalSensitiveKeys;

    public CalciteParser(SqlParser.Config config, List<String> additionalSensitiveKeys) {
        this.config = config;
        this.additionalSensitiveKeys = additionalSensitiveKeys;
    }

    /**
     * Parses a SQL statement into a {@link SqlNode}. The {@link SqlNode} is not yet validated.
     *
     * @param sql a sql string to parse
     * @return a parsed sql node
     * @throws SqlParserException if an exception is thrown when parsing the statement
     * @throws SqlParserEOFException if the statement is incomplete
     */
    public SqlNode parse(String sql) {
        try {
            SqlParser parser = SqlParser.create(sql, config);
            return parser.parseStmt();
        } catch (SqlParseException e) {
            throw toStatementException(sql, e);
        }
    }

    /**
     * Parses a SQL string into a {@link SqlNodeList}. The {@link SqlNodeList} is not yet validated.
     *
     * @param sql a sql string to parse
     * @return a parsed sql node list
     * @throws SqlParserException if an exception is thrown when parsing the statement
     * @throws SqlParserEOFException if the statement is incomplete
     */
    public SqlNodeList parseSqlList(String sql) {
        try {
            SqlParser parser = SqlParser.create(sql, config);
            return parser.parseStmtList();
        } catch (SqlParseException e) {
            throw toStatementException(sql, e);
        }
    }

    /**
     * Parses a SQL expression into a {@link SqlNode}. The {@link SqlNode} is not yet validated.
     *
     * @param sqlExpression a SQL expression string to parse
     * @return a parsed SQL node
     * @throws SqlParserException if an exception is thrown when parsing the statement
     */
    public SqlNode parseExpression(String sqlExpression) throws SqlParserException {
        try {
            final SqlParser parser = SqlParser.create(sqlExpression, config);
            return parser.parseExpression();
        } catch (SqlParseException e) {
            throw toParserException(sqlExpression, e);
        }
    }

    /**
     * Parses a SQL string as an identifier into a {@link SqlIdentifier}.
     *
     * @param identifier a sql string to parse as an identifier
     * @return a parsed sql node
     * @throws SqlParserException if an exception is thrown when parsing the identifier
     */
    public SqlIdentifier parseIdentifier(String identifier) throws SqlParserException {
        try {
            SqlAbstractParserImpl flinkParser = createFlinkParser(identifier);
            if (flinkParser instanceof FlinkSqlParserImpl) {
                return ((FlinkSqlParserImpl) flinkParser).TableApiIdentifier();
            } else {
                throw new IllegalArgumentException(
                        "Unrecognized sql parser type " + flinkParser.getClass().getName());
            }
        } catch (Exception e) {
            throw new SqlParserException(
                    String.format("Invalid SQL identifier %s.", identifier), e);
        }
    }

    /**
     * Converts a parse error of a statement. An error at the end of the input means the statement
     * is incomplete, which callers such as the SQL client use to keep reading.
     */
    private SqlParserException toStatementException(String sql, SqlParseException e) {
        final String message = e.getMessage();
        if (message != null && message.contains("Encountered \"<EOF>\"")) {
            return new SqlParserEOFException(message, e);
        }
        return toParserException(sql, e);
    }

    private SqlParserException toParserException(String sql, SqlParseException e) {
        final String message = describe(sql, e);
        return new SqlParserException(
                message.isEmpty() ? "SQL parse failed." : "SQL parse failed. " + message, e);
    }

    /**
     * Returns the Calcite message, preceded by the position and the line it points at when the
     * parser recorded one.
     *
     * <p>Errors raised through {@code SqlUtil.newContextException} keep their position only in
     * {@link SqlParseException#getPos()}, and {@link SqlParserException} exposes none, so it goes
     * into the message in the wording validation errors use. The line goes in front so the message
     * still ends with Calcite's text: the SQL client's {@code CliStrings#findReason} picks the
     * exception to print by that suffix. JavaCC and lexer errors get the heading too, although they
     * name the position themselves, so every positioned error starts the same way.
     */
    private String describe(String sql, SqlParseException e) {
        final String message = e.getMessage() == null ? "" : e.getMessage();
        final SqlParserPos pos = e.getPos();
        if (pos == null || pos.getLineNum() <= 0 || message.isEmpty()) {
            return message;
        }
        final boolean point =
                pos.getLineNum() == pos.getEndLineNum()
                        && pos.getColumnNum() == pos.getEndColumnNum();
        final String context =
                point
                        ? RESOURCE.validatorContextPoint(pos.getLineNum(), pos.getColumnNum()).str()
                        : RESOURCE.validatorContext(
                                        pos.getLineNum(),
                                        pos.getColumnNum(),
                                        pos.getEndLineNum(),
                                        pos.getEndColumnNum())
                                .str();
        return ParseErrorSnippet.render(sql, pos, additionalSensitiveKeys)
                .map(snippet -> context + ":\n" + snippet + "\n" + message)
                .orElse(context + ": " + message);
    }

    /**
     * Equivalent to {@link SqlParser#create(Reader, SqlParser.Config)}. The only difference is we
     * do not wrap the {@link FlinkSqlParserImpl} with {@link SqlParser}.
     *
     * <p>It is so that we can access specific parsing methods not accessible through the {@code
     * SqlParser}.
     */
    private SqlAbstractParserImpl createFlinkParser(String expr) {
        SourceStringReader reader = new SourceStringReader(expr);
        SqlAbstractParserImpl parser = config.parserFactory().getParser(reader);
        parser.setTabSize(1);
        parser.setQuotedCasing(config.quotedCasing());
        parser.setUnquotedCasing(config.unquotedCasing());
        parser.setIdentifierMaxLength(config.identifierMaxLength());
        parser.setConformance(config.conformance());
        switch (config.quoting()) {
            case DOUBLE_QUOTE:
                parser.switchTo(SqlAbstractParserImpl.LexicalState.DQID);
                break;
            case BACK_TICK:
                parser.switchTo(SqlAbstractParserImpl.LexicalState.BTID);
                break;
            case BRACKET:
                parser.switchTo(SqlAbstractParserImpl.LexicalState.DEFAULT);
                break;
        }

        return parser;
    }
}
