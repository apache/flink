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

package org.apache.flink.table.client.cli.utils;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.client.cli.parser.SqlClientParserState;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.client.cli.parser.SqlClientParserState.computeCurrentStateAtTheEndOfLine;

/**
 * A utility to read and parse content of a SQL script. The SQL script is located in "resources/sql"
 * path and in the "xx.q" file name pattern. The SQL script is executed and tested by {@link
 * org.apache.flink.table.client.cli.CliClientITCase}.
 */
public final class SqlScriptReader implements AutoCloseable {
    public static final String HINT_START_OF_OUTPUT = "!output";
    private final boolean enableMultiStatement;
    private final BufferedReader reader;
    private String currentLine;

    public static List<TestSqlStatement> parseSqlScript(String in) {
        try (SqlScriptReader sqlReader = new SqlScriptReader(in, false)) {
            return sqlReader.parseSqlScript();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<TestSqlStatement> parseMultiStatementSqlScript(String in) {
        try (SqlScriptReader sqlReader = new SqlScriptReader(in, true)) {
            return sqlReader.parseSqlScript();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SqlScriptReader(String input, boolean multiStatementEnabled) {
        this.reader = new BufferedReader(new StringReader(input));
        this.enableMultiStatement = multiStatementEnabled;
    }

    private List<TestSqlStatement> parseSqlScript() throws IOException {
        List<TestSqlStatement> specs = new ArrayList<>();
        TestSqlStatement spec;
        while ((spec = readNext()) != null) {
            specs.add(spec);
        }
        return specs;
    }

    private void readLine() throws IOException {
        this.currentLine = reader.readLine();
    }

    private @Nullable TestSqlStatement readNext() throws IOException {
        StringBuilder commentLines = new StringBuilder();
        StringBuilder sqlLines = new StringBuilder();
        ReadingStatus status = ReadingStatus.BEGINNING;
        readLine();
        while (currentLine != null) {
            switch (status) {
                case BEGINNING:
                    if (currentLine.startsWith("#") || currentLine.trim().length() == 0) {
                        commentLines.append(currentLine).append("\n");
                        // continue reading if not reach SQL statement
                        readLine();
                    } else {
                        // if current currentLine is not comment and empty currentLine, begin to
                        // read SQL
                        status = ReadingStatus.SQL_STATEMENT;
                    }
                    break;

                case SQL_STATEMENT:
                    if (enableMultiStatement && currentLine.trim().equals(HINT_START_OF_OUTPUT)) {
                        // SQL statement is finished, begin to read result content
                        status = ReadingStatus.RESULT_CONTENT;
                    } else {
                        sqlLines.append(currentLine).append("\n");
                        if (!enableMultiStatement
                                && currentLine.trim().endsWith(";")
                                && computeCurrentStateAtTheEndOfLine(
                                                sqlLines.toString().trim(), SqlDialect.DEFAULT)
                                        == SqlClientParserState.DEFAULT) {
                            // SQL statement is finished, begin to read result content
                            status = ReadingStatus.RESULT_CONTENT;
                        }
                    }
                    // continue reading
                    readLine();
                    break;

                case RESULT_CONTENT:
                    if (!currentLine.startsWith("!")) {
                        // continue consume if not reaching result flag
                        readLine();
                    } else {
                        // reach result flag and return
                        status = ReadingStatus.FINISH;
                    }
                    break;

                case FINISH:
                    return new TestSqlStatement(commentLines.toString(), sqlLines.toString());
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    private enum ReadingStatus {
        BEGINNING,
        SQL_STATEMENT,
        RESULT_CONTENT,
        FINISH
    }
}
