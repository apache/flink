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

import org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImplTokenManager;
import org.apache.flink.sql.parser.impl.SimpleCharStream;
import org.apache.flink.sql.parser.impl.Token;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.operations.Operation;

import javax.annotation.Nonnull;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ClientParser use {@link FlinkSqlParserImplTokenManager} to do lexical analysis. It cannot
 * recognize special hive keywords yet.
 */
public class ClientParser implements SqlCommandParser, FlinkSqlParserImplConstants {

    /** A dumb implementation. TODO: remove this after unifying the SqlMultiLineParser. */
    @Override
    public Optional<Operation> parseCommand(String command) {
        return Optional.empty();
    }

    public Optional<StatementType> parseStatement(@Nonnull String statement)
            throws SqlExecutionException {
        String trimmedStatement = statement.trim();
        FlinkSqlParserImplTokenManager tokenManager =
                new FlinkSqlParserImplTokenManager(
                        new SimpleCharStream(new StringReader(trimmedStatement)));
        // this means to switch to "BACK TICK IDENTIFIER" state to support '`' in Flink SQL
        tokenManager.SwitchTo(2);
        List<Token> tokens = new ArrayList<>();
        Token token = tokenManager.getNextToken();
        while (token.kind != EOF) {
            tokens.add(token);
            token = tokenManager.getNextToken();
        }

        if (tokens.size() == 0 || tokens.get(tokens.size() - 1).kind != SEMICOLON) {
            // throw this to notify the terminal to continue reading input
            throw new SqlExecutionException("", new SqlParserEOFException(""));
        }

        return getStatementType(tokens);
    }

    // ---------------------------------------------------------------------------------------------
    private Optional<StatementType> getStatementType(List<Token> tokens) {
        Token firstToken = tokens.get(0);

        if (firstToken.kind == SEMICOLON) {
            return Optional.empty();
        }

        if (firstToken.kind == IDENTIFIER) {
            // it means the token is not a reserved keyword, potentially a client command
            return getPotentialCommandType(firstToken.image);
        } else if (firstToken.kind == EXPLAIN) {
            return Optional.of(StatementType.EXPLAIN);
        } else if (firstToken.kind == SHOW) {
            return getPotentialShowCreateType(tokens);
        } else if (firstToken.kind == BEGIN) {
            return getPotentialBeginStatementSetType(tokens);
        } else if (firstToken.kind == END) {
            // an 'END;' statement should have exactly 2 tokens
            return tokens.size() == 2
                    ? Optional.of(StatementType.END)
                    : Optional.of(StatementType.OTHER);
        } else {
            return Optional.of(StatementType.OTHER);
        }
    }

    private Optional<StatementType> getPotentialCommandType(String image) {
        switch (image.toUpperCase()) {
            case "QUIT":
            case "EXIT":
                return Optional.of(StatementType.QUIT);
            case "CLEAR":
                return Optional.of(StatementType.CLEAR);
            case "HELP":
                return Optional.of(StatementType.HELP);
            default:
                return Optional.of(StatementType.OTHER);
        }
    }

    private Optional<StatementType> getPotentialShowCreateType(List<Token> tokens) {
        // obviously a 'SHOW CREATE TABLE/VIEW' statement has more than 3 tokens
        if (tokens.size() < 3) {
            return Optional.of(StatementType.OTHER);
        }
        Token secondToken = tokens.get(1), thirdToken = tokens.get(2);
        if (secondToken.kind == CREATE && (thirdToken.kind == TABLE || thirdToken.kind == VIEW)) {
            return Optional.of(StatementType.SHOW_CREATE);
        } else {
            return Optional.of(StatementType.OTHER);
        }
    }

    private Optional<StatementType> getPotentialBeginStatementSetType(List<Token> tokens) {
        // obviously a 'BEGIN STATEMENT SET' statement has more than 3 tokens
        if (tokens.size() < 3) {
            return Optional.of(StatementType.OTHER);
        }
        Token secondToken = tokens.get(1), thirdToken = tokens.get(2);
        if (secondToken.kind == STATEMENT && thirdToken.kind == SET) {
            return Optional.of(StatementType.BEGIN_STATEMENT_SET);
        } else {
            return Optional.of(StatementType.OTHER);
        }
    }
}
