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

import org.apache.flink.sql.parser.impl.FlinkSqlParserImplTokenManager;
import org.apache.flink.sql.parser.impl.SimpleCharStream;
import org.apache.flink.sql.parser.impl.Token;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.operations.Operation;

import java.io.StringReader;
import java.util.Iterator;
import java.util.Optional;

import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.BEGIN;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.CREATE;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.END;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.EOF;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.EXPLAIN;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.IDENTIFIER;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.JAR;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.REMOVE;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.RESET;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.SELECT;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.SEMICOLON;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.SET;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.SHOW;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.STATEMENT;

/**
 * ClientParser uses {@link FlinkSqlParserImplTokenManager} to do lexical analysis. It cannot
 * recognize special hive keywords yet because Hive has a slightly different vocabulary compared to
 * Flink's, which causes the ClientParser misunderstanding some Hive's keywords to IDENTIFIER. But
 * the ClientParser is only responsible to check whether the statement is completed or not and only
 * cares about a few statements. So it's acceptable to tolerate the inaccuracy here.
 */
public class ClientParser implements SqlCommandParser {

    /** A dumb implementation. TODO: remove this after unifying the SqlMultiLineParser. */
    @Override
    public Optional<Operation> parseCommand(String command) {
        return Optional.empty();
    }

    public Optional<StatementType> parseStatement(String statement) throws SqlExecutionException {
        return getStatementType(new TokenIterator(statement.trim()));
    }

    // ---------------------------------------------------------------------------------------------

    private static class TokenIterator implements Iterator<Token> {

        private final FlinkSqlParserImplTokenManager tokenManager;
        private Token currentToken;

        public TokenIterator(String statement) {
            this.tokenManager =
                    new FlinkSqlParserImplTokenManager(
                            new SimpleCharStream(new StringReader(statement)));
            // means to switch to "BACK QUOTED IDENTIFIER" state to support `<IDENTIFIER>` in
            // Flink SQL. Please cc CalciteParser#createFlinkParser for more details.
            this.tokenManager.SwitchTo(2);
            this.currentToken = tokenManager.getNextToken();
        }

        @Override
        public boolean hasNext() {
            return currentToken.kind != EOF;
        }

        @Override
        public Token next() {
            Token before = currentToken;
            currentToken = scan(1);
            return before;
        }

        public Token scan(int pos) {
            Token current = currentToken;
            while (pos-- > 0) {
                if (current.next == null) {
                    current.next = tokenManager.getNextToken();
                }
                current = current.next;
            }

            return current;
        }
    }

    private Optional<StatementType> getStatementType(TokenIterator tokens) {
        if (!tokens.hasNext()) {
            continueReadInput();
        }
        Token firstToken = tokens.scan(0);
        StatementType type;
        if (firstToken.kind == IDENTIFIER) {
            // it means the token is not a reserved keyword, potentially a client command
            type = getPotentialCommandType(firstToken.image);
        } else if (firstToken.kind == SET) {
            // SET
            type = StatementType.SET;
        } else if (firstToken.kind == RESET) {
            // RESET
            type = StatementType.RESET;
        } else if (firstToken.kind == EXPLAIN) {
            // EXPLAIN
            type = StatementType.EXPLAIN;
        } else if (firstToken.kind == SHOW) {
            // SHOW CREATE
            type =
                    tokenMatches(tokens.scan(1), CREATE)
                            ? StatementType.SHOW_CREATE
                            : StatementType.OTHER;
        } else if (firstToken.kind == BEGIN) {
            // BEGIN STATEMENT SET
            type =
                    tokenMatches(tokens.scan(1), STATEMENT) && tokenMatches(tokens.scan(2), SET)
                            ? StatementType.BEGIN_STATEMENT_SET
                            : StatementType.OTHER;
        } else if (firstToken.kind == END) {
            // END
            type =
                    tokenMatches(tokens.scan(1), SEMICOLON)
                            ? StatementType.END
                            : StatementType.OTHER;
        } else if (firstToken.kind == REMOVE) {
            // REMOVE JAR
            type =
                    tokenMatches(tokens.scan(1), JAR)
                            ? StatementType.REMOVE_JAR
                            : StatementType.OTHER;
        } else if (firstToken.kind == SELECT) {
            // SELECT
            type = StatementType.SELECT;
        } else {
            type = StatementType.OTHER;
        }

        checkIncompleteStatement(tokens);
        return Optional.of(type);
    }

    private static void continueReadInput() {
        // throw this to notify the terminal to continue reading input
        throw new SqlExecutionException(
                "The SQL statement is incomplete.",
                new SqlParserEOFException("The SQL statement is incomplete."));
    }

    private boolean tokenMatches(Token token, int kind) {
        return token.kind == kind;
    }

    private StatementType getPotentialCommandType(String image) {
        switch (image.toUpperCase()) {
            case "QUIT":
            case "EXIT":
                return StatementType.QUIT;
            case "CLEAR":
                return StatementType.CLEAR;
            case "HELP":
                return StatementType.HELP;
            default:
                return StatementType.OTHER;
        }
    }

    private void checkIncompleteStatement(TokenIterator iterator) {
        Token before = iterator.next(), current = iterator.next();

        while (!tokenMatches(current, EOF)) {
            before = current;
            current = iterator.next();
        }

        if (!(tokenMatches(before, SEMICOLON) && tokenMatches(current, EOF))) {
            // not ended with ;
            continueReadInput();
        }
    }
}
