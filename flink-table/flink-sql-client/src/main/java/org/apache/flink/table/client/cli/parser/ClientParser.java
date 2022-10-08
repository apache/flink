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
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.COMPILE;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.CREATE;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.END;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.EOF;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.EXECUTE;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.EXPLAIN;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.IDENTIFIER;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.JAR;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.REMOVE;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.RESET;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.SEMICOLON;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.SET;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.SHOW;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.STATEMENT;

/**
 * ClientParser use {@link FlinkSqlParserImplTokenManager} to do lexical analysis. It cannot
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

    private static class TokenIterator implements Iterator<Token> {

        private final FlinkSqlParserImplTokenManager tokenManager;
        private Token currentToken;

        public TokenIterator(String statement) {
            tokenManager =
                    new FlinkSqlParserImplTokenManager(
                            new SimpleCharStream(new StringReader(statement)));
            // means to switch to "BACK QUOTED IDENTIFIER" state to support '`xxx`' in Flink SQL
            tokenManager.SwitchTo(2);
            this.currentToken = tokenManager.getNextToken();
            // this will make all tokens link together
            checkIncompleteStatement();
        }

        @Override
        public boolean hasNext() {
            return currentToken.kind != EOF;
        }

        @Override
        public Token next() {
            Token before = currentToken;
            currentToken = before.next;
            return before;
        }

        boolean nextTokenMatched(int kind) {
            return hasNext() && next().kind == kind;
        }

        /**
         * check these follow special cases: 1. "". 2. "COMPILE/EXECUTE STATEMENT SET BEGIN\n INSERT
         * xxx;". 3. "EXPLAIN COMPILE/EXECUTE STATEMENT SET BEGIN\n INSERT xxx;". 4. "EXPLAIN BEGIN
         * STATEMENT SET;". 5. string not ended with ';'.
         */
        void checkIncompleteStatement() {
            // case 1
            if (currentToken.kind == EOF) {
                continueReadInput();
            }

            Token head = currentToken, tail = tokenManager.getNextToken();

            // case 2, 3 and 4
            boolean setNotEnded =
                    (currentToken.kind == COMPILE || currentToken.kind == EXECUTE)
                            || (currentToken.kind == EXPLAIN
                                    && (tail.kind == COMPILE
                                            || tail.kind == EXECUTE
                                            || tail.kind == BEGIN));

            // case 5
            boolean statementNotEnded = head.kind != SEMICOLON && tail.kind != SEMICOLON;

            while (tail.kind != EOF) {
                head.next = tail;
                head = tail;
                // update setEnded
                if (tail.kind == END) {
                    setNotEnded = false;
                }
                // update endedWithSemicolon
                statementNotEnded = tail.kind != SEMICOLON;
                tail = tokenManager.getNextToken();
            }

            // hasNext() needs an EOF tail token
            head.next = tail;

            if (setNotEnded || statementNotEnded) {
                continueReadInput();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    private Optional<StatementType> getStatementType(TokenIterator tokens) {
        Token firstToken = tokens.next();
        if (firstToken.kind == SEMICOLON && !tokens.hasNext()) {
            return Optional.empty();
        }

        if (firstToken.kind == IDENTIFIER) {
            // it means the token is not a reserved keyword, potentially a client command
            return getPotentialCommandType(firstToken.image);
        } else if (firstToken.kind == SET) {
            return Optional.of(StatementType.SET);
        } else if (firstToken.kind == RESET) {
            return Optional.of(StatementType.RESET);
        } else if (firstToken.kind == EXPLAIN) {
            return Optional.of(StatementType.EXPLAIN);
        } else if (firstToken.kind == SHOW) {
            return Optional.of(
                    tokens.nextTokenMatched(CREATE)
                            ? StatementType.SHOW_CREATE
                            : StatementType.OTHER);
        } else if (firstToken.kind == BEGIN) {
            return Optional.of(
                    tokens.nextTokenMatched(STATEMENT) && tokens.nextTokenMatched(SET)
                            ? StatementType.BEGIN_STATEMENT_SET
                            : StatementType.OTHER);
        } else if (firstToken.kind == END) {
            return Optional.of(
                    tokens.nextTokenMatched(SEMICOLON) ? StatementType.END : StatementType.OTHER);
        } else if (firstToken.kind == REMOVE) {
            return Optional.of(
                    tokens.nextTokenMatched(JAR) ? StatementType.REMOVE_JAR : StatementType.OTHER);
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

    private static void continueReadInput() {
        // throw this to notify the terminal to continue reading input
        throw new SqlExecutionException(
                "The SQL statement is incomplete.",
                new SqlParserEOFException("The SQL statement is incomplete."));
    }
}
