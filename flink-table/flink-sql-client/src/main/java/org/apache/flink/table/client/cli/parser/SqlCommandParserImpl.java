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
import org.apache.flink.sql.parser.impl.TokenMgrError;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.gateway.SqlExecutionException;

import java.io.StringReader;
import java.util.Iterator;
import java.util.Optional;

import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.EOF;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.IDENTIFIER;
import static org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants.SEMICOLON;

/**
 * The {@link SqlCommandParserImpl} uses {@link FlinkSqlParserImplTokenManager} to do lexical
 * analysis. It cannot recognize special hive keywords yet because Hive has a slightly different
 * vocabulary compared to Flink's, which causes the {@link SqlCommandParserImpl} misunderstanding
 * some Hive's keywords to IDENTIFIER. But the ClientParser is only responsible to check whether the
 * statement is completed or not and only cares about a few statements. So it's acceptable to
 * tolerate the inaccuracy here.
 */
public class SqlCommandParserImpl implements SqlCommandParser {

    public Optional<Command> parseStatement(String statement) throws SqlExecutionException {
        // normalize
        statement = statement.trim();
        // meet empty statement, e.g ";\n"
        if (statement.isEmpty() || statement.equals(";")) {
            return Optional.empty();
        } else {
            return Optional.of(getCommand(new TokenIterator(statement)));
        }
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
                    try {
                        current.next = tokenManager.getNextToken();
                    } catch (TokenMgrError tme) {
                        throw new SqlExecutionException(
                                "SQL parse failed. " + tme.getMessage(), tme);
                    }
                }
                current = current.next;
            }

            return current;
        }
    }

    private Command getCommand(TokenIterator tokens) {
        if (!tokens.hasNext()) {
            continueReadInput();
        }
        Token firstToken = tokens.scan(0);
        Command type;
        if (firstToken.kind == IDENTIFIER) {
            // it means the token is not a reserved keyword, potentially a client command
            type = getPotentialCommandType(firstToken.image);
        } else {
            type = Command.OTHER;
        }

        checkIncompleteStatement(tokens);
        return type;
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

    private Command getPotentialCommandType(String image) {
        switch (image.toUpperCase()) {
            case "QUIT":
            case "EXIT":
                return Command.QUIT;
            case "CLEAR":
                return Command.CLEAR;
            case "HELP":
                return Command.HELP;
            default:
                return Command.OTHER;
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
