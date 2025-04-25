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

package org.apache.flink.table.gateway.service.application;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Executor to execute the statements. */
public class ScriptExecutor {

    final SessionContext context;
    final Printer printer;

    private ScriptExecutor(SessionContext context) {
        this(context, new Printer());
    }

    public ScriptExecutor(SessionContext context, Printer printer) {
        this.context = context;
        this.printer = printer;
    }

    public static ScriptExecutor of(SessionContext context) {
        return new ScriptExecutor(context);
    }

    public void execute(String script) {
        ResultIterator iterator = new ResultIterator(script);
        try {
            while (iterator.hasNext()) {
                Result result = iterator.next();
                printer.print(result.statement);
                if (result.error != null) {
                    throw result.error;
                } else {
                    printer.print(checkNotNull(result.fetcher));
                }
            }
        } catch (Throwable t) {
            printer.print(t);
            throw new SqlGatewayException("Failed to execute the script.", t);
        }
    }

    class ResultIterator implements Iterator<Result> {

        // these 3 string builders is here to pad the split sql to its original line and column
        // number
        StringBuilder previousPaddingSqlBuilder = new StringBuilder();
        StringBuilder currentPaddingSqlBuilder = new StringBuilder();
        StringBuilder currentPaddingLineBuilder = new StringBuilder();

        private final String script;
        private int position;

        private String statement;
        private Throwable throwable;
        private OperationExecutor executor;

        public ResultIterator(String script) {
            this.script = script;
            this.position = 0;
        }

        @Override
        public boolean hasNext() {
            State state = State.NORMAL;
            StringBuilder currentSqlBuilder = new StringBuilder();
            char currentChar = 0;

            boolean hasNext = false;
            // rebuild the executor because statement may change planner.
            executor =
                    new OperationExecutor(
                            context,
                            (config, classloader) ->
                                    StreamExecutionEnvironment.getExecutionEnvironment(config));
            for (int i = position; i < script.length(); i++) {
                char lastChar = currentChar;
                currentChar = script.charAt(i);

                currentSqlBuilder.append(currentChar);
                currentPaddingLineBuilder.append(" ");

                switch (currentChar) {
                    case '\'':
                        if (state == State.SINGLE_QUOTE) {
                            state = State.NORMAL;
                        } else if (state == State.NORMAL) {
                            state = State.SINGLE_QUOTE;
                        }
                        break;
                    case '"':
                        if (state == State.DOUBLE_QUOTE) {
                            state = State.NORMAL;
                        } else if (state == State.NORMAL) {
                            state = State.DOUBLE_QUOTE;
                        }
                        break;
                    case '`':
                        if (state == State.BACK_QUOTE) {
                            state = State.NORMAL;
                        } else if (state == State.NORMAL) {
                            state = State.BACK_QUOTE;
                        }
                        break;
                    case '-':
                        if (lastChar == '-' && state == State.NORMAL) {
                            state = State.SINGLE_COMMENT;
                        }
                        break;
                    case '\n':
                        if (state == State.SINGLE_COMMENT) {
                            state = State.NORMAL;
                        }
                        currentPaddingLineBuilder.setLength(0);
                        currentPaddingSqlBuilder.append("\n");
                        break;
                    case '*':
                        if (lastChar == '/' && state == State.NORMAL) {
                            state = State.MULTI_LINE_COMMENT;
                        }
                        break;
                    case '/':
                        if (lastChar == '*' && state == State.MULTI_LINE_COMMENT) {
                            state = State.NORMAL;
                        }
                        break;
                    case ';':
                        if (state == State.NORMAL) {
                            i =
                                    prefetch(
                                            i + 1,
                                            currentSqlBuilder,
                                            currentPaddingSqlBuilder,
                                            currentPaddingLineBuilder);
                            statement = currentSqlBuilder.toString();
                            try {
                                position = i + 1;
                                parse(previousPaddingSqlBuilder + statement);
                            } catch (SqlParserEOFException e) {
                                if (i == script.length() - 1) {
                                    throwable = e;
                                } else {
                                    // keep reading
                                    continue;
                                }
                            } catch (Throwable t) {
                                throwable = t;
                            }

                            hasNext = true;
                            previousPaddingSqlBuilder.append(currentPaddingSqlBuilder);
                            previousPaddingSqlBuilder.append(currentPaddingLineBuilder);
                            currentPaddingSqlBuilder.setLength(0);
                            currentPaddingLineBuilder.setLength(0);
                        }
                        break;
                    default:
                        break;
                }

                if (hasNext) {
                    return true;
                }
            }
            position = script.length();

            statement = currentSqlBuilder.toString();
            if (!StringUtils.isNullOrWhitespaceOnly(statement)) {
                parse(previousPaddingSqlBuilder + statement);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Result next() {
            if (throwable != null) {
                // clear the exception
                Throwable t = throwable;
                throwable = null;
                return new Result(statement, t);
            }
            try {
                return new Result(
                        statement, executor.executeStatement(OperationHandle.create(), statement));
            } catch (Throwable t) {
                return new Result(statement, t);
            }
        }

        /**
         * Prefetch characters until the character is not semicolon or white space.
         *
         * @param begin the next token to fetch
         * @param currentSqlBuilder current fetched sql
         * @param currentPaddingSqlBuilder current padding script
         * @param currentPaddingLineBuilder the current padding line
         * @return the last fetched character
         */
        private int prefetch(
                int begin,
                StringBuilder currentSqlBuilder,
                StringBuilder currentPaddingSqlBuilder,
                StringBuilder currentPaddingLineBuilder) {
            State state = State.NORMAL;
            char currentChar;
            for (int i = begin; i < script.length(); i++) {
                currentChar = script.charAt(i);
                char nextChar = i + 1 < script.length() ? script.charAt(i + 1) : currentChar;

                switch (currentChar) {
                    case '-':
                        if (nextChar == '-' && state == State.NORMAL) {
                            state = State.SINGLE_COMMENT;
                        }
                        break;
                    case '\n':
                        if (state == State.SINGLE_COMMENT) {
                            state = State.NORMAL;
                        }
                        break;
                    case '*':
                        if (nextChar == '/' && state == State.MULTI_LINE_COMMENT) {
                            state = State.NORMAL;
                            currentSqlBuilder.append("*/");
                            currentPaddingLineBuilder.append("  ");
                            i = i + 1;
                            continue;
                        }
                        break;
                    case '/':
                        if (nextChar == '*' && state == State.NORMAL) {
                            state = State.MULTI_LINE_COMMENT;
                        }
                        break;
                }

                if (state == State.NORMAL
                        && currentChar != ';'
                        && !Character.isWhitespace(currentChar)) {
                    return i - 1;
                }

                currentSqlBuilder.append(currentChar);
                if (currentChar == '\n') {
                    currentPaddingLineBuilder.setLength(0);
                    currentPaddingSqlBuilder.append("\n");
                } else {
                    currentPaddingLineBuilder.append(" ");
                }
            }
            return script.length() - 1;
        }

        private void parse(String statement) {
            List<Operation> operations =
                    executor.getTableEnvironment().getParser().parse(statement);
            if (operations.size() != 1) {
                throw new SqlGatewayException(
                        "Unsupported SQL query! Something unexpected happens. Please report an issue to the Flink Community.");
            }
        }
    }

    static class Result {

        final String statement;
        final @Nullable ResultFetcher fetcher;
        final @Nullable Throwable error;

        public Result(String statement, ResultFetcher fetcher) {
            this(statement, fetcher, null);
        }

        public Result(String statement, Throwable error) {
            this(statement, null, error);
        }

        private Result(
                String statement, @Nullable ResultFetcher fetcher, @Nullable Throwable error) {
            this.statement = statement;
            this.fetcher = fetcher;
            this.error = error;
        }
    }

    enum State {
        SINGLE_QUOTE, // '

        DOUBLE_QUOTE, // "

        BACK_QUOTE, // `

        SINGLE_COMMENT, // --

        MULTI_LINE_COMMENT, /* */

        NORMAL
    }
}
