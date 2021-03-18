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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.operations.Operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Parser that uses {@link StatementParseStrategy} to parse statement to {@link Operation}. */
public class ParseStrategyParser {

    public static final ParseStrategyParser INSTANCE = new ParseStrategyParser();

    private static final List<StatementParseStrategy> REGEX_STRATEGIES =
            Arrays.asList(
                    ClearOperationParseStrategy.INSTANCE,
                    HelpOperationParseStrategy.INSTANCE,
                    QuitOperationParseStrategy.INSTANCE,
                    ResetOperationParseStrategy.INSTANCE,
                    SetOperationParseStrategy.INSTANCE,
                    SourceOperationParseStrategy.INSTANCE);

    /**
     * Determine whether the input statement matches any {@link StatementParseStrategy}.
     *
     * @param statement that command to evaluate
     * @return whether this statement matches the strategy
     */
    public boolean matches(String statement) {
        for (StatementParseStrategy strategy : REGEX_STRATEGIES) {
            if (strategy.match(statement)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Convert the input statement to the {@link Operation}.
     *
     * @param statement the command to evaluate
     * @return parsed operation that represents the command
     */
    public Operation convert(String statement) {
        for (StatementParseStrategy strategy : REGEX_STRATEGIES) {
            if (strategy.match(statement)) {
                return strategy.convert(statement);
            }
        }
        throw new TableException(
                String.format("ParseStrategyParser fails to parse the statement: %s.", statement));
    }

    /**
     * Returns completion hints for the given statement at the given cursor position. The completion
     * happens case insensitively.
     *
     * @param statement Partial or slightly incorrect SQL statement
     * @param cursor cursor position
     * @return completion hints that fit at the current cursor position
     */
    public String[] getCompletionHints(String statement, int cursor) {
        String normalizedStatement = statement.trim().toUpperCase();
        List<String> hints = new ArrayList<>();
        for (StatementParseStrategy strategy : REGEX_STRATEGIES) {
            for (String hint : strategy.getHints()) {
                if (hint.startsWith(normalizedStatement) && cursor < hint.length()) {
                    hints.add(getCompletionHint(normalizedStatement, hint));
                }
            }
        }

        return hints.toArray(new String[0]);
    }

    private String getCompletionHint(String statement, String commandHint) {
        if (statement.length() == 0) {
            return commandHint;
        }
        int cursorPos = statement.length() - 1;
        int returnStartPos;
        if (Character.isWhitespace(commandHint.charAt(cursorPos + 1))) {
            returnStartPos = Math.min(commandHint.length() - 1, cursorPos + 2);
        } else {
            // 'add ja' should return 'jar'
            returnStartPos = cursorPos;
            while (returnStartPos > 0
                    && !Character.isWhitespace(commandHint.charAt(returnStartPos - 1))) {
                returnStartPos--;
            }
        }

        return commandHint.substring(returnStartPos);
    }
}
