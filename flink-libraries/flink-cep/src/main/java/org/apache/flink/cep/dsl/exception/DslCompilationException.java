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

package org.apache.flink.cep.dsl.exception;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Exception thrown when DSL compilation fails.
 *
 * <p>This exception is thrown during the parsing and translation phase when the DSL expression
 * contains syntax errors or semantic issues that prevent it from being compiled into a valid Flink
 * Pattern.
 *
 * <p>Common causes:
 * <ul>
 *   <li>Invalid syntax (e.g., unmatched parentheses, invalid operators)
 *   <li>Unknown pattern names in event correlation
 *   <li>Invalid quantifiers or time window specifications
 * </ul>
 */
@PublicEvolving
public class DslCompilationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int line;
    private final int column;
    private final String dslFragment;

    /**
     * Create a compilation exception with a simple message.
     *
     * @param message The error message
     */
    public DslCompilationException(String message) {
        super(message);
        this.line = -1;
        this.column = -1;
        this.dslFragment = null;
    }

    /**
     * Create a compilation exception with a message and cause.
     *
     * @param message The error message
     * @param cause The underlying cause
     */
    public DslCompilationException(String message, Throwable cause) {
        super(message, cause);
        this.line = -1;
        this.column = -1;
        this.dslFragment = null;
    }

    /**
     * Create a compilation exception with location information.
     *
     * @param message The error message
     * @param line The line number where the error occurred (0-based)
     * @param column The column number where the error occurred (0-based)
     * @param dslFragment The fragment of DSL that caused the error
     */
    public DslCompilationException(String message, int line, int column, String dslFragment) {
        super(formatMessage(message, line, column, dslFragment));
        this.line = line;
        this.column = column;
        this.dslFragment = dslFragment;
    }

    /**
     * Get the line number where the error occurred.
     *
     * @return The line number (0-based), or -1 if not available
     */
    public int getLine() {
        return line;
    }

    /**
     * Get the column number where the error occurred.
     *
     * @return The column number (0-based), or -1 if not available
     */
    public int getColumn() {
        return column;
    }

    /**
     * Get the DSL fragment that caused the error.
     *
     * @return The DSL fragment, or null if not available
     */
    public String getDslFragment() {
        return dslFragment;
    }

    private static String formatMessage(String message, int line, int column, String fragment) {
        StringBuilder sb = new StringBuilder();
        sb.append("DSL compilation error at line ").append(line + 1);
        sb.append(", column ").append(column + 1).append(": ");
        sb.append(message);

        if (fragment != null && !fragment.isEmpty()) {
            sb.append("\n  ").append(fragment);
            sb.append("\n  ");
            for (int i = 0; i < column; i++) {
                sb.append(" ");
            }
            sb.append("^");
        }

        return sb.toString();
    }
}
