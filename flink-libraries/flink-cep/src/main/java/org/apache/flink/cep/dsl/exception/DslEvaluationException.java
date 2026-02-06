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
 * Exception thrown during runtime evaluation of DSL expressions.
 *
 * <p>This exception is thrown when an expression that compiled successfully fails during runtime
 * evaluation, typically due to type mismatches or missing attributes.
 *
 * <p>Common causes:
 *
 * <ul>
 *   <li>Type mismatch (e.g., comparing a string with a number)
 *   <li>Missing attributes on events
 *   <li>Null values in comparisons
 *   <li>Referenced pattern not found in context
 * </ul>
 */
@PublicEvolving
public class DslEvaluationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String expression;
    private final Object event;

    /**
     * Create an evaluation exception with a simple message.
     *
     * @param message The error message
     */
    public DslEvaluationException(String message) {
        super(message);
        this.expression = null;
        this.event = null;
    }

    /**
     * Create an evaluation exception with a message and cause.
     *
     * @param message The error message
     * @param cause The underlying cause
     */
    public DslEvaluationException(String message, Throwable cause) {
        super(message, cause);
        this.expression = null;
        this.event = null;
    }

    /**
     * Create an evaluation exception with expression and event context.
     *
     * @param message The error message
     * @param expression The expression that failed
     * @param event The event being evaluated
     */
    public DslEvaluationException(String message, String expression, Object event) {
        super(formatMessage(message, expression, event));
        this.expression = expression;
        this.event = event;
    }

    /**
     * Create an evaluation exception with expression, event, and cause.
     *
     * @param message The error message
     * @param expression The expression that failed
     * @param event The event being evaluated
     * @param cause The underlying cause
     */
    public DslEvaluationException(
            String message, String expression, Object event, Throwable cause) {
        super(formatMessage(message, expression, event), cause);
        this.expression = expression;
        this.event = event;
    }

    /**
     * Get the expression that failed.
     *
     * @return The expression, or null if not available
     */
    public String getExpression() {
        return expression;
    }

    /**
     * Get the event that was being evaluated.
     *
     * @return The event, or null if not available
     */
    public Object getEvent() {
        return event;
    }

    private static String formatMessage(String message, String expression, Object event) {
        StringBuilder sb = new StringBuilder();
        sb.append("DSL evaluation error: ").append(message);

        if (expression != null) {
            sb.append("\n  Expression: ").append(expression);
        }

        if (event != null) {
            sb.append("\n  Event: ").append(event);
        }

        return sb.toString();
    }
}
