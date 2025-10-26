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

package org.apache.flink.cep.dsl.condition;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cep.dsl.api.EventAdapter;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink CEP condition implementation for DSL expressions.
 *
 * <p>This condition evaluates a list of {@link DslExpression}s combined with either AND or OR
 * logic. It also supports optional event type matching when strict type matching is enabled.
 *
 * <p>Features:
 * <ul>
 *   <li>Short-circuit evaluation (AND stops on first false, OR stops on first true)
 *   <li>Event type filtering (optional)
 *   <li>Support for complex nested expressions
 *   <li>Event correlation across patterns
 * </ul>
 */
@Internal
public class DslCondition<T> extends IterativeCondition<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DslCondition.class);

    private final List<DslExpression<T>> expressions;
    private final LogicalOperator logicalOperator;
    private final EventAdapter<T> eventAdapter;
    private final String eventTypePattern;

    /**
     * Create a condition with only event type matching (no expressions).
     *
     * @param eventAdapter The event adapter for attribute extraction
     * @param eventTypePattern The expected event type (null to skip type checking)
     */
    public DslCondition(EventAdapter<T> eventAdapter, String eventTypePattern) {
        this(eventAdapter, eventTypePattern, new ArrayList<>(), LogicalOperator.AND);
    }

    /**
     * Create a condition with expressions and optional event type matching.
     *
     * @param eventAdapter The event adapter for attribute extraction
     * @param eventTypePattern The expected event type (null to skip type checking)
     * @param expressions The list of expressions to evaluate
     * @param logicalOperator The logical operator combining expressions (AND or OR)
     */
    public DslCondition(
            EventAdapter<T> eventAdapter,
            String eventTypePattern,
            List<DslExpression<T>> expressions,
            LogicalOperator logicalOperator) {
        this.eventAdapter = eventAdapter;
        this.eventTypePattern = eventTypePattern;
        this.expressions = expressions;
        this.logicalOperator = logicalOperator;
    }

    @Override
    public boolean filter(T event, Context<T> context) throws Exception {
        // Step 1: Check event type if specified
        if (eventTypePattern != null) {
            String actualType = eventAdapter.getEventType(event);
            if (!matchesEventType(actualType, eventTypePattern)) {
                LOG.trace(
                        "Event type mismatch: expected '{}', got '{}'",
                        eventTypePattern,
                        actualType);
                return false;
            }
        }

        // Step 2: Evaluate expressions
        if (expressions.isEmpty()) {
            // No expressions means accept all events (of the right type)
            return true;
        }

        return evaluateExpressions(event, context);
    }

    /**
     * Evaluate all expressions with the configured logical operator.
     *
     * @param event The event to evaluate
     * @param context The pattern context
     * @return true if expressions evaluate to true according to the logical operator
     */
    private boolean evaluateExpressions(T event, Context<T> context) {
        if (logicalOperator == LogicalOperator.AND) {
            // Short-circuit AND: stop on first false
            for (DslExpression<T> expr : expressions) {
                if (!expr.evaluate(event, eventAdapter, context)) {
                    LOG.trace("AND expression failed: {}", expr);
                    return false;
                }
            }
            LOG.trace("All AND expressions passed ({} expressions)", expressions.size());
            return true;
        } else {
            // Short-circuit OR: stop on first true
            for (DslExpression<T> expr : expressions) {
                if (expr.evaluate(event, eventAdapter, context)) {
                    LOG.trace("OR expression succeeded: {}", expr);
                    return true;
                }
            }
            LOG.trace("All OR expressions failed ({} expressions)", expressions.size());
            return false;
        }
    }

    /**
     * Check if the actual event type matches the expected pattern.
     *
     * <p>Uses case-insensitive comparison.
     *
     * @param actualType The actual event type from the event
     * @param pattern The expected event type pattern
     * @return true if types match
     */
    private boolean matchesEventType(String actualType, String pattern) {
        if (actualType == null || pattern == null) {
            return false;
        }
        return actualType.equalsIgnoreCase(pattern);
    }

    /** Logical operator for combining expressions. */
    public enum LogicalOperator {
        /** All expressions must be true. */
        AND,
        /** At least one expression must be true. */
        OR
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (eventTypePattern != null) {
            sb.append("EventType=").append(eventTypePattern);
            if (!expressions.isEmpty()) {
                sb.append(" AND ");
            }
        }
        if (!expressions.isEmpty()) {
            sb.append("(");
            for (int i = 0; i < expressions.size(); i++) {
                if (i > 0) {
                    sb.append(" ").append(logicalOperator).append(" ");
                }
                sb.append(expressions.get(i));
            }
            sb.append(")");
        }
        return sb.toString();
    }
}
