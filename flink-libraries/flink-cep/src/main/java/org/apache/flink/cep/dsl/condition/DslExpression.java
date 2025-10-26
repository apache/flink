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

import java.io.Serializable;
import java.util.Optional;

/**
 * Represents a single condition expression in the DSL.
 *
 * <p>An expression consists of:
 * <ul>
 *   <li>An attribute name (e.g., "temperature")
 *   <li>A comparison operator (e.g., >, <, =)
 *   <li>Either a constant value (e.g., 100) or a reference to another event (e.g., A.id)
 * </ul>
 *
 * <p>Examples:
 * <ul>
 *   <li>temperature > 100 (constant comparison)
 *   <li>A.id = B.id (event correlation)
 *   <li>status != 'error' (string comparison)
 * </ul>
 */
@Internal
public class DslExpression<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DslExpression.class);

    private final String attribute;
    private final ComparisonOperator operator;
    private final Object constantValue;
    private final String referencePattern;
    private final String referenceAttribute;

    /**
     * Create an expression for constant comparison (e.g., temperature > 100).
     *
     * @param attribute The attribute name to compare
     * @param operator The comparison operator
     * @param value The constant value to compare against
     */
    public DslExpression(String attribute, ComparisonOperator operator, Object value) {
        this.attribute = attribute;
        this.operator = operator;
        this.constantValue = value;
        this.referencePattern = null;
        this.referenceAttribute = null;
    }

    /**
     * Create an expression for event correlation (e.g., A.id = B.id).
     *
     * @param attribute The attribute name on the current event
     * @param operator The comparison operator
     * @param refPattern The name of the pattern to reference
     * @param refAttribute The attribute name on the referenced pattern
     */
    public DslExpression(
            String attribute,
            ComparisonOperator operator,
            String refPattern,
            String refAttribute) {
        this.attribute = attribute;
        this.operator = operator;
        this.constantValue = null;
        this.referencePattern = refPattern;
        this.referenceAttribute = refAttribute;
    }

    /**
     * Evaluate this expression against an event and pattern context.
     *
     * @param event The current event to evaluate
     * @param adapter The event adapter for attribute extraction
     * @param context The pattern context providing access to previously matched events
     * @return true if the expression evaluates to true, false otherwise
     */
    public boolean evaluate(
            T event, EventAdapter<T> adapter, IterativeCondition.Context<T> context) {

        // Get left-hand side value from current event
        Optional<Object> leftValue = adapter.getAttribute(event, attribute);
        if (!leftValue.isPresent()) {
            LOG.debug(
                    "Attribute '{}' not found on event, expression evaluates to false", attribute);
            return false;
        }

        // Get right-hand side value
        Object rightValue;
        if (referencePattern != null) {
            // Event correlation: get value from referenced pattern
            rightValue = getReferenceValue(context, adapter);
            if (rightValue == null) {
                LOG.debug(
                        "Reference attribute '{}.{}' not found in context, expression evaluates to false",
                        referencePattern,
                        referenceAttribute);
                return false;
            }
        } else {
            // Constant comparison
            rightValue = constantValue;
        }

        // Perform comparison
        try {
            boolean result = operator.evaluate(leftValue.get(), rightValue);
            LOG.trace(
                    "Expression evaluation: {} {} {} = {}",
                    leftValue.get(),
                    operator.getSymbol(),
                    rightValue,
                    result);
            return result;
        } catch (Exception e) {
            LOG.warn(
                    "Error evaluating expression: {} {} {}",
                    leftValue.get(),
                    operator.getSymbol(),
                    rightValue,
                    e);
            return false;
        }
    }

    /**
     * Get the value of a referenced attribute from the pattern context.
     *
     * @param context The pattern context
     * @param adapter The event adapter
     * @return The referenced value, or null if not found
     */
    private Object getReferenceValue(
            IterativeCondition.Context<T> context, EventAdapter<T> adapter) {
        try {
            Iterable<T> events = context.getEventsForPattern(referencePattern);
            for (T event : events) {
                Optional<Object> value = adapter.getAttribute(event, referenceAttribute);
                if (value.isPresent()) {
                    return value.get();
                }
            }
        } catch (Exception e) {
            LOG.warn(
                    "Error accessing reference pattern '{}' in context",
                    referencePattern,
                    e);
        }
        return null;
    }

    @Override
    public String toString() {
        if (referencePattern != null) {
            return String.format(
                    "%s %s %s.%s",
                    attribute, operator.getSymbol(), referencePattern, referenceAttribute);
        } else {
            return String.format("%s %s %s", attribute, operator.getSymbol(), constantValue);
        }
    }
}
