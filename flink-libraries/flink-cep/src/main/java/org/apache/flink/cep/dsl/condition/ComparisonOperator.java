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

import java.math.BigDecimal;

/**
 * Comparison operators supported by the CEP DSL.
 *
 * <p>This enum provides type-safe comparison operations for DSL expressions. All numeric
 * comparisons are performed using {@link BigDecimal} to ensure consistent precision across
 * different numeric types.
 */
@Internal
public enum ComparisonOperator {
    EQUALS("=") {
        @Override
        public boolean evaluate(Object left, Object right) {
            if (left == null || right == null) {
                return left == right;
            }
            return left.equals(right);
        }
    },

    NOT_EQUALS("!=") {
        @Override
        public boolean evaluate(Object left, Object right) {
            return !EQUALS.evaluate(left, right);
        }
    },

    LESS_THAN("<") {
        @Override
        public boolean evaluate(Object left, Object right) {
            return compareNumbers(left, right) < 0;
        }
    },

    LESS_THAN_OR_EQUAL("<=") {
        @Override
        public boolean evaluate(Object left, Object right) {
            return compareNumbers(left, right) <= 0;
        }
    },

    GREATER_THAN(">") {
        @Override
        public boolean evaluate(Object left, Object right) {
            return compareNumbers(left, right) > 0;
        }
    },

    GREATER_THAN_OR_EQUAL(">=") {
        @Override
        public boolean evaluate(Object left, Object right) {
            return compareNumbers(left, right) >= 0;
        }
    };

    private final String symbol;

    ComparisonOperator(String symbol) {
        this.symbol = symbol;
    }

    /**
     * Get the symbol representation of this operator.
     *
     * @return The operator symbol (e.g., "=", "!=", "<", etc.)
     */
    public String getSymbol() {
        return symbol;
    }

    /**
     * Evaluate the comparison between two values.
     *
     * @param left The left-hand side value
     * @param right The right-hand side value
     * @return true if the comparison holds, false otherwise
     * @throws IllegalArgumentException if numeric comparison is attempted on non-numeric types
     */
    public abstract boolean evaluate(Object left, Object right);

    /**
     * Find an operator by its symbol.
     *
     * @param symbol The operator symbol to search for
     * @return The matching operator
     * @throws IllegalArgumentException if no operator with the given symbol exists
     */
    public static ComparisonOperator fromSymbol(String symbol) {
        for (ComparisonOperator op : values()) {
            if (op.symbol.equals(symbol)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown operator symbol: " + symbol);
    }

    /**
     * Compare two numbers using BigDecimal for consistent precision.
     *
     * @param left The left-hand side value (must be a Number)
     * @param right The right-hand side value (must be a Number)
     * @return negative if left < right, zero if equal, positive if left > right
     * @throws IllegalArgumentException if either value is not a Number
     */
    private static int compareNumbers(Object left, Object right) {
        if (!(left instanceof Number) || !(right instanceof Number)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot compare non-numeric values: %s (%s) and %s (%s)",
                            left,
                            left != null ? left.getClass().getName() : "null",
                            right,
                            right != null ? right.getClass().getName() : "null"));
        }

        BigDecimal leftBd = toBigDecimal((Number) left);
        BigDecimal rightBd = toBigDecimal((Number) right);
        return leftBd.compareTo(rightBd);
    }

    /**
     * Convert a Number to BigDecimal with appropriate precision.
     *
     * @param number The number to convert
     * @return The number as a BigDecimal
     */
    private static BigDecimal toBigDecimal(Number number) {
        if (number instanceof BigDecimal) {
            return (BigDecimal) number;
        } else if (number instanceof Double || number instanceof Float) {
            // Use string conversion to avoid precision issues
            return new BigDecimal(number.toString());
        } else {
            // Integer, Long, Short, Byte
            return BigDecimal.valueOf(number.longValue());
        }
    }
}
