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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.Internal;

import java.util.Optional;

/** Utility methods for working with {@link Expression}s. */
@Internal
public final class ExpressionUtils {

    /**
     * Extracts the value (excluding null) of a given class from an expression assuming it is a
     * {@link ValueLiteralExpression}.
     *
     * @param expression literal to extract the value from
     * @param targetClass expected class to extract from the literal
     * @param <V> type of extracted value
     * @return extracted value or empty if could not extract value of given type
     */
    public static <V> Optional<V> extractValue(Expression expression, Class<V> targetClass) {
        if (expression instanceof ValueLiteralExpression) {
            final ValueLiteralExpression valueLiteral = (ValueLiteralExpression) expression;
            return valueLiteral.getValueAs(targetClass);
        }
        return Optional.empty();
    }

    private ExpressionUtils() {}
}
