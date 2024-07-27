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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.table.expressions.ValueLiteralExpression;

/** Utility functions that can be used for writing {@link SqlCallSyntax}. */
@Internal
class CallSyntaxUtils {

    /**
     * Converts the given {@link ResolvedExpression} into a SQL string. Wraps the string with
     * parenthesis if the expression is not a leaf expression such as e.g. {@link
     * ValueLiteralExpression} or {@link FieldReferenceExpression}.
     */
    static String asSerializableOperand(ResolvedExpression expression) {
        if (expression.getResolvedChildren().isEmpty()) {
            return expression.asSerializableString();
        }

        return String.format("(%s)", expression.asSerializableString());
    }

    static <T extends TableSymbol> T getSymbolLiteral(ResolvedExpression operands, Class<T> clazz) {
        return ((ValueLiteralExpression) operands).getValueAs(clazz).get();
    }

    private CallSyntaxUtils() {}
}
