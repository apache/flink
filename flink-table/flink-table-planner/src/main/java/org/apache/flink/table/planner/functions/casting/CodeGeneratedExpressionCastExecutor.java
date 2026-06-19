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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.utils.CastExecutor;

import org.codehaus.janino.ExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;

/**
 * Cast executor which can be instantiated starting from an expression code.
 *
 * @param <IN> Input internal type
 * @param <OUT> Output internal type
 */
@Internal
class CodeGeneratedExpressionCastExecutor<IN, OUT> implements CastExecutor<IN, OUT> {

    private final ExpressionEvaluator expressionEvaluator;

    // To reuse for invocations
    private final Object[] inputArray;

    CodeGeneratedExpressionCastExecutor(ExpressionEvaluator expressionEvaluator) {
        this.expressionEvaluator = expressionEvaluator;
        inputArray = new Object[1];
    }

    @SuppressWarnings("unchecked")
    @Override
    public OUT cast(IN value) throws TableException {
        try {
            inputArray[0] = value;
            return (OUT) expressionEvaluator.evaluate(inputArray);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof TableException) {
                // Expected exception created by the rule, so no need to wrap it
                throw (TableException) e.getCause();
            }
            throw new TableException(
                    "Cannot execute the compiled expression for an unknown cause. " + e.getCause(),
                    e);
        }
    }
}
