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
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Cast rule that is able to generate a single expression containing all the casting logic.
 *
 * @param <IN> Input internal type
 * @param <OUT> Output internal type
 */
@Internal
public interface ExpressionCodeGeneratorCastRule<IN, OUT> extends CodeGeneratorCastRule<IN, OUT> {

    /**
     * Generate a Java expression performing the casting. This expression can be wrapped in another
     * expression, or assigned to a variable or returned from a function.
     *
     * <p>NOTE: the {@code inputTerm} is always either a primitive or a non-null object, while the
     * expression result is either a primitive or a nullable object.
     */
    String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType);
}
