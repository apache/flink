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

package org.apache.flink.table.planner.functions.casting.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.functions.casting.CastCodeBlock;
import org.apache.flink.table.planner.functions.casting.CastRulePredicate;
import org.apache.flink.table.planner.functions.casting.CodeGeneratorCastRule;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.primitiveTypeTermForType;

/**
 * Base class for cast rules supporting code generation. This class inherits from {@link
 * AbstractCodeGeneratorCastRule} and takes care of nullability checks.
 */
@Internal
public abstract class AbstractNullAwareCodeGeneratorCastRule<IN, OUT>
        extends AbstractCodeGeneratorCastRule<IN, OUT> {

    protected AbstractNullAwareCodeGeneratorCastRule(CastRulePredicate predicate) {
        super(predicate);
    }

    /**
     * This method doesn't need to take care of null checks handling of input values.
     * Implementations should write the cast result in the {@code returnVariable}.
     */
    protected abstract String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType);

    @Override
    public CastCodeBlock generateCodeBlock(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String inputIsNullTerm,
            LogicalType inputType,
            LogicalType targetType) {
        StringBuilder resultCode = new StringBuilder();

        // Result of a casting can be null only and only if the input is null
        boolean isResultNullable = inputType.isNullable();
        String nullTerm;
        if (isResultNullable) {
            nullTerm = context.declareVariable("boolean", "isNull");
            resultCode.append(nullTerm).append(" = ").append(inputIsNullTerm).append(";\n");
        } else {
            nullTerm = "false";
        }

        // Create the result value variable
        String returnTerm = context.declareVariable(primitiveTypeTermForType(targetType), "result");

        // Generate the code block
        String castCodeBlock =
                this.generateCodeBlockInternal(
                        context, inputTerm, returnTerm, inputType, targetType);

        if (isResultNullable) {
            resultCode
                    .append("if (!")
                    .append(nullTerm)
                    .append(") {\n  ")
                    .append(castCodeBlock)
                    .append("\n} else {\n")
                    .append(returnTerm)
                    .append(" = ")
                    .append(CodeGenUtils.primitiveDefaultValue(targetType))
                    .append(";\n}");
        } else {
            resultCode.append(castCodeBlock).append("\n");
        }

        return new CastCodeBlock(resultCode.toString(), returnTerm, nullTerm);
    }
}
