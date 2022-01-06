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

import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.isPrimitiveNullable;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.primitiveDefaultValue;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.primitiveTypeTermForType;

/**
 * Base class for cast rules supporting code generation. This class inherits from {@link
 * AbstractCodeGeneratorCastRule} and takes care of nullability checks.
 */
abstract class AbstractNullAwareCodeGeneratorCastRule<IN, OUT>
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
        final CastRuleUtils.CodeWriter writer = new CastRuleUtils.CodeWriter();

        final boolean isResultNullable = inputType.isNullable() || isPrimitiveNullable(targetType);
        String nullTerm;
        if (isResultNullable) {
            nullTerm = context.declareVariable("boolean", "isNull");
            writer.assignStmt(nullTerm, inputIsNullTerm);
        } else {
            nullTerm = "false";
        }

        // Create the result value variable
        final String returnTerm =
                context.declareVariable(primitiveTypeTermForType(targetType), "result");

        // Generate the code block
        final String castCodeBlock =
                this.generateCodeBlockInternal(
                        context, inputTerm, returnTerm, inputType, targetType);

        if (isResultNullable) {
            writer.ifStmt(
                    "!" + nullTerm,
                    thenWriter -> {
                        thenWriter.appendBlock(castCodeBlock);

                        // If the result type is not primitive,
                        // then perform another null check
                        if (isPrimitiveNullable(targetType)) {
                            thenWriter.assignStmt(nullTerm, returnTerm + " == null");
                        }
                    },
                    elseWriter ->
                            elseWriter.assignStmt(returnTerm, primitiveDefaultValue(targetType)));
        } else {
            writer.appendBlock(castCodeBlock);
        }

        return CastCodeBlock.withCode(writer.toString(), returnTerm, nullTerm);
    }
}
