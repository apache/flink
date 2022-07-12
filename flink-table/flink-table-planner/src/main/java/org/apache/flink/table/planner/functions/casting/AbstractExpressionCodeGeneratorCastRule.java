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

import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.util.Collections;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.boxedTypeTermForType;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.box;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.unbox;

/**
 * Base class for cast rules that supports code generation, requiring only an expression to perform
 * the cast. If the casting logic requires to generate several statements, look at {@link
 * AbstractNullAwareCodeGeneratorCastRule}.
 *
 * <p>NOTE: the {@code inputTerm} is always either a primitive or a non-null object.
 */
abstract class AbstractExpressionCodeGeneratorCastRule<IN, OUT>
        extends AbstractNullAwareCodeGeneratorCastRule<IN, OUT>
        implements ExpressionCodeGeneratorCastRule<IN, OUT> {

    protected AbstractExpressionCodeGeneratorCastRule(CastRulePredicate predicate) {
        super(predicate);
    }

    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return returnVariable
                + " = "
                + generateExpression(context, inputTerm, inputLogicalType, targetLogicalType)
                + ";\n";
    }

    @Override
    public CastExecutor<IN, OUT> create(
            CastRule.Context context, LogicalType inputLogicalType, LogicalType targetLogicalType) {
        if (this.canFail(inputLogicalType, targetLogicalType)) {
            // We can't use the ExpressionEvaluator because we need proper wrapping of the eventual
            // exception
            return super.create(context, inputLogicalType, targetLogicalType);
        }

        final String inputArgumentName = "inputValue";

        final String expression =
                // We need to wrap the expression in a null check
                CastRuleUtils.ternaryOperator(
                        inputArgumentName + " == null",
                        "null",
                        // Values are always boxed when passed to ExpressionEvaluator and no auto
                        // boxing/unboxing is provided, so we need to take care of it manually
                        box(
                                generateExpression(
                                        createCodeGeneratorCastRuleContext(context),
                                        unbox(
                                                // We need the casting because the rules uses the
                                                // concrete classes (e.g. StringData and
                                                // BinaryStringData)
                                                cast(
                                                        boxedTypeTermForType(inputLogicalType),
                                                        inputArgumentName),
                                                inputLogicalType),
                                        inputLogicalType,
                                        targetLogicalType),
                                targetLogicalType));

        return new CodeGeneratedExpressionCastExecutor<>(
                CompileUtils.compileExpression(
                        expression,
                        Collections.singletonList(inputArgumentName),
                        Collections.singletonList(
                                LogicalTypeUtils.toInternalConversionClass(inputLogicalType)),
                        LogicalTypeUtils.toInternalConversionClass(targetLogicalType)));
    }

    private static CodeGeneratorCastRule.Context createCodeGeneratorCastRuleContext(
            CastRule.Context ctx) {
        return new CodeGeneratorCastRule.Context() {
            @Override
            public boolean isPrinting() {
                return ctx.isPrinting();
            }

            @Override
            public boolean legacyBehaviour() {
                return ctx.legacyBehaviour();
            }

            @Override
            public String getSessionTimeZoneTerm() {
                return "java.util.TimeZone.getTimeZone(\"" + ctx.getSessionZoneId().getId() + "\")";
            }

            @Override
            public String declareVariable(String type, String variablePrefix) {
                throw new UnsupportedOperationException(
                        "No variable can be declared when using AbstractExpressionCodeGeneratorCastRule. You should use AbstractCodeGeneratorCastRule instead.");
            }

            @Override
            public String declareTypeSerializer(LogicalType type) {
                throw new UnsupportedOperationException(
                        "No type serializer can be declared when using AbstractExpressionCodeGeneratorCastRule. You should use AbstractCodeGeneratorCastRule instead.");
            }

            @Override
            public String declareClassField(String type, String field, String initialization) {
                throw new UnsupportedOperationException(
                        "No class field can be declared when using AbstractExpressionCodeGeneratorCastRule. You should use AbstractCodeGeneratorCastRule instead.");
            }
        };
    }
}
