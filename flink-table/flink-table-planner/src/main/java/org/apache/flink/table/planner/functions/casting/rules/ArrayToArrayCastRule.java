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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.planner.functions.casting.CastCodeBlock;
import org.apache.flink.table.planner.functions.casting.CastRulePredicate;
import org.apache.flink.table.planner.functions.casting.CastRuleProvider;
import org.apache.flink.table.planner.functions.casting.CodeGeneratorCastRule;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.rowFieldReadAccess;
import static org.apache.flink.table.planner.functions.casting.rules.CastRuleUtils.functionCall;

/** Array to array casting rule. */
@Internal
public class ArrayToArrayCastRule
        extends AbstractNullAwareCodeGeneratorCastRule<ArrayData, ArrayData> {

    public static final ArrayToArrayCastRule INSTANCE = new ArrayToArrayCastRule();

    private ArrayToArrayCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        input.is(LogicalTypeRoot.ARRAY)
                                                && target.is(LogicalTypeRoot.ARRAY)
                                                && isValidArrayCasting(
                                                        ((ArrayType) input).getElementType(),
                                                        ((ArrayType) target).getElementType()))
                        .build());
    }

    private static boolean isValidArrayCasting(
            LogicalType innerInputType, LogicalType innerTargetType) {
        return CastRuleProvider.resolve(innerInputType, innerTargetType) != null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        LogicalType innerInputType = ((ArrayType) inputLogicalType).getElementType();
        LogicalType innerTargetType = ((ArrayType) targetLogicalType).getElementType();

        String innerTargetTypeTerm = arrayElementType(innerTargetType);
        String arraySize = inputTerm + ".size()";
        String objArrayTerm = newName("objArray");

        StringBuilder result = new StringBuilder();
        result.append(
                innerTargetTypeTerm
                        + "[] "
                        + objArrayTerm
                        + " = new "
                        + innerTargetTypeTerm
                        + "["
                        + arraySize
                        + "];\n");

        result.append("for (int i = 0; i < " + arraySize + "; i++) {\n");
        CastCodeBlock codeBlock =
                CastRuleProvider.generateCodeBlock(
                        context,
                        rowFieldReadAccess("i", inputTerm, innerInputType),
                        functionCall(inputTerm + ".isNullAt", "i"),
                        innerInputType.copy(false), // Null check is done at the array access level
                        innerTargetType);

        String innerElementCode =
                codeBlock.getCode()
                        + "\n"
                        + objArrayTerm
                        + "[i] = "
                        + codeBlock.getReturnTerm()
                        + ";\n";

        // Add null check if inner type is nullable
        if (innerInputType.isNullable()) {
            result.append("if (" + inputTerm + ".isNullAt(i)) {\n")
                    .append(objArrayTerm + "[i] = null;\n")
                    .append("} else {\n")
                    .append(innerElementCode)
                    .append("}\n");
        } else {
            result.append(innerElementCode);
        }

        result.append("}\n");

        result.append(
                returnVariable
                        + " = new "
                        + className(GenericArrayData.class)
                        + "("
                        + objArrayTerm
                        + ");\n");

        return result.toString();
    }

    private static String arrayElementType(LogicalType t) {
        if (t.isNullable()) {
            return "Object";
        }
        switch (t.getTypeRoot()) {
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "byte";
            case SMALLINT:
                return "short";
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return "int";
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return "long";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DISTINCT_TYPE:
                return arrayElementType(((DistinctType) t).getSourceType());
        }
        return "Object";
    }
}
