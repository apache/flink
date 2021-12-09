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

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.types.logical.VarCharType.STRING_TYPE;

/**
 * Any source type to {@link LogicalTypeFamily#BINARY_STRING} cast rule.
 *
 * <p>This rule is used for casting from any of the {@link LogicalTypeFamily#PREDEFINED} types to
 * {@link LogicalTypeRoot#CHAR} or {@link LogicalTypeRoot#VARCHAR}. It calls the underlying concrete
 * matching rule, i.e.: {@link NumericToStringCastRule} to do the actual conversion and then
 * performs any necessary trimming or padding so that the length of the result string value matches
 * the one specified by the length of the target {@link LogicalTypeRoot#CHAR} or {@link
 * LogicalTypeRoot#VARCHAR} type.
 */
class CharVarCharTrimPadCastRule
        extends AbstractNullAwareCodeGeneratorCastRule<Object, StringData> {

    static final CharVarCharTrimPadCastRule INSTANCE = new CharVarCharTrimPadCastRule();

    private CharVarCharTrimPadCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (inputType, targetType) ->
                                        targetType.is(LogicalTypeFamily.CHARACTER_STRING)
                                                && !targetType.equals(STRING_TYPE))
                        .build());
    }

    /* Example generated code for STRING() -> CHAR(6) cast

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        if (_myInput.numChars() > 6) {
            result$1 = _myInput.substring(0, 6);
        } else {
            if (_myInput.numChars() < 6) {
                int padLength$1;
                padLength$1 = 6 - _myInput.numChars();
                org.apache.flink.table.data.binary.BinaryStringData padString$2;
                padString$2 = org.apache.flink.table.data.binary.BinaryStringData.blankString(padLength$1);
                result$1 = org.apache.flink.table.data.binary.BinaryStringDataUtil.concat(_myInput, padString$2);
            } else {
                result$1 = _myInput;
            }
        }
        isNull$0 = result$1 == null;
    } else {
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
    }

    */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final int length = LogicalTypeChecks.getLength(targetLogicalType);
        CastRule<?, ?> castRule =
                CastRuleProvider.resolve(inputLogicalType, VarCharType.STRING_TYPE);

        // Only used for non-Constructed types - for constructed type and RAW, the trimming/padding
        // is applied on each individual rule, i.e.: ArrayToStringCastRule, RawToStringCastRule
        if (castRule instanceof ExpressionCodeGeneratorCastRule) {
            @SuppressWarnings("rawtypes")
            final String stringExpr =
                    ((ExpressionCodeGeneratorCastRule) castRule)
                            .generateExpression(
                                    context, inputTerm, inputLogicalType, targetLogicalType);

            final CastRuleUtils.CodeWriter writer = new CastRuleUtils.CodeWriter();
            if (context.legacyBehaviour()
                    || !(couldTrim(length) || couldPad(targetLogicalType, length))) {
                return writer.assignStmt(returnVariable, stringExpr).toString();
            }
            return writer.ifStmt(
                            methodCall(stringExpr, "numChars") + " > " + length,
                            thenWriter ->
                                    thenWriter.assignStmt(
                                            returnVariable,
                                            methodCall(stringExpr, "substring", 0, length)),
                            elseWriter -> {
                                if (couldPad(targetLogicalType, length)) {
                                    final String padLength = newName("padLength");
                                    final String padString = newName("padString");
                                    elseWriter.ifStmt(
                                            methodCall(stringExpr, "numChars") + " < " + length,
                                            thenInnerWriter ->
                                                    thenInnerWriter
                                                            .declStmt(int.class, padLength)
                                                            .assignStmt(
                                                                    padLength,
                                                                    length
                                                                            + " - "
                                                                            + methodCall(
                                                                                    stringExpr,
                                                                                    "numChars"))
                                                            .declStmt(
                                                                    BinaryStringData.class,
                                                                    padString)
                                                            .assignStmt(
                                                                    padString,
                                                                    staticCall(
                                                                            BinaryStringData.class,
                                                                            "blankString",
                                                                            padLength))
                                                            .assignStmt(
                                                                    returnVariable,
                                                                    staticCall(
                                                                            BinaryStringDataUtil
                                                                                    .class,
                                                                            "concat",
                                                                            stringExpr,
                                                                            padString)),
                                            elseInnerWriter ->
                                                    elseInnerWriter.assignStmt(
                                                            returnVariable, stringExpr));
                                } else {
                                    elseWriter.assignStmt(returnVariable, stringExpr);
                                }
                            })
                    .toString();
        } else {
            throw new IllegalStateException("This is a bug. Please file an issue.");
        }
    }

    // ---------------
    // Shared methods
    // ---------------

    static String stringExceedsLength(String strTerm, int targetLength) {
        return methodCall(strTerm, "length") + " > " + targetLength;
    }

    static String stringShouldPad(String strTerm, int targetLength) {
        return methodCall(strTerm, "length") + " < " + targetLength;
    }

    static boolean couldTrim(int targetLength) {
        return targetLength < VarCharType.MAX_LENGTH;
    }

    static boolean couldPad(LogicalType targetType, int targetLength) {
        return targetType.is(LogicalTypeRoot.CHAR) && targetLength < VarCharType.MAX_LENGTH;
    }

    static CastRuleUtils.CodeWriter padAndTrimStringIfNeeded(
            CastRuleUtils.CodeWriter writer,
            LogicalType targetType,
            boolean legacyBehaviour,
            int length,
            String resultStringTerm,
            String builderTerm) {
        writer.declStmt(String.class, resultStringTerm)
                .assignStmt(resultStringTerm, methodCall(builderTerm, "toString"));

        // Trim and Pad if needed
        if (!legacyBehaviour && (couldTrim(length) || couldPad(targetType, length))) {
            writer.ifStmt(
                    stringExceedsLength(builderTerm, length),
                    thenWriter ->
                            thenWriter.assignStmt(
                                    resultStringTerm,
                                    methodCall(
                                            builderTerm,
                                            "substring",
                                            0,
                                            staticCall(
                                                    Math.class,
                                                    "min",
                                                    methodCall(builderTerm, "length"),
                                                    length))),
                    elseWriter ->
                            padStringIfNeeded(
                                    elseWriter,
                                    targetType,
                                    legacyBehaviour,
                                    length,
                                    resultStringTerm));
        }
        return writer;
    }

    static void padStringIfNeeded(
            CastRuleUtils.CodeWriter writer,
            LogicalType targetType,
            boolean legacyBehaviour,
            int length,
            String returnTerm) {

        // Pad if needed
        if (!legacyBehaviour && couldPad(targetType, length)) {
            final String padLength = newName("padLength");
            final String sbPadding = newName("sbPadding");
            writer.ifStmt(
                    stringShouldPad(returnTerm, length),
                    thenWriter ->
                            thenWriter
                                    .declStmt(int.class, padLength)
                                    .assignStmt(
                                            padLength,
                                            length + " - " + methodCall(returnTerm, "length"))
                                    .declStmt(StringBuilder.class, sbPadding)
                                    .assignStmt(sbPadding, constructorCall(StringBuilder.class))
                                    .forStmt(
                                            padLength,
                                            (idx, loopWriter) ->
                                                    loopWriter.stmt(
                                                            methodCall(
                                                                    sbPadding, "append", "\" \"")))
                                    .assignStmt(
                                            returnTerm,
                                            returnTerm
                                                    + " + "
                                                    + methodCall(sbPadding, "toString")));
        }
    }
}
