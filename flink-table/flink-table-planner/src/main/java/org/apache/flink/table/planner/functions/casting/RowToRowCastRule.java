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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.rowFieldReadAccess;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.binaryWriterWriteField;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.binaryWriterWriteNull;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/**
 * {@link LogicalTypeRoot#ROW} and {@link LogicalTypeRoot#STRUCTURED_TYPE} to {@link
 * LogicalTypeRoot#ROW} and {@link LogicalTypeRoot#STRUCTURED_TYPE} cast rule.
 */
class RowToRowCastRule extends AbstractNullAwareCodeGeneratorCastRule<RowData, RowData> {

    static final RowToRowCastRule INSTANCE = new RowToRowCastRule();

    private RowToRowCastRule() {
        super(CastRulePredicate.builder().predicate(RowToRowCastRule::matches).build());
    }

    private static boolean matches(LogicalType input, LogicalType target) {
        if (!((input.is(LogicalTypeRoot.ROW) || input.is(LogicalTypeRoot.STRUCTURED_TYPE))
                && (target.is(LogicalTypeRoot.ROW)
                        || target.is(LogicalTypeRoot.STRUCTURED_TYPE)))) {
            return false;
        }

        final List<LogicalType> inputFields = LogicalTypeChecks.getFieldTypes(input);
        final List<LogicalType> targetFields = LogicalTypeChecks.getFieldTypes(target);

        if (inputFields.size() < targetFields.size()) {
            return false;
        }

        return IntStream.range(0, targetFields.size())
                .allMatch(i -> CastRuleProvider.exists(inputFields.get(i), targetFields.get(i)));
    }

    /* Example generated code for ROW<`f0` BIGINT, `f1` BIGINT, `f2` STRING, `f3` ARRAY<STRING>>:

    isNull$29 = false;
    if (!isNull$29) {
        writer$32.reset();
        boolean f0IsNull$34 = row$26.isNullAt(0);
        if (!f0IsNull$34) {
            int f0Value$33 = row$26.getInt(0);
            result$35 = ((long) (f0Value$33));
            if (!false) {
                writer$32.writeLong(0, result$35);
            } else {
                writer$32.setNullAt(0);
            }
        } else {
            writer$32.setNullAt(0);
        }
        boolean f1IsNull$37 = row$26.isNullAt(1);
        if (!f1IsNull$37) {
            int f1Value$36 = row$26.getInt(1);
            result$38 = ((long) (f1Value$36));
            if (!false) {
                writer$32.writeLong(1, result$38);
            } else {
                writer$32.setNullAt(1);
            }
        } else {
            writer$32.setNullAt(1);
        }
        boolean f2IsNull$40 = row$26.isNullAt(2);
        if (!f2IsNull$40) {
            int f2Value$39 = row$26.getInt(2);
            isNull$41 = f2IsNull$40;
            if (!isNull$41) {
                result$42 =
                        org.apache.flink.table.data.binary.BinaryStringData.fromString(
                                org.apache.flink.table.utils.DateTimeUtils
                                        .formatTimestampMillis(f2Value$39, 0));
                isNull$41 = result$42 == null;
            } else {
                result$42 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            }
            if (!isNull$41) {
                writer$32.writeString(2, result$42);
            } else {
                writer$32.setNullAt(2);
            }
        } else {
            writer$32.setNullAt(2);
        }
        boolean f3IsNull$44 = row$26.isNullAt(3);
        if (!f3IsNull$44) {
            org.apache.flink.table.data.ArrayData f3Value$43 = row$26.getArray(3);
            isNull$45 = f3IsNull$44;
            if (!isNull$45) {
                Object[] objArray$47 = new Object[f3Value$43.size()];
                for (int i$48 = 0; i$48 < f3Value$43.size(); i$48++) {
                    if (!f3Value$43.isNullAt(i$48)) {
                        objArray$47[i$48] =
                                ((org.apache.flink.table.data.binary.BinaryStringData)
                                        f3Value$43.getString(i$48));
                    }
                }
                result$46 = new org.apache.flink.table.data.GenericArrayData(objArray$47);
                isNull$45 = result$46 == null;
            } else {
                result$46 = null;
            }
            if (!isNull$45) {
                writer$32.writeArray(3, result$46, typeSerializer$49);
            } else {
                writer$32.setNullAt(3);
            }
        } else {
            writer$32.setNullAt(3);
        }
        writer$32.complete();
        result$30 = row$31;
        isNull$29 = result$30 == null;
    } else {
        result$30 = null;
    }

     */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final List<LogicalType> inputFields = LogicalTypeChecks.getFieldTypes(inputLogicalType);
        final List<LogicalType> targetFields = LogicalTypeChecks.getFieldTypes(targetLogicalType);

        // Declare the row and row data
        final String rowTerm = newName("row");
        final String writerTerm = newName("writer");
        context.declareClassField(
                className(BinaryRowData.class),
                rowTerm,
                constructorCall(BinaryRowData.class, inputFields.size()));
        context.declareClassField(
                className(BinaryRowWriter.class),
                writerTerm,
                constructorCall(BinaryRowWriter.class, rowTerm));

        final CastRuleUtils.CodeWriter writer =
                new CastRuleUtils.CodeWriter().stmt(methodCall(writerTerm, "reset"));

        for (int i = 0; i < targetFields.size(); i++) {
            final LogicalType inputFieldType = inputFields.get(i);
            final LogicalType targetFieldType = targetFields.get(i);
            final String indexTerm = String.valueOf(i);

            final String fieldTerm = newName("f" + indexTerm + "Value");
            final String fieldIsNullTerm = newName("f" + indexTerm + "IsNull");

            final CastCodeBlock codeBlock =
                    // Null check is done at the row access level
                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                            context, fieldTerm, inputFieldType, targetFieldType);

            final String readField = rowFieldReadAccess(indexTerm, inputTerm, inputFieldType);
            final String writeField =
                    binaryWriterWriteField(
                            context,
                            writerTerm,
                            targetFieldType,
                            indexTerm,
                            codeBlock.getReturnTerm());
            final String writeNull = binaryWriterWriteNull(writerTerm, targetFieldType, indexTerm);

            writer.declStmt(
                            boolean.class,
                            fieldIsNullTerm,
                            methodCall(inputTerm, "isNullAt", indexTerm))
                    .ifStmt(
                            "!" + fieldIsNullTerm,
                            thenBodyWriter ->
                                    thenBodyWriter
                                            // If element not null, extract it and
                                            // execute the cast and then write it with the writer
                                            .declPrimitiveStmt(inputFieldType, fieldTerm, readField)
                                            .append(codeBlock)
                                            .ifStmt(
                                                    "!" + codeBlock.getIsNullTerm(),
                                                    thenCastResultWriter ->
                                                            thenCastResultWriter.stmt(writeField),
                                                    elseCastResultWriter ->
                                                            elseCastResultWriter.stmt(writeNull)),
                            elseBodyWriter ->
                                    // If element is null, just write NULL
                                    elseBodyWriter.stmt(writeNull));
        }

        writer.stmt(methodCall(writerTerm, "complete"))
                .assignStmt(returnVariable, methodCall(rowTerm, "copy"));
        return writer.toString();
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        final List<LogicalType> inputFields = LogicalTypeChecks.getFieldTypes(inputLogicalType);
        final List<LogicalType> targetFields = LogicalTypeChecks.getFieldTypes(targetLogicalType);

        return IntStream.range(0, Math.min(inputFields.size(), targetFields.size()))
                .anyMatch(i -> CastRuleProvider.canFail(inputFields.get(i), targetFields.get(i)));
    }
}
