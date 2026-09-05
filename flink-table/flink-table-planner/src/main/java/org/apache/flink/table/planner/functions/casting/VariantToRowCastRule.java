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

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.runtime.functions.VariantCastUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.variant.Variant;

import java.util.List;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.binaryWriterWriteField;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.binaryWriterWriteNull;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * {@link LogicalTypeRoot#VARIANT} to {@link LogicalTypeRoot#ROW} and {@link
 * LogicalTypeRoot#STRUCTURED_TYPE} cast rule.
 *
 * <p>The variant must be an object, otherwise the cast fails. Fields match <b>by name</b> rather
 * than by position, because a JSON object is unordered. Name matching is case sensitive. A target
 * field absent from the object fails the cast. A field present but set to a VARIANT null maps to
 * SQL {@code NULL} when it is nullable and fails the cast when it is {@code NOT NULL}. A {@link
 * LogicalTypeRoot#VARIANT} target field is the exception: there the field cast is the identity, so
 * a VARIANT null is kept as a variant null rather than downgraded to SQL {@code NULL}. Object
 * fields the target does not name are dropped, so the row is a projection. A {@code STRUCTURED}
 * target shares the {@code RowData} representation and is served by the same rule.
 */
class VariantToRowCastRule extends AbstractVariantToConstructedCastRule<RowData> {

    static final VariantToRowCastRule INSTANCE = new VariantToRowCastRule();

    private VariantToRowCastRule() {
        super(CastRulePredicate.builder().predicate(VariantToRowCastRule::matches).build());
    }

    private static boolean matches(LogicalType input, LogicalType target) {
        if (!(input.is(LogicalTypeRoot.VARIANT)
                && (target.is(LogicalTypeRoot.ROW)
                        || target.is(LogicalTypeRoot.STRUCTURED_TYPE)))) {
            return false;
        }
        return LogicalTypeChecks.getFieldTypes(target).stream()
                .allMatch(fieldType -> CastRuleProvider.resolve(input, fieldType) != null);
    }

    /* Example generated code for ROW<`id` INT, `raw` VARIANT>. The typed `id` field runs the leaf
    cast; the `raw` field runs the identity cast and is written as-is, so a VARIANT null is kept
    rather than turned into SQL NULL:

    org.apache.flink.table.runtime.functions.VariantCastUtils.requireObject(
            variant$2, "ROW<`id` INT, `raw` VARIANT>");
    writer$4.reset();
    org.apache.flink.types.variant.Variant f0Value$5 = variant$2.getField("id");
    if (f0Value$5 == null) {
        throw new org.apache.flink.table.api.TableRuntimeException(
                "Cannot cast the VARIANT object to ROW<`id` INT, `raw` VARIANT> because the field "
                        + "'id' is not present in the VARIANT.");
    } else {
        if (f0Value$5.isNull()) {
            writer$4.setNullAt(0);
        } else {
            result$6 =
                    ((int) org.apache.flink.table.runtime.functions.VariantCastUtils.toIntegral(
                            f0Value$5, -2147483648L, 2147483647L, "INTEGER"));
            if (!isNull$7) {
                writer$4.writeInt(0, result$6);
            } else {
                writer$4.setNullAt(0);
            }
        }
    }
    org.apache.flink.types.variant.Variant f1Value$8 = variant$2.getField("raw");
    if (f1Value$8 == null) {
        throw new org.apache.flink.table.api.TableRuntimeException(
                "Cannot cast the VARIANT object to ROW<`id` INT, `raw` VARIANT> because the field "
                        + "'raw' is not present in the VARIANT.");
    } else {
        writer$4.writeVariant(1, f1Value$8);
    }
    writer$4.complete();
    result$3 = row$1.copy();

    A NOT NULL typed field throws instead of the setNullAt(i) shown above when its value is a
    VARIANT null.

    */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final List<String> fieldNames = LogicalTypeChecks.getFieldNames(targetLogicalType);
        final List<LogicalType> fieldTypes = LogicalTypeChecks.getFieldTypes(targetLogicalType);
        final CodeGeneratorContext codeGeneratorContext = context.getCodeGeneratorContext();

        final String rowTerm = newName(codeGeneratorContext, "row");
        final String writerTerm = newName(codeGeneratorContext, "writer");
        context.declareClassField(
                className(BinaryRowData.class),
                rowTerm,
                constructorCall(BinaryRowData.class, fieldTypes.size()));
        context.declareClassField(
                className(BinaryRowWriter.class),
                writerTerm,
                constructorCall(BinaryRowWriter.class, rowTerm));

        final CastRuleUtils.CodeWriter writer =
                new CastRuleUtils.CodeWriter()
                        .stmt(
                                staticCall(
                                        VariantCastUtils.class,
                                        "requireObject",
                                        inputTerm,
                                        strLiteral(targetLogicalType.asSummaryString())))
                        .stmt(methodCall(writerTerm, "reset"));

        for (int i = 0; i < fieldTypes.size(); i++) {
            final LogicalType fieldType = fieldTypes.get(i);
            final String fieldName = fieldNames.get(i);
            final String indexTerm = String.valueOf(i);
            final String fieldTerm = newName(codeGeneratorContext, "f" + indexTerm + "Value");

            // The field is guaranteed present and non-null here, since a missing field and a
            // VARIANT
            // null are handled below, so the inner cast is the plain VARIANT-to-field rule.
            final CastCodeBlock codeBlock =
                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                            context, fieldTerm, inputLogicalType, fieldType);
            final String writeField =
                    binaryWriterWriteField(
                            context, writerTerm, fieldType, indexTerm, codeBlock.getReturnTerm());
            final String writeNull = binaryWriterWriteNull(writerTerm, fieldType, indexTerm);

            // getField returns Java null for a field the object does not carry, which always fails
            // the cast.
            writer.declStmt(
                    Variant.class,
                    fieldTerm,
                    methodCall(inputTerm, "getField", strLiteral(fieldName)));

            if (fieldType.is(LogicalTypeRoot.VARIANT)) {
                // The field cast is the identity, so a present VARIANT null field is a valid
                // variant
                // null and is kept as-is rather than downgraded to SQL NULL, matching
                // ARRAY<VARIANT>
                // and the top-level VARIANT cast. An absent field still fails the cast.
                writer.ifStmt(
                        fieldTerm + " == null",
                        absentWriter ->
                                absentWriter.throwStmt(
                                        missingFieldError(fieldName, targetLogicalType)),
                        presentWriter -> presentWriter.append(codeBlock).stmt(writeField));
                continue;
            }

            // A field explicitly set to a VARIANT null returns a variant whose isNull() is true; it
            // maps to SQL NULL for a nullable field and fails the cast for a NOT NULL one.
            writer.ifStmt(
                    fieldTerm + " == null",
                    absentWriter ->
                            absentWriter.throwStmt(missingFieldError(fieldName, targetLogicalType)),
                    presentWriter ->
                            presentWriter.ifStmt(
                                    methodCall(fieldTerm, "isNull"),
                                    nullWriter -> {
                                        if (fieldType.isNullable()) {
                                            nullWriter.stmt(writeNull);
                                        } else {
                                            nullWriter.throwStmt(
                                                    nullFieldError(fieldName, targetLogicalType));
                                        }
                                    },
                                    valueWriter ->
                                            valueWriter
                                                    .append(codeBlock)
                                                    .ifStmt(
                                                            "!" + codeBlock.getIsNullTerm(),
                                                            thenWriter ->
                                                                    thenWriter.stmt(writeField),
                                                            elseWriter ->
                                                                    elseWriter.stmt(writeNull))));
        }

        writer.stmt(methodCall(writerTerm, "complete"))
                .assignStmt(returnVariable, methodCall(rowTerm, "copy"));
        return writer.toString();
    }

    private static String missingFieldError(String fieldName, LogicalType targetType) {
        final String message =
                String.format(
                        "Cannot cast the VARIANT object to %s because the field '%s' is not present in the VARIANT.",
                        targetType.asSummaryString(), fieldName);
        return constructorCall(TableRuntimeException.class, strLiteral(message));
    }

    private static String nullFieldError(String fieldName, LogicalType targetType) {
        final String message =
                String.format(
                        "Cannot cast the VARIANT object to %s because the field '%s' is a VARIANT null and the target does not accept NULL.",
                        targetType.asSummaryString(), fieldName);
        return constructorCall(TableRuntimeException.class, strLiteral(message));
    }
}
