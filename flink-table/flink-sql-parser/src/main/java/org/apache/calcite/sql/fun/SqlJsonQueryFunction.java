/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The <code>JSON_QUERY</code> function.
 *
 * <p>This class was copied over from Calcite to support RETURNING clause in JSON_QUERY
 * (CALCITE-6365). When upgrading to Calcite 1.38.0 version, please remove the entire class.
 */
public class SqlJsonQueryFunction extends SqlFunction {

    public SqlJsonQueryFunction() {
        super(
                "JSON_QUERY",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.cascade(
                        opBinding ->
                                explicitTypeSpec(opBinding)
                                        .orElseGet(
                                                () ->
                                                        ReturnTypes.VARCHAR_2000.inferReturnType(
                                                                opBinding)),
                        SqlTypeTransforms.FORCE_NULLABLE),
                null,
                OperandTypes.family(
                        Arrays.asList(
                                SqlTypeFamily.ANY,
                                SqlTypeFamily.CHARACTER,
                                SqlTypeFamily.ANY,
                                SqlTypeFamily.ANY,
                                SqlTypeFamily.ANY,
                                SqlTypeFamily.ANY),
                        i -> i >= 5),
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public @Nullable String getSignatureTemplate(int operandsCount) {
        if (operandsCount == 6) {
            return "{0}({1} {2} RETURNING {6} {3} WRAPPER {4} ON EMPTY {5} ON ERROR)";
        } else {
            return "{0}({1} {2} {3} WRAPPER {4} ON EMPTY {5} ON ERROR)";
        }
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        call.operand(0).unparse(writer, 0, 0);
        writer.sep(",", true);
        call.operand(1).unparse(writer, 0, 0);

        if (call.operandCount() == 6) {
            writer.keyword("RETURNING");
            call.operand(5).unparse(writer, 0, 0);
        }
        final SqlJsonQueryWrapperBehavior wrapperBehavior = getEnumValue(call.operand(2));
        switch (wrapperBehavior) {
            case WITHOUT_ARRAY:
                writer.keyword("WITHOUT ARRAY");
                break;
            case WITH_CONDITIONAL_ARRAY:
                writer.keyword("WITH CONDITIONAL ARRAY");
                break;
            case WITH_UNCONDITIONAL_ARRAY:
                writer.keyword("WITH UNCONDITIONAL ARRAY");
                break;
            default:
                throw new IllegalStateException("unreachable code");
        }
        writer.keyword("WRAPPER");
        unparseEmptyOrErrorBehavior(writer, getEnumValue(call.operand(3)));
        writer.keyword("ON EMPTY");
        unparseEmptyOrErrorBehavior(writer, getEnumValue(call.operand(4)));
        writer.keyword("ON ERROR");
        writer.endFunCall(frame);
    }

    public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
        final List<SqlNode> args = new ArrayList<>();
        args.add(operands[0]);
        args.add(operands[1]);

        if (operands[2] == null) {
            args.add(SqlLiteral.createSymbol(SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY, pos));
        } else {
            args.add(operands[2]);
        }
        if (operands[3] == null) {
            args.add(SqlLiteral.createSymbol(SqlJsonQueryEmptyOrErrorBehavior.NULL, pos));
        } else {
            args.add(operands[3]);
        }
        if (operands[4] == null) {
            args.add(SqlLiteral.createSymbol(SqlJsonQueryEmptyOrErrorBehavior.NULL, pos));
        } else {
            args.add(operands[4]);
        }

        if (operands.length >= 6 && operands[5] != null) {
            args.add(operands[5]);
        }

        pos = pos.plusAll(operands);
        return new SqlBasicCall(this, args, pos, functionQualifier);
    }

    private static void unparseEmptyOrErrorBehavior(
            SqlWriter writer, SqlJsonQueryEmptyOrErrorBehavior emptyBehavior) {
        switch (emptyBehavior) {
            case NULL:
                writer.keyword("NULL");
                break;
            case ERROR:
                writer.keyword("ERROR");
                break;
            case EMPTY_ARRAY:
                writer.keyword("EMPTY ARRAY");
                break;
            case EMPTY_OBJECT:
                writer.keyword("EMPTY OBJECT");
                break;
            default:
                throw new IllegalStateException("unreachable code");
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Enum<E>> E getEnumValue(SqlNode operand) {
        return (E) requireNonNull(((SqlLiteral) operand).getValue(), "operand.value");
    }

    public static boolean hasExplicitTypeSpec(@Nullable SqlNode[] operands) {
        return operands.length >= 6;
    }

    public static List<SqlNode> removeTypeSpecOperands(SqlCall call) {
        return call.getOperandList().subList(0, 5);
    }

    /** Returns the optional explicit returning type specification. * */
    private static Optional<RelDataType> explicitTypeSpec(SqlOperatorBinding opBinding) {
        if (opBinding.getOperandCount() >= 6) {
            return Optional.of(opBinding.getOperandType(5));
        }
        return Optional.empty();
    }
}
