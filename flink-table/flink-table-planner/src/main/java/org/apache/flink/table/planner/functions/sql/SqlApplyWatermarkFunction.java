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

package org.apache.flink.table.planner.functions.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * SQL function for APPLY_WATERMARK that allows flexible watermark assignment on any table
 * expression.
 *
 * <p>Syntax: {@code APPLY_WATERMARK(table_expr, DESCRIPTOR(time_column), watermark_expr)}
 *
 * <p>Example: {@code APPLY_WATERMARK(my_table, DESCRIPTOR(event_time), event_time - INTERVAL '5'
 * SECOND)}
 */
public class SqlApplyWatermarkFunction extends SqlFunction {

    public SqlApplyWatermarkFunction() {
        super(
                "APPLY_WATERMARK",
                SqlKind.OTHER_FUNCTION,
                ARG0_TABLE_RETURN_TYPE, // Return type same as input table
                null,
                new OperandMetadataImpl(),
                SqlFunctionCategory.SYSTEM);
    }

    /** Return type is the same as the input table operand. */
    private static final SqlReturnTypeInference ARG0_TABLE_RETURN_TYPE =
            opBinding -> opBinding.getOperandType(0);

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        // Return type is the same as the input table with the time column marked as ROWTIME
        return ARG0_TABLE_RETURN_TYPE.inferReturnType(opBinding);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.keyword(getName());
        final SqlWriter.Frame frame = writer.startList("(", ")");
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(1).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(2).unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
    }

    /** Operand type checker for APPLY_WATERMARK function. */
    private static class OperandMetadataImpl implements SqlOperandTypeChecker {

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            final SqlValidator validator = callBinding.getValidator();
            final SqlValidatorScope scope = callBinding.getScope();
            final List<SqlNode> operands = callBinding.operands();

            if (operands.size() != 3) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                            new org.apache.calcite.runtime.CalciteException(
                                    "Expected 3 arguments", null));
                }
                return false;
            }

            // Operand 0: TABLE expression
            RelDataType tableType = validator.getValidatedNodeType(operands.get(0));
            if (!tableType.isStruct()) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                            new org.apache.calcite.runtime.CalciteException(
                                    "First argument must be a TABLE", null));
                }
                return false;
            }

            // Operand 1: DESCRIPTOR(column_name)
            SqlNode descriptorArg = operands.get(1);
            if (!(descriptorArg instanceof SqlCall)) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                            new org.apache.calcite.runtime.CalciteException(
                                    "Second argument must be DESCRIPTOR(column_name)", null));
                }
                return false;
            }

            SqlCall descriptorCall = (SqlCall) descriptorArg;
            if (!descriptorCall.getOperator().getName().equalsIgnoreCase("DESCRIPTOR")) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                            new org.apache.calcite.runtime.CalciteException(
                                    "Second argument must be DESCRIPTOR(column_name)", null));
                }
                return false;
            }

            // Extract column name from DESCRIPTOR
            if (descriptorCall.getOperandList().isEmpty()) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                            new org.apache.calcite.runtime.CalciteException(
                                    "DESCRIPTOR must specify a column name", null));
                }
                return false;
            }

            SqlNode columnNode = descriptorCall.getOperandList().get(0);
            if (!(columnNode instanceof SqlIdentifier)) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                            new org.apache.calcite.runtime.CalciteException(
                                    "DESCRIPTOR argument must be a column identifier", null));
                }
                return false;
            }

            SqlIdentifier columnId = (SqlIdentifier) columnNode;
            String columnName = columnId.getSimple();

            // Validate that the column exists in the table and is of TIMESTAMP type
            boolean found = false;
            for (int i = 0; i < tableType.getFieldCount(); i++) {
                if (tableType.getFieldList().get(i).getName().equalsIgnoreCase(columnName)) {
                    RelDataType fieldType = tableType.getFieldList().get(i).getType();
                    SqlTypeName typeName = fieldType.getSqlTypeName();

                    // Check if it's a TIMESTAMP type
                    if (typeName != SqlTypeName.TIMESTAMP
                            && typeName != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                        if (throwOnFailure) {
                            throw callBinding.newError(
                                    new org.apache.calcite.runtime.CalciteException(
                                            String.format(
                                                    "Column '%s' must be of TIMESTAMP or TIMESTAMP_WITH_LOCAL_TIME_ZONE type, got %s",
                                                    columnName, typeName),
                                            null));
                        }
                        return false;
                    }
                    found = true;
                    break;
                }
            }

            if (!found) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                            new org.apache.calcite.runtime.CalciteException(
                                    String.format(
                                            "Column '%s' not found in table. Available columns: %s",
                                            columnName, tableType.getFieldNames()),
                                    null));
                }
                return false;
            }

            // Operand 2: Watermark expression
            // Must return a TIMESTAMP type
            RelDataType watermarkType = validator.getValidatedNodeType(operands.get(2));
            SqlTypeName watermarkTypeName = watermarkType.getSqlTypeName();
            if (watermarkTypeName != SqlTypeName.TIMESTAMP
                    && watermarkTypeName != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                            new org.apache.calcite.runtime.CalciteException(
                                    String.format(
                                            "Watermark expression must return TIMESTAMP type, got %s",
                                            watermarkTypeName),
                                    null));
                }
                return false;
            }

            return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(3);
        }

        @Override
        public String getAllowedSignatures(org.apache.calcite.sql.SqlOperator op, String opName) {
            return opName
                    + "(TABLE table_expr, DESCRIPTOR(time_column), watermark_expr)\n"
                    + "Example: APPLY_WATERMARK(my_table, DESCRIPTOR(event_time), event_time - INTERVAL '5' SECOND)";
        }

        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }

        @Override
        public boolean isOptional(int i) {
            return false;
        }
    }
}
