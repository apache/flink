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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * SQL function {@code APPLY_WATERMARK} that assigns or overrides the watermark strategy on a
 * relational input (table, view, or subquery) at query time.
 *
 * <p>Syntax:
 *
 * <pre>{@code
 * APPLY_WATERMARK(table_expr, DESCRIPTOR(rowtime_column), watermark_expr)
 * }</pre>
 *
 * <p>Examples:
 *
 * <pre>{@code
 * -- Assign watermark using the rowtime column with a 5-second bound
 * SELECT * FROM APPLY_WATERMARK(
 *   TABLE orders,
 *   DESCRIPTOR(order_time),
 *   order_time - INTERVAL '5' SECOND);
 *
 * -- Apply watermark on a non-materialized view
 * SELECT * FROM APPLY_WATERMARK(
 *   TABLE silver_events,
 *   DESCRIPTOR(event_time),
 *   event_time - INTERVAL '30' SECOND);
 * }</pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>The function returns a relation with the same schema as the input.
 *   <li>The named column becomes the rowtime attribute and the supplied expression defines the
 *       watermark.
 *   <li>If the input already carries a watermark (e.g. defined in {@code CREATE TABLE}), the
 *       APPLY_WATERMARK assignment overrides it for the scope of the surrounding query only. No
 *       catalog metadata is mutated.
 * </ul>
 *
 * <p>Implementation note: this function is registered as a {@link SqlTableFunction} so the
 * validator treats the first operand as a table expression. The watermark expression in the third
 * operand is currently scoped at the call site (outer scope). Resolving column references against
 * the table operand's row type requires a follow-up that introduces a dedicated {@link
 * org.apache.calcite.sql.validate.SqlValidatorScope} for the function call (tracked as a follow-up
 * of FLINK-39062).
 */
public class SqlApplyWatermarkFunction extends SqlFunction implements SqlTableFunction {

    private static final String PARAM_INPUT = "INPUT";
    private static final String PARAM_ROWTIME = "ROWTIME";
    private static final String PARAM_WATERMARK_EXPR = "WATERMARK_EXPR";

    public SqlApplyWatermarkFunction() {
        super(
                "APPLY_WATERMARK",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                null,
                new OperandMetadataImpl(),
                SqlFunctionCategory.SYSTEM);
    }

    /**
     * The output schema is identical to the input table operand, except the column referenced by
     * the {@code DESCRIPTOR} operand is promoted to a rowtime time-indicator type. This keeps the
     * row type produced by the {@link org.apache.calcite.rel.logical.LogicalTableFunctionScan} in
     * sync with the row type produced by {@link
     * org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner}, so the rule that
     * rewrites the former into the latter does not run into a Calcite type-equivalence assertion.
     */
    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return opBinding -> {
            final RelDataType inputType;
            String rowtimeColumn = null;
            if (opBinding instanceof SqlCallBinding) {
                final SqlCallBinding callBinding = (SqlCallBinding) opBinding;
                inputType = callBinding.getValidator().getValidatedNodeType(callBinding.operand(0));
                final SqlNode descriptor = callBinding.operand(1);
                if (descriptor instanceof SqlCall && descriptor.getKind() == SqlKind.DESCRIPTOR) {
                    final List<SqlNode> cols = ((SqlCall) descriptor).getOperandList();
                    if (!cols.isEmpty() && cols.get(0) instanceof SqlIdentifier) {
                        final SqlIdentifier id = (SqlIdentifier) cols.get(0);
                        rowtimeColumn = id.isSimple() ? id.getSimple() : Util.last(id.names);
                    }
                }
            } else {
                inputType = opBinding.getOperandType(0);
            }

            if (rowtimeColumn == null) {
                return inputType;
            }

            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            if (!(typeFactory instanceof FlinkTypeFactory)) {
                return inputType;
            }
            final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
            // Prefer the validator's name matcher (case-insensitive in Flink's default
            // configuration) for consistency with OperandMetadataImpl.checkOperandTypes.
            // The matcher is only available during SQL validation; when the row type is
            // re-inferred later (e.g. from a RexCallBinding), fall back to a case-insensitive
            // linear search so that the rowtime column is still promoted to a time-indicator
            // type and the row type stays aligned with LogicalWatermarkAssigner.
            final int rowtimeIdx;
            if (opBinding instanceof SqlCallBinding) {
                final SqlNameMatcher matcher =
                        ((SqlCallBinding) opBinding)
                                .getValidator()
                                .getCatalogReader()
                                .nameMatcher();
                rowtimeIdx = matcher.indexOf(inputType.getFieldNames(), rowtimeColumn);
            } else {
                rowtimeIdx = indexOfIgnoreCase(inputType.getFieldNames(), rowtimeColumn);
            }
            if (rowtimeIdx < 0) {
                return inputType;
            }

            final List<RelDataTypeField> newFields = new ArrayList<>(inputType.getFieldCount());
            for (RelDataTypeField field : inputType.getFieldList()) {
                if (field.getIndex() == rowtimeIdx) {
                    final boolean isLtz =
                            field.getType().getSqlTypeName()
                                    == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
                    final RelDataType rowtimeIndicator =
                            flinkTypeFactory.createRowtimeIndicatorType(
                                    field.getType().isNullable(), isLtz);
                    newFields.add(
                            new RelDataTypeFieldImpl(
                                    field.getName(), field.getIndex(), rowtimeIndicator));
                } else {
                    newFields.add(field);
                }
            }
            final RelDataTypeFactory.Builder builder = typeFactory.builder();
            builder.addAll(newFields);
            return builder.build();
        };
    }

    /** Only the first operand is a table; the rest are scalar expressions. */
    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }

    /**
     * Case-insensitive lookup used as a fallback for row-type inference when no validator name
     * matcher is available (e.g. when the row type is re-inferred from a {@code RexCallBinding}).
     */
    private static int indexOfIgnoreCase(List<String> fieldNames, String name) {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldNames.get(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Custom validation that mirrors the behaviour of {@code SqlMLTableFunction}: skip the
     * DESCRIPTOR operand (its identifiers are validated against the input table inside {@link
     * OperandMetadataImpl}, not against the outer query scope) and validate every other operand
     * against the outer scope.
     *
     * <p>Note: the watermark expression (operand 2) is currently validated in the outer scope. To
     * support column references against the input table (e.g. {@code event_time - INTERVAL '5'
     * SECOND}), a dedicated {@link SqlValidatorScope} that exposes the input row type must be
     * introduced. This is tracked as a follow-up of FLINK-39062.
     */
    @Override
    public void validateCall(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlValidatorScope operandScope) {
        assert call.getOperator() == this;
        for (SqlNode operand : call.getOperandList()) {
            if (operand.getKind() == SqlKind.DESCRIPTOR) {
                continue;
            }
            operand.validate(validator, scope);
        }
    }

    /** Operand metadata performs structural and type validation for APPLY_WATERMARK. */
    private static class OperandMetadataImpl implements SqlOperandMetadata {

        private static final List<String> PARAMETERS =
                Collections.unmodifiableList(
                        Arrays.asList(PARAM_INPUT, PARAM_ROWTIME, PARAM_WATERMARK_EXPR));

        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
            return Collections.nCopies(
                    PARAMETERS.size(), typeFactory.createSqlType(SqlTypeName.ANY));
        }

        @Override
        public List<String> paramNames() {
            return PARAMETERS;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            // Operand 0 must be a TABLE and operand 1 a DESCRIPTOR(column).
            // SqlValidatorUtils.checkTableAndDescriptorOperands additionally verifies that
            // every descriptor column actually appears in the table schema.
            if (!SqlValidatorUtils.checkTableAndDescriptorOperands(callBinding, 1)) {
                return SqlValidatorUtils.throwValidationSignatureErrorOrReturnFalse(
                        callBinding, throwOnFailure);
            }

            final List<SqlNode> operands = callBinding.operands();
            final SqlCall descriptor = (SqlCall) operands.get(1);
            final List<SqlNode> descriptorCols = descriptor.getOperandList();
            if (descriptorCols.size() != 1) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        "APPLY_WATERMARK expects exactly one column in "
                                                + "DESCRIPTOR, but got "
                                                + descriptorCols.size()
                                                + ".")),
                        throwOnFailure);
            }

            // Resolve the rowtime column and assert it is a TIMESTAMP / TIMESTAMP_LTZ.
            final RelDataType tableType = callBinding.getOperandType(0);
            final SqlNameMatcher matcher =
                    callBinding.getValidator().getCatalogReader().nameMatcher();
            final SqlIdentifier columnId = (SqlIdentifier) descriptorCols.get(0);
            final String columnName =
                    columnId.isSimple() ? columnId.getSimple() : Util.last(columnId.names);
            final int columnIndex = matcher.indexOf(tableType.getFieldNames(), columnName);
            if (columnIndex < 0) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        String.format(
                                                "Column '%s' specified in DESCRIPTOR is not "
                                                        + "found in the input. Available columns: %s.",
                                                columnName, tableType.getFieldNames()))),
                        throwOnFailure);
            }
            final SqlTypeName rowtimeTypeName =
                    tableType.getFieldList().get(columnIndex).getType().getSqlTypeName();
            if (rowtimeTypeName != SqlTypeName.TIMESTAMP
                    && rowtimeTypeName != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        String.format(
                                                "APPLY_WATERMARK rowtime column '%s' must be of "
                                                        + "TIMESTAMP or TIMESTAMP_LTZ type, but was %s.",
                                                columnName, rowtimeTypeName))),
                        throwOnFailure);
            }

            // Operand 2: watermark expression must produce TIMESTAMP / TIMESTAMP_LTZ.
            final RelDataType watermarkType = callBinding.getOperandType(2);
            final SqlTypeName watermarkTypeName = watermarkType.getSqlTypeName();
            if (watermarkTypeName != SqlTypeName.TIMESTAMP
                    && watermarkTypeName != SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        String.format(
                                                "APPLY_WATERMARK watermark expression must "
                                                        + "evaluate to TIMESTAMP or TIMESTAMP_LTZ, but was %s.",
                                                watermarkTypeName))),
                        throwOnFailure);
            }

            return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.of(3);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName + "(TABLE input, DESCRIPTOR(rowtime_column), watermark_expression)";
        }

        @Override
        public boolean isOptional(int i) {
            return false;
        }
    }
}
