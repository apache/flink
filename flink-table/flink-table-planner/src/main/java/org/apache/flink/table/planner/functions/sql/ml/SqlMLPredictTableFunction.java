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

package org.apache.flink.table.planner.functions.sql.ml;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlModelCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;

/**
 * SqlMlPredictTableFunction implements an operator for prediction.
 *
 * <p>It allows four parameters:
 *
 * <ol>
 *   <li>a table
 *   <li>a model name
 *   <li>a descriptor to provide a column name from the input table
 *   <li>an optional config map
 * </ol>
 */
public class SqlMLPredictTableFunction extends SqlMLTableFunction {

    public SqlMLPredictTableFunction() {
        super("ML_PREDICT", new PredictOperandMetadata());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides because the first parameter of ML table-value function is an explicit TABLE
     * parameter, which is not scalar.
     */
    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }

    @Override
    protected RelDataType inferRowType(SqlOperatorBinding opBinding) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType inputRowType = opBinding.getOperandType(0);
        final RelDataType modelOutputRowType = opBinding.getOperandType(1);

        return typeFactory
                .builder()
                .kind(inputRowType.getStructKind())
                .addAll(inputRowType.getFieldList())
                .addAll(
                        SqlValidatorUtils.makeOutputUnique(
                                inputRowType.getFieldList(), modelOutputRowType.getFieldList()))
                .build();
    }

    private static class PredictOperandMetadata implements SqlOperandMetadata {
        private static final List<String> PARAM_NAMES =
                List.of(PARAM_INPUT, PARAM_MODEL, PARAM_COLUMN, PARAM_CONFIG);
        private static final List<String> MANDATORY_PARAM_NAMES =
                List.of(PARAM_INPUT, PARAM_MODEL, PARAM_COLUMN);

        PredictOperandMetadata() {}

        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
            return Collections.nCopies(
                    PARAM_NAMES.size(), typeFactory.createSqlType(SqlTypeName.ANY));
        }

        @Override
        public List<String> paramNames() {
            return PARAM_NAMES;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!SqlValidatorUtils.checkTableAndDescriptorOperands(callBinding, 2)) {
                return SqlValidatorUtils.throwValidationSignatureErrorOrReturnFalse(
                        callBinding, throwOnFailure);
            }

            if (!SqlValidatorUtils.throwExceptionOrReturnFalse(
                    checkModelSignature(callBinding), throwOnFailure)) {
                return false;
            }

            return SqlValidatorUtils.throwExceptionOrReturnFalse(
                    checkConfig(callBinding), throwOnFailure);
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.between(MANDATORY_PARAM_NAMES.size(), PARAM_NAMES.size());
        }

        @Override
        public boolean isOptional(int i) {
            return i > getOperandCountRange().getMin() && i <= getOperandCountRange().getMax();
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName
                    + "(TABLE table_name, MODEL model_name, DESCRIPTOR(input_columns), [MAP[]])";
        }

        private static Optional<RuntimeException> checkModelSignature(SqlCallBinding callBinding) {
            SqlValidator validator = callBinding.getValidator();

            // Check second operand is SqlModelCall
            if (!(callBinding.operand(1) instanceof SqlModelCall)) {
                return Optional.of(
                        new ValidationException("Second operand must be a model identifier."));
            }

            // Get descriptor columns
            SqlCall descriptorCall = (SqlCall) callBinding.operand(2);
            List<SqlNode> descriptCols = descriptorCall.getOperandList();

            // Get model input size
            SqlModelCall modelCall = (SqlModelCall) callBinding.operand(1);
            RelDataType modelInputType = modelCall.getInputType(validator);

            // Check sizes match
            if (descriptCols.size() != modelInputType.getFieldCount()) {
                return Optional.of(
                        new ValidationException(
                                String.format(
                                        "Number of descriptor input columns (%d) does not match model input size (%d)",
                                        descriptCols.size(), modelInputType.getFieldCount())));
            }

            // Check types match
            final RelDataType tableType = validator.getValidatedNodeType(callBinding.operand(0));
            final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
            for (int i = 0; i < descriptCols.size(); i++) {
                SqlIdentifier columnName = (SqlIdentifier) descriptCols.get(i);
                String descriptColName =
                        columnName.isSimple()
                                ? columnName.getSimple()
                                : Util.last(columnName.names);
                int index = matcher.indexOf(tableType.getFieldNames(), descriptColName);
                RelDataType sourceType = tableType.getFieldList().get(index).getType();
                RelDataType targetType = modelInputType.getFieldList().get(i).getType();

                LogicalType sourceLogicalType = toLogicalType(sourceType);
                LogicalType targetLogicalType = toLogicalType(targetType);

                if (!LogicalTypeCasts.supportsImplicitCast(sourceLogicalType, targetLogicalType)) {
                    return Optional.of(
                            new ValidationException(
                                    String.format(
                                            "Descriptor column type %s cannot be assigned to model input type %s at position %d",
                                            sourceLogicalType, targetLogicalType, i)));
                }
            }

            return Optional.empty();
        }

        private static Optional<RuntimeException> checkConfig(SqlCallBinding callBinding) {
            if (callBinding.getOperandCount() < PARAM_NAMES.size()) {
                return Optional.empty();
            }

            SqlNode configNode = callBinding.operand(3);
            if (!configNode.getKind().equals(SqlKind.MAP_VALUE_CONSTRUCTOR)) {
                return Optional.of(new ValidationException("Config param should be a MAP."));
            }

            // Map operands can only be SqlCharStringLiteral or cast of SqlCharStringLiteral
            SqlCall mapCall = (SqlCall) configNode;
            for (int i = 0; i < mapCall.operandCount(); i++) {
                SqlNode operand = mapCall.operand(i);
                if (operand instanceof SqlCharStringLiteral) {
                    continue;
                }
                if (operand.getKind().equals(SqlKind.CAST)) {
                    SqlCall castCall = (SqlCall) operand;
                    if (castCall.operand(0) instanceof SqlCharStringLiteral) {
                        continue;
                    }
                }
                return Optional.of(
                        new ValidationException(
                                String.format(
                                        "ML_PREDICT config param can only be a MAP of string literals. The item at position %d is %s.",
                                        i, operand)));
            }

            return Optional.empty();
        }
    }
}
