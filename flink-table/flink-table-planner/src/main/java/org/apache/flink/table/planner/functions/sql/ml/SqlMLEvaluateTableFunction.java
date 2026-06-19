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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.ml.TaskType;
import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
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
import org.apache.calcite.util.NlsString;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * SqlMLEvaluateTableFunction implements an operator for model evaluation.
 *
 * <p>It allows six parameters:
 *
 * <ol>
 *   <li>a table name
 *   <li>a model name
 *   <li>a descriptor to provide label column names from the input table
 *   <li>a descriptor to provide feature column names from the input table
 *   <li>a task string describing the type of machine learning task. See {@link TaskType}.
 *   <li>an optional config map
 * </ol>
 *
 * <p>The function returns a MAP type containing evaluation metrics and results.
 */
public class SqlMLEvaluateTableFunction extends SqlMLTableFunction {

    public static final String PARAM_LABEL = "LABEL";
    public static final String PARAM_ARGS = "ARGS";
    public static final String PARAM_TASK = "TASK";

    public SqlMLEvaluateTableFunction() {
        super("ML_EVALUATE", new EvaluateOperandMetadata());
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

        // Create a MAP type for evaluation results
        RelDataType keyType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType valueType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RelDataType mapType = typeFactory.createMapType(keyType, valueType);

        return typeFactory
                .builder()
                .kind(inputRowType.getStructKind())
                .add("result", mapType)
                .build();
    }

    private static class EvaluateOperandMetadata implements SqlOperandMetadata {
        private static final List<String> PARAM_NAMES =
                List.of(
                        PARAM_INPUT,
                        PARAM_MODEL,
                        PARAM_LABEL,
                        PARAM_ARGS,
                        PARAM_TASK,
                        PARAM_CONFIG);
        private static final List<String> MANDATORY_PARAM_NAMES =
                List.of(PARAM_INPUT, PARAM_MODEL, PARAM_LABEL, PARAM_ARGS, PARAM_TASK);

        EvaluateOperandMetadata() {}

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
            if (!SqlValidatorUtils.checkTableAndDescriptorOperands(callBinding, 2, 3)) {
                return SqlValidatorUtils.throwValidationSignatureErrorOrReturnFalse(
                        callBinding, throwOnFailure);
            }

            if (!SqlValidatorUtils.throwExceptionOrReturnFalse(
                    checkModelSignature(callBinding, 3), throwOnFailure)) {
                return false;
            }

            if (!SqlValidatorUtils.throwExceptionOrReturnFalse(
                    checkModelOutputType(callBinding, 2), throwOnFailure)) {
                return false;
            }

            if (!SqlValidatorUtils.throwExceptionOrReturnFalse(
                    checkTask(callBinding.operand(4)), throwOnFailure)) {
                return false;
            }

            if (callBinding.getOperandCount() == PARAM_NAMES.size()) {
                // Check config parameters
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        checkConfig(callBinding, callBinding.operand(5)), throwOnFailure);
            }
            return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.between(MANDATORY_PARAM_NAMES.size(), PARAM_NAMES.size());
        }

        @Override
        public boolean isOptional(int i) {
            return i >= getOperandCountRange().getMin() && i < getOperandCountRange().getMax();
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName
                    + "(TABLE table_name, MODEL model_name, DESCRIPTOR(label_column), DESCRIPTOR(feature_columns), [task], [MAP[]])";
        }

        private static Optional<RuntimeException> checkTask(SqlNode node) {
            // Check if the task is a valid string
            if (!(node instanceof SqlCharStringLiteral)) {
                return Optional.of(
                        new ValidationException(
                                "Expected a valid task string literal, but got: "
                                        + node.getClass().getSimpleName()
                                        + "."));
            }

            String task = ((SqlCharStringLiteral) node).getValueAs(NlsString.class).getValue();
            if (!TaskType.isValidTaskType(task)) {
                return Optional.of(
                        new ValidationException(
                                "Unsupported task: "
                                        + task
                                        + ". Supported tasks are: "
                                        + Arrays.toString(TaskType.values())
                                        + "."));
            }
            return Optional.empty();
        }

        private static Optional<RuntimeException> checkModelOutputType(
                SqlCallBinding callBinding, int outputDescriptorIndex) {
            SqlCall descriptorCall = (SqlCall) callBinding.operand(outputDescriptorIndex);
            List<SqlNode> descriptCols = descriptorCall.getOperandList();
            if (descriptCols.size() != 1) {
                return Optional.of(
                        new ValidationException(
                                "Label descriptor must have exactly one column for evaluation."));
            }

            SqlValidator validator = callBinding.getValidator();
            SqlModelCall modelCall = (SqlModelCall) callBinding.operand(1);
            RelDataType modelOutputType = modelCall.getOutputType(validator);
            if (modelOutputType.getFieldCount() != 1) {
                return Optional.of(
                        new ValidationException(
                                "Model output must have exactly one field for evaluation."));
            }

            final RelDataType tableType = validator.getValidatedNodeType(callBinding.operand(0));
            final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
            Tuple3<Boolean, LogicalType, LogicalType> result =
                    checkModelDescriptorType(
                            tableType,
                            modelOutputType.getFieldList().get(0).getType(),
                            descriptCols.get(0),
                            matcher);
            if (!result.f0) {
                return Optional.of(
                        new ValidationException(
                                String.format(
                                        "Label descriptor column type %s cannot be assigned to model output type %s for evaluation.",
                                        result.f1, result.f2)));
            }
            return Optional.empty();
        }
    }
}
