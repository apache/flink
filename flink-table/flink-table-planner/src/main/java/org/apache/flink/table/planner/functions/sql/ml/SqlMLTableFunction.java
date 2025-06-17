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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.types.Either;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlModelCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions.ASYNC;
import static org.apache.flink.table.api.config.MLPredictRuntimeConfigOptions.ASYNC_CAPACITY;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CHARACTER_STRING;

/**
 * Base class for a table-valued function that works with models. Examples include {@code
 * ML_PREDICT}.
 */
public abstract class SqlMLTableFunction extends SqlFunction implements SqlTableFunction {

    private static final String TABLE_INPUT_ERROR =
            "SqlMLTableFunction must have only one table as first operand.";

    protected static final String PARAM_INPUT = "INPUT";
    protected static final String PARAM_MODEL = "MODEL";
    protected static final String PARAM_COLUMN = "ARGS";
    protected static final String PARAM_CONFIG = "CONFIG";

    public SqlMLTableFunction(String name, SqlOperandMetadata operandMetadata) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                null,
                operandMetadata,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public void validateCall(
            SqlCall call,
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlValidatorScope operandScope) {
        assert call.getOperator() == this;
        final List<SqlNode> operandList = call.getOperandList();

        // ML table function should take only one table as input and use descriptor to reference
        // columns in the table. The scope for descriptor validation should be the input table which
        // is also an operand of the call. We defer the validation of the descriptor since
        // validation here will quality the descriptor columns to be NOT simple name which
        // complicates checks in later stages. We validate the descriptor columns appear in table
        // column in SqlOperandMetadata.
        boolean foundSelect = false;
        for (SqlNode operand : operandList) {
            if (operand.getKind().equals(SqlKind.DESCRIPTOR)) {
                continue;
            }
            if (operand.getKind().equals(SqlKind.SET_SEMANTICS_TABLE)) {
                operand = ((SqlCall) operand).getOperandList().get(0);
                if (foundSelect) {
                    throw new ValidationException(TABLE_INPUT_ERROR);
                }
                foundSelect = true;
            }

            if (operand.getKind().equals(SqlKind.SELECT)) {
                if (foundSelect) {
                    throw new ValidationException(TABLE_INPUT_ERROR);
                }
                foundSelect = true;
            }
            operand.validate(validator, scope);
        }
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return this::inferRowType;
    }

    protected abstract RelDataType inferRowType(SqlOperatorBinding opBinding);

    protected static Optional<RuntimeException> checkModelSignature(
            SqlCallBinding callBinding, int inputDescriptorIndex) {
        SqlValidator validator = callBinding.getValidator();

        // Check second operand is SqlModelCall
        if (!(callBinding.operand(1) instanceof SqlModelCall)) {
            return Optional.of(
                    new ValidationException("Second operand must be a model identifier."));
        }

        // Get input descriptor columns
        SqlCall descriptorCall = (SqlCall) callBinding.operand(inputDescriptorIndex);
        List<SqlNode> descriptCols = descriptorCall.getOperandList();

        // Get model input size
        SqlModelCall modelCall = (SqlModelCall) callBinding.operand(1);
        RelDataType modelInputType = modelCall.getInputType(validator);

        // Check sizes match
        if (descriptCols.size() != modelInputType.getFieldCount()) {
            return Optional.of(
                    new ValidationException(
                            String.format(
                                    "Number of input descriptor columns (%d) does not match model input size (%d).",
                                    descriptCols.size(), modelInputType.getFieldCount())));
        }

        // Check input types match
        final RelDataType tableType = validator.getValidatedNodeType(callBinding.operand(0));
        final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
        for (int i = 0; i < descriptCols.size(); i++) {
            Tuple3<Boolean, LogicalType, LogicalType> result =
                    checkModelDescriptorType(
                            tableType,
                            modelInputType.getFieldList().get(i).getType(),
                            descriptCols.get(i),
                            matcher);
            if (!result.f0) {
                return Optional.of(
                        new ValidationException(
                                String.format(
                                        "Input descriptor column type %s cannot be assigned to model input type %s at position %d.",
                                        result.f1, result.f2, i)));
            }
        }
        return Optional.empty();
    }

    protected static Tuple3<Boolean, LogicalType, LogicalType> checkModelDescriptorType(
            RelDataType tableType,
            RelDataType modelType,
            SqlNode descriptorNode,
            SqlNameMatcher matcher) {
        SqlIdentifier columnName = (SqlIdentifier) descriptorNode;
        String descriptorColName =
                columnName.isSimple() ? columnName.getSimple() : Util.last(columnName.names);
        int index = matcher.indexOf(tableType.getFieldNames(), descriptorColName);
        RelDataType sourceType = tableType.getFieldList().get(index).getType();

        LogicalType sourceLogicalType = toLogicalType(sourceType);
        LogicalType targetLogicalType = toLogicalType(modelType);

        return Tuple3.of(
                LogicalTypeCasts.supportsImplicitCast(sourceLogicalType, targetLogicalType),
                sourceLogicalType,
                targetLogicalType);
    }

    protected static Optional<RuntimeException> checkConfig(
            SqlCallBinding callBinding, SqlNode configNode) {
        if (!configNode.getKind().equals(SqlKind.MAP_VALUE_CONSTRUCTOR)) {
            return Optional.of(new ValidationException("Config param should be a MAP."));
        }

        RelDataType mapType = callBinding.getValidator().getValidatedNodeType(configNode);

        assert mapType instanceof MapSqlType;

        LogicalType keyType = toLogicalType(mapType.getKeyType());
        LogicalType valueType = toLogicalType(mapType.getValueType());
        if (!keyType.is(CHARACTER_STRING) || !valueType.is(CHARACTER_STRING)) {
            return Optional.of(
                    new ValidationException(
                            String.format(
                                    "Config param can only be a MAP of string literals but node's type is %s at position %s.",
                                    mapType, configNode.getParserPosition())));
        }

        List<SqlNode> operands = ((SqlCall) configNode).getOperandList();
        Map<String, String> runtimeConfig = new HashMap<>();
        for (int i = 0; i < operands.size(); i += 2) {
            Either<String, RuntimeException> key =
                    reduceLiteral(operands.get(i), callBinding.getValidator());
            Either<String, RuntimeException> value =
                    reduceLiteral(operands.get(i + 1), callBinding.getValidator());

            if (key.isRight()) {
                return Optional.of(key.right());
            } else if (value.isRight()) {
                return Optional.of(value.right());
            } else {
                runtimeConfig.put(key.left(), value.left());
            }
        }

        return checkConfigValue(runtimeConfig);
    }

    private static Optional<RuntimeException> checkConfigValue(Map<String, String> runtimeConfig) {
        Configuration config = Configuration.fromMap(runtimeConfig);
        try {
            MLPredictRuntimeConfigOptions.getSupportedOptions().forEach(config::get);
        } catch (Throwable t) {
            return Optional.of(new ValidationException("Failed to parse the config.", t));
        }

        // option value check
        // async options are all optional
        Boolean async = config.get(ASYNC);
        if (Boolean.TRUE.equals(async)) {
            Integer capacity = config.get(ASYNC_CAPACITY);
            if (capacity != null && capacity <= 0) {
                return Optional.of(
                        new ValidationException(
                                String.format(
                                        "Invalid runtime config option '%s'. Its value should be positive integer but was %s.",
                                        ASYNC_CAPACITY.key(), capacity)));
            }
        }

        return Optional.empty();
    }

    private static Either<String, RuntimeException> reduceLiteral(
            SqlNode operand, SqlValidator validator) {
        if (operand instanceof SqlCharStringLiteral) {
            return Either.Left(
                    ((SqlCharStringLiteral) operand).getValueAs(NlsString.class).getValue());
        } else if (operand.getKind() == SqlKind.CAST) {
            // CAST(CAST('v' AS STRING) AS STRING)
            SqlCall call = (SqlCall) operand;
            SqlDataTypeSpec dataType = call.operand(1);
            if (!toLogicalType(dataType.deriveType(validator)).is(CHARACTER_STRING)) {
                return Either.Right(
                        new ValidationException("Don't support to cast value to non-string type."));
            }
            return reduceLiteral((call.operand(0)), validator);
        } else {
            return Either.Right(
                    new ValidationException(
                            String.format(
                                    "Unsupported expression %s is in runtime config at position %s. Currently, "
                                            + "runtime config should be be a MAP of string literals.",
                                    operand, operand.getParserPosition())));
        }
    }
}
