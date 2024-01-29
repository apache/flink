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

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter.toRelDataType;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.adaptArguments;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createInvalidCallException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createInvalidInputException;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createUnexpectedException;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;

/**
 * A {@link SqlOperandTypeChecker} backed by {@link TypeInference}.
 *
 * <p>Note: This class must be kept in sync with {@link TypeInferenceUtil}.
 */
@Internal
public final class TypeInferenceOperandChecker
        implements SqlOperandTypeChecker, SqlOperandMetadata {

    private final DataTypeFactory dataTypeFactory;

    private final FunctionDefinition definition;

    private final TypeInference typeInference;

    private final SqlOperandCountRange countRange;

    public TypeInferenceOperandChecker(
            DataTypeFactory dataTypeFactory,
            FunctionDefinition definition,
            TypeInference typeInference) {
        this.dataTypeFactory = dataTypeFactory;
        this.definition = definition;
        this.typeInference = typeInference;
        this.countRange =
                new ArgumentCountRange(typeInference.getInputTypeStrategy().getArgumentCount());
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        final CallContext callContext =
                new CallBindingCallContext(dataTypeFactory, definition, callBinding, null);
        try {
            return checkOperandTypesOrError(callBinding, callContext);
        } catch (ValidationException e) {
            if (throwOnFailure) {
                throw createInvalidCallException(callContext, e);
            }
            return false;
        } catch (Throwable t) {
            throw createUnexpectedException(callContext, t);
        }
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        if (typeInference.getOptionalArguments().isPresent()
                && typeInference.getOptionalArguments().get().stream()
                        .anyMatch(Boolean::booleanValue)) {
            int notOptionalCount =
                    (int)
                            typeInference.getOptionalArguments().get().stream()
                                    .filter(optional -> !optional)
                                    .count();
            ArgumentCount argumentCount =
                    ConstantArgumentCount.between(notOptionalCount, countRange.getMax());
            return new ArgumentCountRange(argumentCount);
        } else {
            return countRange;
        }
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        return TypeInferenceUtil.generateSignature(typeInference, opName, definition);
    }

    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

    @Override
    public boolean isOptional(int i) {
        Optional<List<Boolean>> optionalArguments = typeInference.getOptionalArguments();
        if (optionalArguments.isPresent()) {
            return optionalArguments.get().get(i);
        } else {
            return false;
        }
    }

    @Override
    public boolean isFixedParameters() {
        // This method returns true only if optional arguments are declared and at least one
        // optional argument is present.
        // Otherwise, it defaults to false, bypassing the parameter check in Calcite.
        return typeInference.getOptionalArguments().isPresent()
                && typeInference.getOptionalArguments().get().stream()
                        .anyMatch(Boolean::booleanValue);
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        return typeInference
                .getTypedArguments()
                .map(
                        types ->
                                types.stream()
                                        .map(
                                                type ->
                                                        toRelDataType(
                                                                type.getLogicalType(), typeFactory))
                                        .collect(Collectors.toList()))
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Could not find the argument types. "
                                                + "Currently named arguments are not supported "
                                                + "for varArgs and multi different argument names "
                                                + "with overload function"));
    }

    @Override
    public List<String> paramNames() {
        return typeInference
                .getNamedArguments()
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Could not find the argument names. "
                                                + "Currently named arguments are not supported "
                                                + "for varArgs and multi different argument names "
                                                + "with overload function"));
    }

    // --------------------------------------------------------------------------------------------

    private boolean checkOperandTypesOrError(SqlCallBinding callBinding, CallContext callContext) {
        final CallContext adaptedCallContext;
        try {
            adaptedCallContext = adaptArguments(typeInference, callContext, null);
        } catch (ValidationException e) {
            throw createInvalidInputException(typeInference, callContext, e);
        }

        insertImplicitCasts(callBinding, adaptedCallContext.getArgumentDataTypes());

        return true;
    }

    private void insertImplicitCasts(SqlCallBinding callBinding, List<DataType> expectedDataTypes) {
        final FlinkTypeFactory flinkTypeFactory = unwrapTypeFactory(callBinding);
        final List<SqlNode> operands = callBinding.operands();
        for (int i = 0; i < operands.size(); i++) {
            final LogicalType expectedType = expectedDataTypes.get(i).getLogicalType();
            SqlNode sqlNode = operands.get(i);
            // skip default node
            if (sqlNode.getKind() == SqlKind.DEFAULT) {
                continue;
            }
            final LogicalType argumentType =
                    toLogicalType(SqlTypeUtil.deriveType(callBinding, operands.get(i)));

            if (!supportsAvoidingCast(argumentType, expectedType)) {
                final RelDataType expectedRelDataType =
                        flinkTypeFactory.createFieldTypeFromLogicalType(expectedType);
                final SqlNode castedOperand = castTo(operands.get(i), expectedRelDataType);
                callBinding.getCall().setOperand(i, castedOperand);
                updateInferredType(callBinding.getValidator(), castedOperand, expectedRelDataType);
            }
        }
    }

    /** Adopted from {@link org.apache.calcite.sql.validate.implicit.AbstractTypeCoercion}. */
    private SqlNode castTo(SqlNode node, RelDataType type) {
        return SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO,
                node,
                SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
    }

    /** Adopted from {@link org.apache.calcite.sql.validate.implicit.AbstractTypeCoercion}. */
    private void updateInferredType(SqlValidator validator, SqlNode node, RelDataType type) {
        validator.setValidatedNodeType(node, type);
        final SqlValidatorNamespace namespace = validator.getNamespace(node);
        if (namespace != null) {
            namespace.setType(type);
        }
    }
}
