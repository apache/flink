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

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Base class for a table-valued function that computes windows. Examples include {@code TUMBLE},
 * {@code HOP}, {@code CUMULATE} and {@code SESSION}.
 *
 * <p>Note: we copied the implementation from Calcite's {@link
 * org.apache.calcite.sql.SqlWindowTableFunction}, but support return additional {@code window_time}
 * time attribute column which should keep the same type with original time attribute.
 */
public class SqlWindowTableFunction extends SqlFunction implements SqlTableFunction {

    /** The data source which the table function computes with. */
    protected static final String PARAM_DATA = "DATA";

    /** The time attribute column. Also known as the event time. */
    protected static final String PARAM_TIMECOL = "TIMECOL";

    /** The window duration INTERVAL. */
    protected static final String PARAM_SIZE = "SIZE";

    /** The optional align offset for each window. */
    protected static final String PARAM_OFFSET = "OFFSET";

    /** The session key(s), only used for SESSION window. */
    protected static final String PARAM_KEY = "KEY";

    /** The slide interval, only used for HOP window. */
    protected static final String PARAM_SLIDE = "SLIDE";

    /** The slide interval, only used for HOP window. */
    protected static final String PARAM_STEP = "STEP";

    /**
     * Type-inference strategy whereby the row type of a table function call is a ROW, which is
     * combined from the row type of operand #0 (which is a TABLE) and two additional fields. The
     * fields are as follows:
     *
     * <ol>
     *   <li>{@code window_start}: TIMESTAMP type to indicate a window's start
     *   <li>{@code window_end}: TIMESTAMP type to indicate a window's end
     *   <li>{@code window_time}: TIMESTAMP type with time attribute metadata to indicate a window's
     *       time attribute
     * </ol>
     */
    public static final SqlReturnTypeInference ARG0_TABLE_FUNCTION_WINDOWING =
            SqlWindowTableFunction::inferRowType;

    /** Creates a window table function with a given name. */
    public SqlWindowTableFunction(String name, SqlOperandMetadata operandMetadata) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                null,
                operandMetadata,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public SqlOperandMetadata getOperandTypeChecker() {
        return (SqlOperandMetadata) super.getOperandTypeChecker();
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return ARG0_TABLE_FUNCTION_WINDOWING;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides because the first parameter of table-value function windowing is an explicit
     * TABLE parameter, which is not scalar.
     */
    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }

    /** Helper for {@link #ARG0_TABLE_FUNCTION_WINDOWING}. */
    private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType inputRowType = opBinding.getOperandType(0);
        final RelDataType descriptorType = opBinding.getOperandType(1);
        final RelDataTypeField timeField = descriptorType.getFieldList().get(0);
        final RelDataType timeAttributeType;
        if (timeField.getType().getSqlTypeName() == SqlTypeName.NULL) {
            // the type is not inferred yet, we should infer the type here,
            // see org.apache.flink.table.planner.functions.sql.SqlDescriptorOperator.deriveType
            RelDataTypeField field = inputRowType.getField(timeField.getName(), false, false);
            if (field == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Can't find the time attribute field '%s' in the input schema %s.",
                                timeField.getName(), inputRowType.getFullTypeString()));
            }
            timeAttributeType = field.getType();
        } else {
            // the type has been inferred, use it directly
            timeAttributeType = timeField.getType();
        }
        return inferRowType(typeFactory, inputRowType, timeAttributeType);
    }

    public static RelDataType inferRowType(
            RelDataTypeFactory typeFactory,
            RelDataType inputRowType,
            RelDataType timeAttributeType) {
        return typeFactory
                .builder()
                .kind(inputRowType.getStructKind())
                .addAll(inputRowType.getFieldList())
                .add("window_start", SqlTypeName.TIMESTAMP, 3)
                .add("window_end", SqlTypeName.TIMESTAMP, 3)
                .add("window_time", typeFactory.createTypeWithNullability(timeAttributeType, false))
                .build();
    }

    /** Partial implementation of operand type checker. */
    protected abstract static class AbstractOperandMetadata implements SqlOperandMetadata {
        final List<String> paramNames;
        final int mandatoryParamCount;

        AbstractOperandMetadata(List<String> paramNames, int mandatoryParamCount) {
            this.paramNames = ImmutableList.copyOf(paramNames);
            this.mandatoryParamCount = mandatoryParamCount;
            Preconditions.checkArgument(
                    mandatoryParamCount >= 0 && mandatoryParamCount <= paramNames.size());
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.between(mandatoryParamCount, paramNames.size());
        }

        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
            return Collections.nCopies(
                    paramNames.size(), typeFactory.createSqlType(SqlTypeName.ANY));
        }

        @Override
        public List<String> paramNames() {
            return paramNames;
        }

        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }

        @Override
        public boolean isOptional(int i) {
            return i > getOperandCountRange().getMin() && i <= getOperandCountRange().getMax();
        }

        boolean throwValidationSignatureErrorOrReturnFalse(
                SqlCallBinding callBinding, boolean throwOnFailure) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            } else {
                return false;
            }
        }

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        boolean throwExceptionOrReturnFalse(Optional<RuntimeException> e, boolean throwOnFailure) {
            if (e.isPresent()) {
                if (throwOnFailure) {
                    throw e.get();
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        /**
         * Checks whether the heading operands are in the form {@code (ROW, DESCRIPTOR, DESCRIPTOR
         * ..., other params)}, returning whether successful, and throwing if any columns are not
         * found.
         *
         * @param callBinding The call binding
         * @param descriptorCount The number of descriptors following the first operand (e.g. the
         *     table)
         * @return true if validation passes; throws if any columns are not found
         */
        boolean checkTableAndDescriptorOperands(SqlCallBinding callBinding, int descriptorCount) {
            final SqlNode operand0 = callBinding.operand(0);
            final SqlValidator validator = callBinding.getValidator();
            final RelDataType type = validator.getValidatedNodeType(operand0);
            if (type.getSqlTypeName() != SqlTypeName.ROW) {
                return false;
            }
            for (int i = 1; i < descriptorCount + 1; i++) {
                final SqlNode operand = callBinding.operand(i);
                if (operand.getKind() != SqlKind.DESCRIPTOR) {
                    return false;
                }
                validateColumnNames(
                        validator, type.getFieldNames(), ((SqlCall) operand).getOperandList());
            }
            return true;
        }

        /**
         * Checks whether the type that the operand of time col descriptor refers to is valid.
         *
         * @param callBinding The call binding
         * @param pos The position of the descriptor at the operands of the call
         * @return true if validation passes, false otherwise
         */
        Optional<RuntimeException> checkTimeColumnDescriptorOperand(
                SqlCallBinding callBinding, int pos) {
            SqlValidator validator = callBinding.getValidator();
            SqlNode operand0 = callBinding.operand(0);
            RelDataType type = validator.getValidatedNodeType(operand0);
            List<SqlNode> operands = ((SqlCall) callBinding.operand(pos)).getOperandList();
            SqlIdentifier identifier = (SqlIdentifier) operands.get(0);
            String columnName = identifier.getSimple();
            SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
            for (RelDataTypeField field : type.getFieldList()) {
                if (matcher.matches(field.getName(), columnName)) {
                    if (FlinkTypeFactory.isTimeIndicatorType(field.getType())) {
                        return Optional.empty();
                    } else {
                        ValidationException exception =
                                new ValidationException(
                                        String.format(
                                                "The window function %s requires the timecol is a time attribute type, but is %s.",
                                                callBinding.getOperator().getAllowedSignatures(),
                                                field.getType()));
                        return Optional.of(exception);
                    }
                }
            }
            IllegalArgumentException error =
                    new IllegalArgumentException(
                            String.format(
                                    "Can't find the time attribute field '%s' in the input schema %s.",
                                    columnName, type.getFullTypeString()));
            return Optional.of(error);
        }

        /**
         * Checks whether the operands starting from position {@code startPos} are all of type
         * {@code INTERVAL}, returning whether successful.
         *
         * @param callBinding The call binding
         * @param startPos The start position to validate (starting index is 0)
         * @return true if validation passes
         */
        boolean checkIntervalOperands(SqlCallBinding callBinding, int startPos) {
            final SqlValidator validator = callBinding.getValidator();
            for (int i = startPos; i < callBinding.getOperandCount(); i++) {
                final RelDataType type = validator.getValidatedNodeType(callBinding.operand(i));
                if (!SqlTypeUtil.isInterval(type)) {
                    return false;
                }
            }
            return true;
        }

        void validateColumnNames(
                SqlValidator validator, List<String> fieldNames, List<SqlNode> columnNames) {
            final SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
            for (SqlNode columnName : columnNames) {
                final String name = ((SqlIdentifier) columnName).getSimple();
                if (matcher.indexOf(fieldNames, name) < 0) {
                    throw SqlUtil.newContextException(
                            columnName.getParserPosition(), RESOURCE.unknownIdentifier(name));
                }
            }
        }
    }
}
