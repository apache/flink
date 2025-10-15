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

package org.apache.flink.table.planner.functions.sql.ml;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;

/**
 * {@link SqlVectorSearchTableFunction} implements an operator for search.
 *
 * <p>It allows four parameters:
 *
 * <ol>
 *   <li>a table
 *   <li>a descriptor to provide a column name from the input table
 *   <li>a query column from the left table
 *   <li>a literal value for top k
 * </ol>
 */
public class SqlVectorSearchTableFunction extends SqlFunction implements SqlTableFunction {

    private static final String PARAM_SEARCH_TABLE = "SEARCH_TABLE";
    private static final String PARAM_COLUMN_TO_SEARCH = "COLUMN_TO_SEARCH";
    private static final String PARAM_COLUMN_TO_QUERY = "COLUMN_TO_QUERY";
    private static final String PARAM_TOP_K = "TOP_K";

    private static final String OUTPUT_SCORE = "score";

    public SqlVectorSearchTableFunction() {
        super(
                "VECTOR_SEARCH",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                null,
                new OperandMetadataImpl(),
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return new SqlReturnTypeInference() {
            @Override
            public @Nullable RelDataType inferReturnType(SqlOperatorBinding opBinding) {
                final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
                final RelDataType inputRowType = opBinding.getOperandType(0);

                return typeFactory
                        .builder()
                        .kind(inputRowType.getStructKind())
                        .addAll(inputRowType.getFieldList())
                        .addAll(
                                SqlValidatorUtils.makeOutputUnique(
                                        inputRowType.getFieldList(),
                                        Collections.singletonList(
                                                new RelDataTypeFieldImpl(
                                                        OUTPUT_SCORE,
                                                        0,
                                                        typeFactory.createSqlType(
                                                                SqlTypeName.DOUBLE)))))
                        .build();
            }
        };
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }

    private static class OperandMetadataImpl implements SqlOperandMetadata {

        private static final List<String> PARAMETERS =
                Collections.unmodifiableList(
                        Arrays.asList(
                                PARAM_SEARCH_TABLE,
                                PARAM_COLUMN_TO_SEARCH,
                                PARAM_COLUMN_TO_QUERY,
                                PARAM_TOP_K));

        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory relDataTypeFactory) {
            return Collections.nCopies(
                    PARAMETERS.size(), relDataTypeFactory.createSqlType(SqlTypeName.ANY));
        }

        @Override
        public List<String> paramNames() {
            return PARAMETERS;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            // check vector table contains descriptor columns
            if (!SqlValidatorUtils.checkTableAndDescriptorOperands(callBinding, 1)) {
                return SqlValidatorUtils.throwValidationSignatureErrorOrReturnFalse(
                        callBinding, throwOnFailure);
            }

            List<SqlNode> operands = callBinding.operands();
            // check descriptor has one column
            SqlCall descriptor = (SqlCall) operands.get(1);
            List<SqlNode> descriptorCols = descriptor.getOperandList();
            if (descriptorCols.size() != 1) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        String.format(
                                                "Expect parameter COLUMN_TO_SEARCH for VECTOR_SEARCH only contains one column, but multiple columns are found in operand %s.",
                                                descriptor))),
                        throwOnFailure);
            }

            // check descriptor type is ARRAY<FLOAT> or ARRAY<DOUBLE>
            RelDataType searchTableType = callBinding.getOperandType(0);
            SqlNameMatcher matcher = callBinding.getValidator().getCatalogReader().nameMatcher();
            SqlIdentifier columnName = (SqlIdentifier) descriptorCols.get(0);
            String descriptorColName =
                    columnName.isSimple() ? columnName.getSimple() : Util.last(columnName.names);
            int index = matcher.indexOf(searchTableType.getFieldNames(), descriptorColName);
            RelDataType targetType = searchTableType.getFieldList().get(index).getType();
            LogicalType targetLogicalType = toLogicalType(targetType);

            if (!(targetLogicalType.is(LogicalTypeRoot.ARRAY)
                    && ((ArrayType) (targetLogicalType))
                            .getElementType()
                            .isAnyOf(LogicalTypeRoot.FLOAT, LogicalTypeRoot.DOUBLE))) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        String.format(
                                                "Expect search column `%s` type is ARRAY<FLOAT> or ARRAY<DOUBLE>, but its type is %s.",
                                                columnName, targetType))),
                        throwOnFailure);
            }

            // check query type is ARRAY<FLOAT> or ARRAY<DOUBLE>
            LogicalType sourceLogicalType = toLogicalType(callBinding.getOperandType(2));
            if (!LogicalTypeCasts.supportsImplicitCast(sourceLogicalType, targetLogicalType)) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        String.format(
                                                "Can not cast the query column type %s to target type %s. Please keep the query column type is same to the search column type.",
                                                sourceLogicalType, targetType))),
                        throwOnFailure);
            }

            // check top_k is a positive integer literal
            LogicalType topKType = toLogicalType(callBinding.getOperandType(3));
            if (!operands.get(3).getKind().equals(SqlKind.LITERAL)
                    || !topKType.is(LogicalTypeRoot.INTEGER)) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        String.format(
                                                "Expect parameter top_k is an INTEGER NOT NULL literal in VECTOR_SEARCH, but it is %s with type %s.",
                                                operands.get(3), topKType))),
                        throwOnFailure);
            }
            Integer topK = callBinding.getOperandLiteralValue(3, Integer.class);
            if (topK == null || topK <= 0) {
                return SqlValidatorUtils.throwExceptionOrReturnFalse(
                        Optional.of(
                                new ValidationException(
                                        String.format(
                                                "Parameter top_k must be greater than 0, but was %s.",
                                                topK))),
                        throwOnFailure);
            }
            return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.between(4, 4);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName
                    + "(TABLE search_table, DESCRIPTOR(column_to_search), column_to_query, top_k)";
        }

        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }

        @Override
        public boolean isOptional(int i) {
            return false;
        }

        @Override
        public boolean isFixedParameters() {
            return true;
        }
    }
}
