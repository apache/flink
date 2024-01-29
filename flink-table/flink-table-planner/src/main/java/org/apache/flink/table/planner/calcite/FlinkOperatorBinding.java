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

package org.apache.flink.table.planner.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * The proxy implementation of {@link SqlOperatorBinding} can be used to correct the operator type
 * when using named parameter.
 */
public class FlinkOperatorBinding extends SqlOperatorBinding {

    private final SqlOperatorBinding sqlOperatorBinding;

    private final List<RelDataType> argumentTypes;

    public FlinkOperatorBinding(SqlOperatorBinding sqlOperatorBinding) {
        super(sqlOperatorBinding.getTypeFactory(), sqlOperatorBinding.getOperator());
        this.sqlOperatorBinding = sqlOperatorBinding;
        this.argumentTypes = getArgumentTypes();
    }

    @Override
    public int getOperandCount() {
        if (!argumentTypes.isEmpty()) {
            return argumentTypes.size();
        } else {
            return sqlOperatorBinding.getOperandCount();
        }
    }

    @Override
    public RelDataType getOperandType(int ordinal) {
        if (sqlOperatorBinding instanceof SqlCallBinding) {
            SqlNode sqlNode = ((SqlCallBinding) sqlOperatorBinding).operands().get(ordinal);
            if (sqlNode.getKind() == SqlKind.DEFAULT && !argumentTypes.isEmpty()) {
                return argumentTypes.get(ordinal);
            } else {
                return ((SqlCallBinding) sqlOperatorBinding)
                        .getValidator()
                        .deriveType(((SqlCallBinding) sqlOperatorBinding).getScope(), sqlNode);
            }
        } else if (sqlOperatorBinding instanceof RexCallBinding) {
            RexNode rexNode = ((RexCallBinding) sqlOperatorBinding).operands().get(ordinal);
            if (rexNode.getKind() == SqlKind.DEFAULT && !argumentTypes.isEmpty()) {
                return argumentTypes.get(ordinal);
            } else {
                return rexNode.getType();
            }
        }
        return sqlOperatorBinding.getOperandType(ordinal);
    }

    @Override
    public CalciteException newError(Resources.ExInst<SqlValidatorException> e) {
        return sqlOperatorBinding.newError(e);
    }

    @Override
    public int getGroupCount() {
        return sqlOperatorBinding.getGroupCount();
    }

    @Override
    public boolean hasFilter() {
        return sqlOperatorBinding.hasFilter();
    }

    @Override
    public SqlOperator getOperator() {
        return sqlOperatorBinding.getOperator();
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
        return sqlOperatorBinding.getTypeFactory();
    }

    @Override
    public @Nullable String getStringLiteralOperand(int ordinal) {
        return sqlOperatorBinding.getStringLiteralOperand(ordinal);
    }

    @Override // to be removed before 2.0
    public int getIntLiteralOperand(int ordinal) {
        return sqlOperatorBinding.getIntLiteralOperand(ordinal);
    }

    @Override
    public boolean isOperandNull(int ordinal, boolean allowCast) {
        return sqlOperatorBinding.isOperandNull(ordinal, allowCast);
    }

    @Override
    public boolean isOperandLiteral(int ordinal, boolean allowCast) {
        return sqlOperatorBinding.isOperandLiteral(ordinal, allowCast);
    }

    @Override
    public @Nullable Object getOperandLiteralValue(int ordinal, RelDataType type) {
        return sqlOperatorBinding.getOperandLiteralValue(ordinal, type);
    }

    @Override
    public @Nullable Comparable getOperandLiteralValue(int ordinal) {
        return sqlOperatorBinding.getOperandLiteralValue(ordinal);
    }

    @Override
    public SqlMonotonicity getOperandMonotonicity(int ordinal) {
        return sqlOperatorBinding.getOperandMonotonicity(ordinal);
    }

    @Override
    public List<RelDataType> collectOperandTypes() {
        return sqlOperatorBinding.collectOperandTypes();
    }

    @Override
    public @Nullable RelDataType getCursorOperand(int ordinal) {
        return sqlOperatorBinding.getCursorOperand(ordinal);
    }

    @Override
    public @Nullable String getColumnListParamInfo(
            int ordinal, String paramName, List<String> columnList) {
        return sqlOperatorBinding.getColumnListParamInfo(ordinal, paramName, columnList);
    }

    @Override
    public <T extends Object> @Nullable T getOperandLiteralValue(int ordinal, Class<T> clazz) {
        return sqlOperatorBinding.getOperandLiteralValue(ordinal, clazz);
    }

    private List<RelDataType> getArgumentTypes() {
        SqlOperandTypeChecker sqlOperandTypeChecker = getOperator().getOperandTypeChecker();
        if (sqlOperandTypeChecker != null
                && sqlOperandTypeChecker.isFixedParameters()
                && sqlOperandTypeChecker instanceof SqlOperandMetadata) {
            SqlOperandMetadata sqlOperandMetadata =
                    ((SqlOperandMetadata) getOperator().getOperandTypeChecker());
            return sqlOperandMetadata.paramTypes(getTypeFactory());
        } else {
            return Collections.emptyList();
        }
    }
}
