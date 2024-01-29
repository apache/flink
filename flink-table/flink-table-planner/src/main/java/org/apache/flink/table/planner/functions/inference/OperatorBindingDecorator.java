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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Decorator to expand operator arguments and reorder the arguments if using the named arguments.
 */
public class OperatorBindingDecorator {

    private final SqlOperatorBinding sqlOperatorBinding;

    private final List<RelDataType> argumentTypes;

    public OperatorBindingDecorator(SqlOperatorBinding binding) {
        this.sqlOperatorBinding = binding;
        this.argumentTypes = getArgumentTypes();
    }

    public int getOperandCount() {
        if (!argumentTypes.isEmpty()) {
            return argumentTypes.size();
        } else {
            return sqlOperatorBinding.getOperandCount();
        }
    }

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

    public boolean isOperandNull(int ordinal, boolean allowCast) {
        return sqlOperatorBinding.isOperandNull(ordinal, allowCast);
    }

    public boolean isOperandLiteral(int ordinal, boolean allowCast) {
        return sqlOperatorBinding.isOperandLiteral(ordinal, allowCast);
    }

    public <T extends Object> @Nullable T getOperandLiteralValue(int ordinal, Class<T> clazz) {
        return sqlOperatorBinding.getOperandLiteralValue(ordinal, clazz);
    }

    private List<RelDataType> getArgumentTypes() {
        SqlOperandTypeChecker sqlOperandTypeChecker =
                sqlOperatorBinding.getOperator().getOperandTypeChecker();
        if (sqlOperandTypeChecker != null
                && sqlOperandTypeChecker.isFixedParameters()
                && sqlOperandTypeChecker instanceof SqlOperandMetadata) {
            SqlOperandMetadata sqlOperandMetadata =
                    ((SqlOperandMetadata) sqlOperatorBinding.getOperator().getOperandTypeChecker());
            return sqlOperandMetadata.paramTypes(sqlOperatorBinding.getTypeFactory());
        } else {
            return Collections.emptyList();
        }
    }
}
