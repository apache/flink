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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlJsonQueryFunction;
import org.apache.calcite.sql.fun.SqlJsonValueFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.Optional;

import static org.apache.flink.table.planner.plan.type.FlinkReturnTypes.VARCHAR_FORCE_NULLABLE;

/**
 * This class is a wrapper class for the {@link SqlJsonQueryFunction} but using the {@code
 * VARCHAR_FORCE_NULLABLE} return type inference.
 */
class SqlJsonQueryFunctionWrapper extends SqlJsonQueryFunction {
    private final SqlReturnTypeInference returnTypeInference;

    SqlJsonQueryFunctionWrapper() {
        this.returnTypeInference =
                ReturnTypes.cascade(
                                SqlJsonQueryFunctionWrapper::explicitTypeSpec,
                                SqlTypeTransforms.FORCE_NULLABLE)
                        .orElse(VARCHAR_FORCE_NULLABLE);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType returnType = returnTypeInference.inferReturnType(opBinding);
        if (returnType == null) {
            throw new IllegalArgumentException(
                    "Cannot infer return type for "
                            + opBinding.getOperator()
                            + "; operand types: "
                            + opBinding.collectOperandTypes());
        }
        return returnType;
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return returnTypeInference;
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
        }

        if (callBinding.getOperandCount() >= 6) {
            final RelDataType type = SqlTypeUtil.deriveType(callBinding, callBinding.operand(5));
            if (SqlTypeUtil.isArray(type) && !SqlTypeUtil.isCharacter(type.getComponentType())) {
                if (throwOnFailure) {
                    throw new ValidationException(
                            String.format(
                                    "Unsupported array element type '%s' for RETURNING ARRAY in JSON_QUERY().",
                                    type.getComponentType()));
                } else {
                    return false;
                }
            }
            return true;
        }

        return true;
    }

    /**
     * Copied and modified from the original {@link SqlJsonValueFunction}.
     *
     * <p>Changes: Instead of returning {@link Optional} this method returns null directly.
     */
    private static RelDataType explicitTypeSpec(SqlOperatorBinding opBinding) {
        if (opBinding.getOperandCount() >= 6) {
            return opBinding.getOperandType(5);
        }
        return null;
    }
}
