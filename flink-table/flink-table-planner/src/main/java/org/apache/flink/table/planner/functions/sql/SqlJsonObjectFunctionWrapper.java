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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlJsonObjectFunction;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static org.apache.flink.table.planner.plan.type.FlinkReturnTypes.VARCHAR_NOT_NULL;

/**
 * This class is a wrapper class for the {@link SqlJsonObjectFunction} but using the {@code
 * VARCHAR_NOT_NULL} return type inference.
 */
class SqlJsonObjectFunctionWrapper extends SqlJsonObjectFunction {

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType returnType = VARCHAR_NOT_NULL.inferReturnType(opBinding);
        if (returnType == null) {
            throw new IllegalArgumentException(
                    "Cannot infer return type for "
                            + opBinding.getOperator()
                            + "; operand types: "
                            + opBinding.collectOperandTypes());
        } else {
            return returnType;
        }
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return VARCHAR_NOT_NULL;
    }
}
