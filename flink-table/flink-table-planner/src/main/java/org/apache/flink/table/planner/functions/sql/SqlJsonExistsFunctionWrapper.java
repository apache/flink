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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.fun.SqlJsonExistsFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * This class is a wrapper class for the {@link SqlJsonExistsFunction} but only accepting a
 * character string as the JSON value, which is the only type that the code generation supports.
 */
public class SqlJsonExistsFunctionWrapper extends SqlJsonExistsFunction {

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
        }

        final RelDataType type = callBinding.getOperandType(0);
        if (SqlTypeUtil.isCharacter(type)) {
            return true;
        }
        if (throwOnFailure) {
            throw new ValidationException(
                    String.format(
                            "Unsupported type '%s' for the first argument of JSON_EXISTS(). "
                                    + "Supported types are CHAR, VARCHAR and STRING. Use "
                                    + "JSON_STRING() to convert a value of another type to a "
                                    + "JSON string.",
                            FlinkTypeFactory.toLogicalType(type).asSummaryString()));
        }
        return false;
    }
}
