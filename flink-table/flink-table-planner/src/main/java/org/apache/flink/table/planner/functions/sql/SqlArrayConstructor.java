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

import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * {@link SqlOperator} for <code>ARRAY</code>, which makes explicit casting if the element type not
 * equals the derived component type.
 */
public class SqlArrayConstructor extends SqlArrayValueConstructor {

    private boolean isEmptyArray(SqlOperatorBinding opBinding) {
        return opBinding.collectOperandTypes().isEmpty();
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType type;
        if (this.isEmptyArray(opBinding)) {
            // Empty array, default type to integer
            type = new BasicSqlType(FlinkTypeSystem.INSTANCE, SqlTypeName.INTEGER);
        } else {
            type = getComponentType(opBinding.getTypeFactory(), opBinding.collectOperandTypes());
        }

        if (null == type) {
            return null;
        }

        // explicit cast elements to component type if they are not same
        SqlValidatorUtils.adjustTypeForArrayConstructor(type, opBinding);

        return SqlTypeUtil.createArrayType(opBinding.getTypeFactory(), type, false);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (this.isEmptyArray(callBinding)) {
            // Empty array
            return true;
        } else {
            return super.checkOperandTypes(callBinding, throwOnFailure);
        }
    }
}
