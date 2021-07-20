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

import org.apache.flink.table.planner.functions.utils.SqlValidatorUtils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * {@link SqlOperator} for <code>MAP</code>, which makes explicit casting if the element type not
 * equals the derived component type.
 */
public class SqlMapConstructor extends SqlMapValueConstructor {

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        Pair<RelDataType, RelDataType> type =
                getComponentTypes(opBinding.getTypeFactory(), opBinding.collectOperandTypes());
        if (null == type) {
            return null;
        }

        // explicit cast elements to component type if they are not same
        SqlValidatorUtils.adjustTypeForMapConstructor(type, opBinding);

        return SqlTypeUtil.createMapType(opBinding.getTypeFactory(), type.left, type.right, false);
    }

    private Pair<RelDataType, RelDataType> getComponentTypes(
            RelDataTypeFactory typeFactory, List<RelDataType> argTypes) {
        return Pair.of(
                typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 0)),
                typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 1)));
    }
}
