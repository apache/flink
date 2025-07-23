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

package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Flink modifications
 *
 * <p>Lines 42 ~ 44 to mitigate the impact of CALCITE-6040.
 */
public class SqlMapValueConstructor extends SqlMultisetValueConstructor {
    public SqlMapValueConstructor() {
        // BEGIN FLINK MODIFICATION
        super("MAP", SqlKind.MAP_VALUE_CONSTRUCTOR);
        // END FLINK MODIFICATION
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        Pair<RelDataType, RelDataType> type =
                getComponentTypes(opBinding.getTypeFactory(), opBinding.collectOperandTypes());
        SqlValidatorUtil.adjustTypeForMapConstructor(type, opBinding);
        return SqlTypeUtil.createMapType(
                opBinding.getTypeFactory(),
                (RelDataType) Objects.requireNonNull(type.left, "inferred key type"),
                (RelDataType) Objects.requireNonNull(type.right, "inferred value type"),
                false);
    }

    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        List<RelDataType> argTypes = SqlTypeUtil.deriveType(callBinding, callBinding.operands());
        if (argTypes.size() == 0) {
            throw callBinding.newValidationError(Static.RESOURCE.mapRequiresTwoOrMoreArgs());
        } else if (argTypes.size() % 2 > 0) {
            throw callBinding.newValidationError(Static.RESOURCE.mapRequiresEvenArgCount());
        } else {
            Pair<RelDataType, RelDataType> componentType =
                    getComponentTypes(callBinding.getTypeFactory(), argTypes);
            if (null != componentType.left && null != componentType.right) {
                return true;
            } else if (throwOnFailure) {
                throw callBinding.newValidationError(Static.RESOURCE.needSameTypeParameter());
            } else {
                return false;
            }
        }
    }

    private static Pair<@Nullable RelDataType, @Nullable RelDataType> getComponentTypes(
            RelDataTypeFactory typeFactory, List<RelDataType> argTypes) {
        return Pair.of(
                typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 0)),
                typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 1)));
    }
}
