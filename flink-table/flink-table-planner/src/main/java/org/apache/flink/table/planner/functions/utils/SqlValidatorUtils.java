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

package org.apache.flink.table.planner.functions.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;

import java.util.List;

/** Utility methods related to SQL validation. */
public class SqlValidatorUtils {

    public static void adjustTypeForArrayConstructor(
            RelDataType componentType, SqlOperatorBinding opBinding) {
        if (opBinding instanceof SqlCallBinding) {
            adjustTypeForMultisetConstructor(
                    componentType, componentType, (SqlCallBinding) opBinding);
        }
    }

    public static void adjustTypeForMapConstructor(
            Pair<RelDataType, RelDataType> componentType, SqlOperatorBinding opBinding) {
        if (opBinding instanceof SqlCallBinding) {
            adjustTypeForMultisetConstructor(
                    componentType.getKey(), componentType.getValue(), (SqlCallBinding) opBinding);
        }
    }

    /**
     * When the element element does not equal with the component type, making explicit casting.
     *
     * @param evenType derived type for element with even index
     * @param oddType derived type for element with odd index
     * @param sqlCallBinding description of call
     */
    private static void adjustTypeForMultisetConstructor(
            RelDataType evenType, RelDataType oddType, SqlCallBinding sqlCallBinding) {
        SqlCall call = sqlCallBinding.getCall();
        List<RelDataType> operandTypes = sqlCallBinding.collectOperandTypes();
        List<SqlNode> operands = call.getOperandList();
        RelDataType elementType;
        for (int i = 0; i < operands.size(); i++) {
            if (i % 2 == 0) {
                elementType = evenType;
            } else {
                elementType = oddType;
            }
            if (operandTypes.get(i).equalsSansFieldNames(elementType)) {
                continue;
            }
            call.setOperand(i, castTo(operands.get(i), elementType));
        }
    }

    private static SqlNode castTo(SqlNode node, RelDataType type) {
        return SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO,
                node,
                SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
    }
}
