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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;

/**
 * {@link SqlFunction} for <code>ARRAY</code>, which makes explicit casting if the element type not
 * equals the derived component type.
 */
public class SqlArrayFunction extends SqlArrayValueConstructor {
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType type =
                getComponentType(opBinding.getTypeFactory(), opBinding.collectOperandTypes());
        if (null == type) {
            return null;
        }

        // explicit cast elements to component type if they are not same
        if (opBinding instanceof SqlCallBinding) {
            SqlCall call = ((SqlCallBinding) opBinding).getCall();
            List<RelDataType> operandTypes = opBinding.collectOperandTypes();
            List<SqlNode> operands = call.getOperandList();
            int idx = -1;
            for (RelDataType opType : operandTypes) {
                idx += 1;
                if (opType.equalsSansFieldNames(type)) {
                    continue;
                }
                call.setOperand(idx, castTo(operands.get(idx), type));
            }
        }

        return SqlTypeUtil.createArrayType(opBinding.getTypeFactory(), type, false);
    }

    private SqlNode castTo(SqlNode node, RelDataType type) {
        return SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO,
                node,
                SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
    }
}
