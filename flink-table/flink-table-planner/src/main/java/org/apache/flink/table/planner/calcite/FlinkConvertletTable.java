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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.Collections;

/**
 * Custom Flink {@link SqlRexConvertletTable} to add custom {@link SqlNode} to {@link RexNode}
 * conversions.
 */
@Internal
public class FlinkConvertletTable implements SqlRexConvertletTable {

    public static final FlinkConvertletTable INSTANCE = new FlinkConvertletTable();

    private FlinkConvertletTable() {}

    @Override
    public SqlRexConvertlet get(SqlCall call) {
        if (call.getOperator().isName("TRY_CAST", false)) {
            return this::convertTryCast;
        }
        return StandardConvertletTable.INSTANCE.get(call);
    }

    // Slightly modified version of StandardConvertletTable::convertCast
    private RexNode convertTryCast(SqlRexContext cx, final SqlCall call) {
        RelDataTypeFactory typeFactory = cx.getTypeFactory();
        final SqlNode leftNode = call.operand(0);
        final SqlNode rightNode = call.operand(1);

        final RexNode valueRex = cx.convertExpression(leftNode);

        RelDataType type;
        if (rightNode instanceof SqlIntervalQualifier) {
            type = typeFactory.createSqlIntervalType((SqlIntervalQualifier) rightNode);
        } else if (rightNode instanceof SqlDataTypeSpec) {
            SqlDataTypeSpec dataType = ((SqlDataTypeSpec) rightNode);
            type = dataType.deriveType(cx.getValidator());
            if (type == null) {
                type = cx.getValidator().getValidatedNodeType(dataType.getTypeName());
            }
        } else {
            throw new IllegalStateException(
                    "Invalid right argument type for TRY_CAST: " + rightNode);
        }
        type = typeFactory.createTypeWithNullability(type, true);

        if (SqlUtil.isNullLiteral(leftNode, false)) {
            final SqlValidatorImpl validator = (SqlValidatorImpl) cx.getValidator();
            validator.setValidatedNodeType(leftNode, type);
            return cx.convertExpression(leftNode);
        }
        return cx.getRexBuilder()
                .makeCall(
                        type, FlinkSqlOperatorTable.TRY_CAST, Collections.singletonList(valueRex));
    }
}
