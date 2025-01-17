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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

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
        final SqlOperator operator = call.getOperator();
        if (operator == FlinkSqlOperatorTable.TRY_CAST) {
            return this::convertTryCast;
        } else if (operator instanceof SqlTableFunction) {
            return this::convertTableArgs;
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

    /**
     * Due to CALCITE-6204, we need to manually extract partition keys and order keys and convert
     * them to {@link RexTableArgCall}.
     *
     * <p>For example:
     *
     * <pre>
     *     SESSION(TABLE my_table PARTITION BY (b, a), DESCRIPTOR(rowtime), INTERVAL '10' MINUTE)
     * </pre>
     *
     * <p>The original SqlNode tree after syntax parse looks like
     *
     * <pre>
     * SqlBasicCall: SESSION
     * ├─ SqlBasicCall: SET_SEMANTICS_TABLE
     * │  ├─ SqlSelect: "SELECT ... FROM ..."
     * │  ├─ SqlNodeList: (PARTITION KEY)
     * │  │  ├─ SqlIdentifier: "b"
     * │  │  └─ SqlIdentifier: "a"
     * │  └─ SqlNodeList: (ORDER KEY)
     * ├─ SqlBasicCall: DESCRIPTOR(`rowtime`)
     * │  └─ SqlIdentifier: "rowtime"
     * └─ SqlInternalLiteral: INTERVAL '5' MINUTE
     * </pre>
     *
     * <p>Calcite will skip the first operand of SESSION operator, which leads to the following
     * wrong rex call
     *
     * <pre>
     * RexCall: SESSION
     * ├─ RexCall: DESCRIPTOR(`rowtime`)
     * │  └─ RexInputRef: `rowtime`
     * └─ RexLiteral: 300000:INTERVAL MINUTE
     * </pre>
     *
     * <p>Instead, we introduce a customized {@link RexTableArgCall} to preserve properties of the
     * table argument (i.e. partition keys and order keys).
     *
     * <pre>
     * RexCall: SESSION
     * ├─ RexTableArgCall: TABLE
     * │  ├─ InputIndex: 0
     * │  ├─ PartitionKeys: [1, 0]
     * │  └─ OrderKeys: []
     * ├─ RexCall: DESCRIPTOR(`rowtime`)
     * │  └─ RexInputRef: `rowtime`
     * └─ RexLiteral: 300000:INTERVAL MINUTE
     * </pre>
     */
    private RexNode convertTableArgs(SqlRexContext cx, final SqlCall call) {
        checkArgument(
                call.getOperator() instanceof SqlTableFunction,
                "Only table functions can have set semantics arguments.");
        final SqlOperator operator = call.getOperator();
        final RelDataType returnType = cx.getValidator().getValidatedNodeType(call);

        final List<RexNode> rewrittenOperands = new ArrayList<>();
        int tableInputCount = 0;
        for (int pos = 0; pos < call.getOperandList().size(); pos++) {
            final SqlNode operand = call.operand(pos);
            if (operand.getKind() == SqlKind.SET_SEMANTICS_TABLE) {
                final SqlBasicCall setSemanticsCall = (SqlBasicCall) operand;
                final SqlNodeList partitionKeys = setSemanticsCall.operand(1);
                final SqlNodeList orderKeys = setSemanticsCall.operand(2);
                checkArgument(
                        orderKeys.isEmpty(), "Table functions do not support order keys yet.");
                rewrittenOperands.add(
                        new RexTableArgCall(
                                cx.getValidator().getValidatedNodeType(operand),
                                tableInputCount++,
                                getPartitionKeyIndices(cx, partitionKeys),
                                new int[0]));
            } else if (operand.isA(SqlKind.QUERY)) {
                rewrittenOperands.add(
                        new RexTableArgCall(
                                cx.getValidator().getValidatedNodeType(operand),
                                tableInputCount++,
                                new int[0],
                                new int[0]));
            } else {
                rewrittenOperands.add(cx.convertExpression(operand));
            }
        }

        return cx.getRexBuilder().makeCall(returnType, operator, rewrittenOperands);
    }

    private static int[] getPartitionKeyIndices(SqlRexContext cx, SqlNodeList partitions) {
        final int[] result = new int[partitions.size()];
        for (int i = 0; i < partitions.getList().size(); i++) {
            final RexNode expr = cx.convertExpression(partitions.get(i));
            result[i] = parseFieldIdx(expr);
        }
        return result;
    }

    private static int parseFieldIdx(RexNode e) {
        if (SqlKind.INPUT_REF == e.getKind()) {
            final RexInputRef ref = (RexInputRef) e;
            return ref.getIndex();
        }
        // should not happen
        throw new IllegalStateException("Unsupported partition key with type: " + e.getKind());
    }
}
