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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.sql.SqlSessionTableFunction;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

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

        if (isSetSemanticsWindowTableFunction(call)) {
            return this::convertSetSemanticsWindowTableFunction;
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

    private boolean isSetSemanticsWindowTableFunction(SqlCall call) {
        if (!(call.getOperator() instanceof SqlWindowTableFunction)) {
            return false;
        }
        List<SqlNode> operands = call.getOperandList();
        return !operands.isEmpty() && operands.get(0).getKind() == SqlKind.SET_SEMANTICS_TABLE;
    }

    /**
     * The partition keys and order keys in ptf will be extracted to {@link
     * RexSetSemanticsTableCall}.
     *
     * <p>Take "SESSION(TABLE my_table PARTITION BY (b, a), DESCRIPTOR(rowtime), INTERVAL '10'
     * MINUTE)" as an example.
     *
     * <pre>
     *     The original SqlCall:
     *
     *     SqlBasicCall: SESSION
     *     +- SqlBasicCall: SET_SEMANTICS_TABLE
     *       +- SqlSelect: "SELECT `my_table`.`a`, `my_table`.`b`, `my_table`.`c`, ... FROM ..."
     *       +- SqlNodeList: (PARTITION KEY)
     *          +- SqlIdentifier: "b"
     *          +- SqlIdentifier: "a"
     *       +- SqlNodeList: (ORDER KEY)
     *     +- SqlBasicCall: DESCRIPTOR(`rowtime`)
     *       +- SqlIdentifier: "rowtime"
     *     +- SqlInternalLiteral: INTERVAL '5' MINUTE
     * </pre>
     *
     * <pre>
     *     After the process in Calcite:
     *
     *     RexCall: SESSION
     *     +- RexCall: DESCRIPTOR(`rowtime`)
     *       +- RexFieldAccess: `rowtime`
     *     +- RexLiteral: 300000:INTERVAL MINUTE
     * </pre>
     *
     * <pre>
     *     After we modified:
     *
     *     RexSetSemanticsTableCall: SESSION
     *     +- PartitionKeys: [0, 1]
     *     +- OrderKeys: []
     *     +- RexCall: DESCRIPTOR(`rowtime`)
     *       +- RexFieldAccess: `rowtime`
     *     +- RexLiteral: 300000:INTERVAL MINUTE
     * </pre>
     */
    private RexNode convertSetSemanticsWindowTableFunction(SqlRexContext cx, final SqlCall call) {
        checkState(
                call.getOperator() instanceof SqlSessionTableFunction,
                "Currently only support SESSION table function in Set Semantics PTF.");
        SqlSessionTableFunction fun = (SqlSessionTableFunction) call.getOperator();

        List<SqlNode> operands = call.getOperandList();

        SqlBasicCall setSemanticsPTFCall = (SqlBasicCall) operands.get(0);
        SqlNodeList partitionKeys = setSemanticsPTFCall.operand(1);
        SqlNodeList orderKeys = setSemanticsPTFCall.operand(2);
        //        RexNode subQuery = cx.convertExpression(setSemanticsPTFCall);
        checkState(orderKeys.isEmpty(), "SESSION table function does not support order keys.");
        RexCall resolvedCall =
                (RexCall) StandardConvertletTable.INSTANCE.convertWindowFunction(cx, fun, call);
        ImmutableBitSet partitionKeyRefs = getPartitionKeyIndices(cx, partitionKeys);

        // attach the partition keys and order keys on the custom rex call
        resolvedCall =
                new RexSetSemanticsTableCall(
                        resolvedCall.getType(),
                        resolvedCall.getOperator(),
                        resolvedCall.getOperands(),
                        partitionKeyRefs,
                        ImmutableBitSet.builder().build());
        return resolvedCall;
    }

    private ImmutableBitSet getPartitionKeyIndices(SqlRexContext cx, SqlNodeList partitions) {
        final ImmutableBitSet.Builder result = ImmutableBitSet.builder();

        for (SqlNode partition : partitions) {
            RexNode expr = cx.convertExpression(partition);
            result.set(parseFieldIdx(expr));
        }
        return result.build();
    }

    private static int parseFieldIdx(RexNode e) {
        switch (e.getKind()) {
            case FIELD_ACCESS:
                final RexFieldAccess f = (RexFieldAccess) e;
                return f.getField().getIndex();
            case INPUT_REF:
                final RexInputRef ref = (RexInputRef) e;
                return ref.getIndex();
            default:
                // should not happen
                throw new TableException("Unsupported partition key with type: " + e.getKind());
        }
    }
}
