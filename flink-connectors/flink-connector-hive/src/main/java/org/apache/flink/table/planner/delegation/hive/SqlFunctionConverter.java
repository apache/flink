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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserBetween;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserSqlFunctionConverter;
import org.apache.flink.table.planner.functions.sql.FlinkSqlTimestampFunction;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/** A RexShuttle that converts Hive function calls so that Flink recognizes them. */
public class SqlFunctionConverter extends RexShuttle {

    // need reflection to get some fields from RexWindow
    private static final Field REX_WINDOW_PART_KEYS;
    private static final Field REX_WINDOW_ORDER_KEYS;

    static {
        try {
            REX_WINDOW_PART_KEYS = RexWindow.class.getDeclaredField("partitionKeys");
            REX_WINDOW_ORDER_KEYS = RexWindow.class.getDeclaredField("orderKeys");
        } catch (Exception e) {
            throw new FlinkHiveException("Failed to init RexWindow fields", e);
        }
    }

    protected final RelOptCluster cluster;
    protected final RexBuilder builder;
    private final SqlOperatorTable opTable;
    private final SqlNameMatcher nameMatcher;

    public SqlFunctionConverter(
            RelOptCluster cluster, SqlOperatorTable opTable, SqlNameMatcher nameMatcher) {
        this.cluster = cluster;
        this.builder = cluster.getRexBuilder();
        this.opTable = opTable;
        this.nameMatcher = nameMatcher;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();
        SqlOperator convertedOp = convertOperator(operator);
        final boolean[] update = null;
        if (convertedOp instanceof SqlCastFunction) {
            RelDataType type = call.getType();
            return builder.makeCall(type, convertedOp, visitList(operands, update));
        } else {
            if (convertedOp instanceof FlinkSqlTimestampFunction) {
                // flink's current_timestamp has different type from hive's, convert it to a literal
                Timestamp currentTS =
                        ((HiveParser.HiveParserSessionState) SessionState.get())
                                .getHiveParserCurrentTS();
                HiveShim hiveShim = HiveParserUtils.getSessionHiveShim();
                try {
                    return HiveParserRexNodeConverter.convertConstant(
                            new ExprNodeConstantDesc(hiveShim.toHiveTimestamp(currentTS)), cluster);
                } catch (SemanticException e) {
                    throw new FlinkHiveException(e);
                }
            }
            return builder.makeCall(convertedOp, visitList(operands, update));
        }
    }

    @Override
    public RexNode visitOver(RexOver over) {
        SqlOperator operator = convertOperator(over.getAggOperator());
        Preconditions.checkArgument(
                operator instanceof SqlAggFunction,
                "Expect converted operator to be an agg function, but got " + operator.toString());
        SqlAggFunction convertedAgg = (SqlAggFunction) operator;
        RexWindow window = over.getWindow();
        // let's not rely on the type of the RexOver created by Hive since it can be different from
        // what Flink expects
        RelDataType inferredType = builder.makeCall(convertedAgg, over.getOperands()).getType();
        // Hive may add literals to partition keys, remove them
        List<RexNode> partitionKeys = new ArrayList<>();
        for (RexNode hivePartitionKey : getPartKeys(window)) {
            if (!(hivePartitionKey instanceof RexLiteral)) {
                partitionKeys.add(hivePartitionKey);
            }
        }
        List<RexFieldCollation> convertedOrderKeys = new ArrayList<>(getOrderKeys(window).size());
        for (RexFieldCollation orderKey : getOrderKeys(window)) {
            convertedOrderKeys.add(
                    new RexFieldCollation(orderKey.getKey().accept(this), orderKey.getValue()));
        }
        final boolean[] update = null;
        return HiveParserUtils.makeOver(
                builder,
                inferredType,
                convertedAgg,
                visitList(over.getOperands(), update),
                visitList(partitionKeys, update),
                convertedOrderKeys,
                window.getLowerBound(),
                window.getUpperBound(),
                window.isRows(),
                true,
                false,
                false,
                false /*these parameters are kept in line with Hive*/);
    }

    public SqlOperator convertOperator(SqlOperator operator) {
        if (operator instanceof SqlFunction) {
            operator = convertOperator(operator, ((SqlFunction) operator).getFunctionType());
        } else if (operator instanceof HiveParserIN || operator instanceof HiveParserBetween) {
            operator = convertOperator(operator, SqlFunctionCategory.USER_DEFINED_FUNCTION);
        }
        return operator;
    }

    public boolean hasOverloadedOp(SqlOperator operator, SqlFunctionCategory functionType) {
        return operator != convertOperator(operator, functionType);
    }

    SqlOperator convertOperator(SqlOperator operator, SqlFunctionCategory functionType) {
        List<SqlOperator> overloads = new ArrayList<>();
        opTable.lookupOperatorOverloads(
                operator.getNameAsId(), functionType, SqlSyntax.FUNCTION, overloads, nameMatcher);
        if (!overloads.isEmpty()) {
            return overloads.get(0);
        }
        return operator;
    }

    boolean isHiveCalciteSqlFn(SqlOperator operator) {
        return operator instanceof HiveParserSqlFunctionConverter.CalciteSqlFn;
    }

    private List<RexNode> getPartKeys(RexWindow window) {
        try {
            return (List<RexNode>) REX_WINDOW_PART_KEYS.get(window);
        } catch (IllegalAccessException e) {
            throw new FlinkHiveException("Failed to get partitionKeys from RexWindow", e);
        }
    }

    private List<RexFieldCollation> getOrderKeys(RexWindow window) {
        try {
            return (List<RexFieldCollation>) REX_WINDOW_ORDER_KEYS.get(window);
        } catch (IllegalAccessException e) {
            throw new FlinkHiveException("Failed to get orderKeys from RexWindow", e);
        }
    }
}
