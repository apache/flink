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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.ArrayList;
import java.util.List;

/** A RexShuttle that converts RexCall for table functions. */
public class TableFunctionConverter extends SqlFunctionConverter {

    // LHS RelNode
    private final RelNode leftRel;

    public TableFunctionConverter(
            RelOptCluster cluster,
            RelNode leftRel,
            SqlOperatorTable opTable,
            SqlNameMatcher nameMatcher) {
        super(cluster, opTable, nameMatcher);
        this.leftRel = leftRel;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        SqlOperator operator = call.getOperator();
        if (isHiveCalciteSqlFn(operator)) {
            // explicitly use USER_DEFINED_TABLE_FUNCTION since Hive can set USER_DEFINED_FUNCTION
            // for UDTF
            SqlOperator convertedOperator =
                    convertOperator(operator, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
            List<RexNode> convertedOperands = new ArrayList<>();
            RelDataTypeFactory.Builder dataTypeBuilder = cluster.getTypeFactory().builder();
            dataTypeBuilder.addAll(leftRel.getRowType().getFieldList());
            dataTypeBuilder.addAll(call.getType().getFieldList());
            RelDataType correlType = dataTypeBuilder.uniquify().build();
            InputRefConverter inputRefConverter = new InputRefConverter(correlType, cluster);
            for (RexNode operand : call.getOperands()) {
                convertedOperands.add(operand.accept(inputRefConverter));
            }
            // create RexCall
            return builder.makeCall(call.getType(), convertedOperator, convertedOperands);
        }
        return super.visitCall(call);
    }

    /** A converter to convert RexInputRef to RexFieldAccess. */
    private static class InputRefConverter extends RexShuttle {
        private final RelDataType correlType;
        private final RelOptCluster cluster;
        private final RexBuilder builder;

        private InputRefConverter(RelDataType correlType, RelOptCluster cluster) {
            this.correlType = correlType;
            this.cluster = cluster;
            this.builder = cluster.getRexBuilder();
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            // create RexCorrelVariable
            CorrelationId correlId = cluster.createCorrel();
            RexNode correlRex = builder.makeCorrel(correlType, correlId);
            // create RexFieldAccess
            return builder.makeFieldAccess(correlRex, inputRef.getIndex());
        }
    }
}
