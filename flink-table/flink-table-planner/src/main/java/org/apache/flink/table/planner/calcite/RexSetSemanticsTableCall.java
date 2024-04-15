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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A special {@link RexCall} that is used to represent a table function with set semantics. See more
 * details in {@link FlinkConvertletTable#convertSetSemanticsWindowTableFunction}.
 */
public class RexSetSemanticsTableCall extends RexCall {

    private final int[] partitionKeys;

    private final int[] orderKeys;

    public RexSetSemanticsTableCall(
            RelDataType type,
            SqlOperator operator,
            List<? extends RexNode> operands,
            int[] partitionKeys,
            int[] orderKeys) {
        super(type, operator, operands);
        this.partitionKeys = partitionKeys;
        this.orderKeys = orderKeys;
    }

    public int[] getPartitionKeys() {
        return partitionKeys;
    }

    public int[] getOrderKeys() {
        return orderKeys;
    }

    @Override
    protected String computeDigest(boolean withType) {
        if ((operands.isEmpty()) && (op.getSyntax() == SqlSyntax.FUNCTION_ID)) {
            return super.computeDigest(withType);
        }
        final StringBuilder sb = new StringBuilder(op.getName());
        sb.append("(");
        appendKeys(partitionKeys, "PARTITION BY", sb);
        appendKeys(orderKeys, "ORDER BY", sb);
        appendOperands(sb);
        sb.append(")");
        if (withType) {
            sb.append(":");

            // NOTE jvs 16-Jan-2005:  for digests, it is very important
            // to use the full type string.
            sb.append(type.getFullTypeString());
        }
        return sb.toString();
    }

    private void appendKeys(int[] keys, String prefix, StringBuilder sb) {
        if (keys.length == 0) {
            return;
        }
        sb.append(
                Arrays.stream(keys)
                        .mapToObj(key -> "$" + key)
                        .collect(Collectors.joining(", ", prefix + "(", "), ")));
    }

    public RexSetSemanticsTableCall copy(
            List<? extends RexNode> newOperands, int[] newPartitionKeys, int[] newOrderKeys) {
        return new RexSetSemanticsTableCall(type, op, newOperands, newPartitionKeys, newOrderKeys);
    }

    @Override
    public RexSetSemanticsTableCall clone(RelDataType type, List<RexNode> operands) {
        return new RexSetSemanticsTableCall(
                type, getOperator(), operands, partitionKeys, orderKeys);
    }
}
