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

import org.apache.flink.table.planner.functions.sql.SqlTableArgOperator;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A special {@link RexCall} that represents a table argument in a signature of {@link
 * StaticArgument}s. The table arguments describe a {@link StaticArgumentTrait#TABLE_AS_SET} or
 * {@link StaticArgumentTrait#TABLE_AS_ROW}.
 *
 * @see FlinkConvertletTable
 */
public class RexTableArgCall extends RexCall {

    private final int inputIndex;
    private final int[] partitionKeys;
    private final int[] orderKeys;

    public RexTableArgCall(RelDataType type, int inputIndex, int[] partitionKeys, int[] orderKeys) {
        super(type, SqlTableArgOperator.INSTANCE, List.of());
        this.inputIndex = inputIndex;
        this.partitionKeys = partitionKeys;
        this.orderKeys = orderKeys;
    }

    public int getInputIndex() {
        return inputIndex;
    }

    public int[] getPartitionKeys() {
        return partitionKeys;
    }

    public int[] getOrderKeys() {
        return orderKeys;
    }

    @Override
    protected String computeDigest(boolean withType) {
        final StringBuilder sb = new StringBuilder(op.getName());
        sb.append("(");
        sb.append("#");
        sb.append(inputIndex);
        sb.append(")");
        if (withType) {
            sb.append(":");
            sb.append(type.getFullTypeString());
        }
        formatKeys(sb, partitionKeys, " PARTITION BY");
        formatKeys(sb, orderKeys, " ORDER BY");
        return sb.toString();
    }

    private void formatKeys(StringBuilder sb, int[] keys, String prefix) {
        if (keys.length == 0) {
            return;
        }
        sb.append(
                Arrays.stream(keys)
                        .mapToObj(key -> "$" + key)
                        .collect(Collectors.joining(", ", prefix + "(", ")")));
    }

    @Override
    public RexCall clone(RelDataType type, List<RexNode> operands) {
        return new RexTableArgCall(type, inputIndex, partitionKeys, orderKeys);
    }

    public RexTableArgCall copy(RelDataType type, int[] partitionKeys, int[] orderKeys) {
        return new RexTableArgCall(type, inputIndex, partitionKeys, orderKeys);
    }
}
