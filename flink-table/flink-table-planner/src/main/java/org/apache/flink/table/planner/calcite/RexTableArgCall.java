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

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.TableSemantics.SortDirection;
import org.apache.flink.table.planner.functions.sql.SqlTableArgOperator;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.functions.TableSemantics.SortDirection.ASC_NULLS_FIRST;
import static org.apache.flink.table.functions.TableSemantics.SortDirection.ASC_NULLS_LAST;
import static org.apache.flink.table.functions.TableSemantics.SortDirection.DESC_NULLS_FIRST;
import static org.apache.flink.table.functions.TableSemantics.SortDirection.DESC_NULLS_LAST;

/**
 * A special {@link RexCall} that represents a table argument in a signature of {@link
 * StaticArgument}s. The table arguments describe a {@link StaticArgumentTrait#SET_SEMANTIC_TABLE}
 * or {@link StaticArgumentTrait#ROW_SEMANTIC_TABLE}.
 *
 * @see FlinkConvertletTable
 */
public class RexTableArgCall extends RexCall {

    /**
     * Sort order for ORDER BY columns.
     *
     * <p>Note: Be careful when changing this class. It ends up in {@link CompiledPlan}. This is
     * also the reason why it has been separated from API-facing {@link SortDirection}.
     */
    public enum SortOrder {
        ASC_NULLS_FIRST,
        ASC_NULLS_LAST,
        DESC_NULLS_FIRST,
        DESC_NULLS_LAST;

        public static SortOrder fromSortDirection(SortDirection direction) {
            switch (direction) {
                case ASC_NULLS_FIRST:
                    return ASC_NULLS_FIRST;
                case ASC_NULLS_LAST:
                    return ASC_NULLS_LAST;
                case DESC_NULLS_FIRST:
                    return DESC_NULLS_FIRST;
                case DESC_NULLS_LAST:
                    return DESC_NULLS_LAST;
                default:
                    throw new IllegalArgumentException("Unknown sort direction: " + direction);
            }
        }
    }

    private final int inputIndex;
    private final int[] partitionKeys;
    private final int[] orderKeys;
    private final SortOrder[] sortOrder;

    public RexTableArgCall(
            RelDataType type,
            int inputIndex,
            int[] partitionKeys,
            int[] orderKeys,
            SortOrder[] order) {
        super(type, SqlTableArgOperator.INSTANCE, List.of());
        this.inputIndex = inputIndex;
        this.partitionKeys = partitionKeys;
        this.orderKeys = orderKeys;
        this.sortOrder = order;
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

    public SortOrder[] getSortOrder() {
        return sortOrder;
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
        formatPartitionKeys(sb, partitionKeys);
        formatOrderKeys(sb);
        return sb.toString();
    }

    private void formatPartitionKeys(StringBuilder sb, int[] keys) {
        if (keys.length == 0) {
            return;
        }
        sb.append(
                Arrays.stream(keys)
                        .mapToObj(key -> "$" + key)
                        .collect(Collectors.joining(", ", " PARTITION BY" + "(", ")")));
    }

    private void formatOrderKeys(StringBuilder sb) {
        if (orderKeys.length == 0) {
            return;
        }
        sb.append(" ORDER BY(");
        for (int i = 0; i < orderKeys.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("$").append(orderKeys[i]);
            if (i < sortOrder.length) {
                sb.append(" ").append(sortOrder[i]);
            }
        }
        sb.append(")");
    }

    @Override
    public RexCall clone(RelDataType type, List<RexNode> operands) {
        return new RexTableArgCall(type, inputIndex, partitionKeys, orderKeys, sortOrder);
    }

    public RexTableArgCall copy(
            RelDataType type, int[] partitionKeys, int[] orderKeys, SortOrder[] order) {
        return new RexTableArgCall(type, inputIndex, partitionKeys, orderKeys, order);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final RexTableArgCall that = (RexTableArgCall) o;
        return inputIndex == that.inputIndex
                && Arrays.equals(partitionKeys, that.partitionKeys)
                && Arrays.equals(orderKeys, that.orderKeys)
                && Arrays.equals(sortOrder, that.sortOrder);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), inputIndex);
        result = 31 * result + Arrays.hashCode(partitionKeys);
        result = 31 * result + Arrays.hashCode(orderKeys);
        result = 31 * result + Arrays.hashCode(sortOrder);
        return result;
    }

    // --------------------------------------------------------------------------------------------
    // Utility methods for ORDER BY extraction
    // --------------------------------------------------------------------------------------------

    /** Result holder for ORDER BY information. */
    public static class OrderByInfo {
        public final int[] columns;
        public final SortOrder[] order;

        public OrderByInfo(int[] columns, SortOrder[] order) {
            this.columns = columns;
            this.order = order;
        }
    }

    /**
     * Extracts ORDER BY information from a SQL node list.
     *
     * @param fieldNames the list of field names from the table type
     * @param orderKeys the SQL node list containing ORDER BY clauses (may be null)
     * @return OrderByInfo containing column indices and sort directions
     */
    public static OrderByInfo extractOrderByInfo(
            List<String> fieldNames, @Nullable SqlNodeList orderKeys) {
        if (orderKeys == null || orderKeys.isEmpty()) {
            return new OrderByInfo(new int[0], new SortOrder[0]);
        }

        final int[] columns = new int[orderKeys.size()];
        final SortOrder[] order = new SortOrder[orderKeys.size()];

        for (int i = 0; i < orderKeys.size(); i++) {
            SqlNode orderItem = orderKeys.get(i);
            boolean descending = false;
            Boolean nullsFirst = null;
            SqlNode columnNode = orderItem;

            // Unwrap NULLS FIRST/LAST and DESCENDING operators
            while (columnNode instanceof SqlCall) {
                final SqlCall call = (SqlCall) columnNode;
                final SqlKind kind = call.getKind();
                if (kind == SqlKind.DESCENDING) {
                    descending = true;
                    columnNode = call.operand(0);
                } else if (kind == SqlKind.NULLS_FIRST) {
                    nullsFirst = true;
                    columnNode = call.operand(0);
                } else if (kind == SqlKind.NULLS_LAST) {
                    nullsFirst = false;
                    columnNode = call.operand(0);
                } else {
                    break;
                }
            }

            // Determine the sort direction with explicit null handling
            final SortOrder direction = getSortOrder(descending, nullsFirst);

            if (!(columnNode instanceof SqlIdentifier)) {
                throw new ValidationException("Column reference expected for ORDER BY clause.");
            }
            final String columnName = ((SqlIdentifier) columnNode).getSimple();
            final int pos = fieldNames.indexOf(columnName);
            if (pos < 0) {
                throw new ValidationException(
                        String.format(
                                "Invalid column '%s' for ORDER BY clause. "
                                        + "Available columns are: %s",
                                columnName, fieldNames));
            }
            columns[i] = pos;
            order[i] = direction;
        }

        return new OrderByInfo(columns, order);
    }

    private static SortOrder getSortOrder(boolean descending, Boolean nullsFirst) {
        final SortOrder direction;
        if (descending) {
            // Default for DESC is NULLS FIRST
            direction =
                    (nullsFirst == null || nullsFirst)
                            ? SortOrder.DESC_NULLS_FIRST
                            : SortOrder.DESC_NULLS_LAST;
        } else {
            // Default for ASC is NULLS LAST
            direction =
                    (nullsFirst == null || !nullsFirst)
                            ? SortOrder.ASC_NULLS_LAST
                            : SortOrder.ASC_NULLS_FIRST;
        }
        return direction;
    }

    /**
     * Converts an array of {@link SortOrder} to an array of {@link SortDirection}.
     *
     * @param orders the array of Order values to convert
     * @return the corresponding array of SortDirection values
     */
    public static SortDirection[] toSortDirections(SortOrder[] orders) {
        final SortDirection[] directions = new SortDirection[orders.length];
        for (int i = 0; i < orders.length; i++) {
            directions[i] = SortDirection.valueOf(orders[i].name());
        }
        return directions;
    }
}
