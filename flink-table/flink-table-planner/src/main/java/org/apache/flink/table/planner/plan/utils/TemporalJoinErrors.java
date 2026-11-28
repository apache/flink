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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Error messages for temporal table joins ({@code FOR SYSTEM_TIME AS OF}).
 *
 * <p>The methods only build messages; they are called right before an exception is thrown.
 */
public final class TemporalJoinErrors {

    /**
     * Names the SQL converter gives to projected expressions, e.g. {@code EXPR$0} or {@code $f3}.
     */
    private static final Pattern GENERATED_COLUMN_NAME = Pattern.compile("^(EXPR\\$|\\$f)\\d+$");

    private static final String TIME_ATTRIBUTE_DEFINITION =
            "A time attribute is a column with a WATERMARK or a PROCTIME() column.";

    private TemporalJoinErrors() {}

    /** The period is not a time attribute. Lists the time attributes of the left side, if any. */
    public static String periodIsNotTimeAttribute(RexNode period, RelDataType leftRowType) {
        final List<String> leftTimeAttributes =
                leftRowType.getFieldList().stream()
                        .filter(field -> FlinkTypeFactory.isTimeIndicatorType(field.getType()))
                        .map(RelDataTypeField::getName)
                        .collect(Collectors.toList());
        if (!leftTimeAttributes.isEmpty()) {
            return String.format(
                    "%s The left side has these time attributes: %s.",
                    periodIsNotTimeAttribute(period), quote(leftTimeAttributes));
        }
        if (period instanceof RexFieldAccess) {
            return periodIsNotTimeAttribute(period)
                    + " If the left side is append-only and the right table has a WATERMARK, a "
                    + "LATERAL SNAPSHOT join can join each row with the current rows of the right "
                    + "table instead.";
        }
        return periodIsNotTimeAttribute(period);
    }

    /** The period is not a time attribute. */
    public static String periodIsNotTimeAttribute(RexNode period) {
        final String regularType = summaryString(period.getType());
        final String actual =
                period instanceof RexFieldAccess
                        ? String.format(
                                "'%s' is a regular %s column",
                                ((RexFieldAccess) period).getField().getName(), regularType)
                        : String.format(
                                "the FOR SYSTEM_TIME AS OF expression is a regular %s value",
                                regularType);
        return String.format(
                "FOR SYSTEM_TIME AS OF must use a time attribute of the left side of the temporal "
                        + "join, but %s. %s",
                actual, TIME_ATTRIBUTE_DEFINITION);
    }

    /** The period is not a plain column of the left side. */
    public static String periodIsNotLeftColumn() {
        return "FOR SYSTEM_TIME AS OF must reference a time attribute column of the left side of "
                + "the temporal join directly, not an expression.";
    }

    /** The period is either not a time attribute or not a plain column of the left side. */
    public static String invalidPeriod(RexNode period) {
        return FlinkTypeFactory.isTimeIndicatorType(period.getType())
                ? periodIsNotLeftColumn()
                : periodIsNotTimeAttribute(period);
    }

    /** The period column cannot be found on the left side. */
    public static String periodNotFoundOnLeftSide(RexNode period) {
        final String column =
                period instanceof RexFieldAccess
                        ? String.format("'%s'", ((RexFieldAccess) period).getField().getName())
                        : "expression";
        return String.format(
                "The FOR SYSTEM_TIME AS OF %s cannot be found on the left side of the temporal "
                        + "join. This is a bug. Please file an issue.",
                column);
    }

    /** The right side of an event-time temporal join has zero or several rowtime attributes. */
    public static String rightSideWithoutSingleRowtime(RelDataType rightRowType) {
        final List<RelDataTypeField> rowtimes = rowtimeFields(rightRowType);
        if (rowtimes.size() < 2) {
            return "The right side of an event-time temporal join must have a column with a "
                    + "WATERMARK, which marks when each row version becomes valid, but it has none. "
                    + "Declare a WATERMARK on the right table, or keep its watermarked column "
                    + "unchanged in the query (a CAST turns it into a regular column).";
        }
        final List<String> names =
                rowtimes.stream().map(RelDataTypeField::getName).collect(Collectors.toList());
        final String regularTypes =
                rowtimes.stream()
                        .map(field -> regularRowtimeType(field.getType()))
                        .distinct()
                        .collect(Collectors.joining(" or "));
        return String.format(
                "The right side of an event-time temporal join must have exactly one column with "
                        + "a WATERMARK, but it has %d: %s. CAST the others to %s.",
                rowtimes.size(), quote(names), regularTypes);
    }

    /** The join condition has no equality between a left and a right column. */
    public static String noEquiJoinCondition() {
        return "A temporal join needs an equality condition between a column of the left side "
                + "and a column of the right side in its ON clause, e.g. ON l.id = r.id.";
    }

    /** The rowtime attributes of both sides of an event-time temporal join differ in type. */
    public static String rowtimeTypeMismatch(RexNode period, RelDataType rightRowType) {
        final String leftName = ((RexFieldAccess) period).getField().getName();
        final String leftType = regularRowtimeType(period.getType());
        final String rightName = rightRowtimeName(rightRowType);
        final String rightType = regularRowtimeType(rowtimeFields(rightRowType).get(0).getType());
        return String.format(
                "The left time attribute '%s' is %s, but the right time attribute '%s' is %s. An "
                        + "event-time temporal join needs both of the same type. Change the "
                        + "schema of one table so that its WATERMARK column has the matching "
                        + "type. Alternatively, declare the WATERMARK on a computed column of the "
                        + "matching type, e.g. CAST(%s AS %s) in the right table. A CAST in the "
                        + "query does not work, because it turns the time attribute into a "
                        + "regular column.",
                leftName, leftType, rightName, rightType, rightName, leftType);
    }

    /** No primary key can be derived for the right side. */
    public static String rightSideWithoutPrimaryKey(Join join, List<RexNode> rightJoinKeys) {
        if (!TemporalTableJoinUtil.isEventTimeTemporalJoin(join.getCondition())) {
            return "Processing-time temporal joins on the right table are not supported. If the "
                    + "left side is append-only and the right table has a WATERMARK, use a "
                    + "LATERAL SNAPSHOT join to join each row with the current rows of the right "
                    + "table.";
        }
        final List<String> keyNames =
                rightJoinKeys.stream()
                        .map(key -> rightFieldName(join, ((RexInputRef) key).getIndex()))
                        .collect(Collectors.toList());
        final String keys = keyList(keyNames);
        return String.format(
                "The right side of an event-time temporal join must have a primary key on the join "
                        + "key, but none can be derived. Declare PRIMARY KEY (%s) NOT ENFORCED on "
                        + "the right table, or deduplicate it to the last row per key with "
                        + "ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s DESC) = 1. To join "
                        + "each row with the current rows of the right table instead of the "
                        + "version valid at the row's time, use a LATERAL SNAPSHOT join, which "
                        + "needs no primary key but an append-only left side.",
                keys, keys, rightRowtimeName(join.getRight().getRowType()));
    }

    /**
     * The join condition does not cover the full primary key of the right side.
     *
     * @param primaryKey indices of the primary key in the row type of the join
     * @param joinKey indices of the right join key in the row type of the join
     */
    public static String primaryKeyNotCovered(
            Join join, List<Integer> primaryKey, List<Integer> joinKey) {
        final List<String> primaryKeyNames =
                primaryKey.stream()
                        .map(index -> rightFieldName(join, index))
                        .collect(Collectors.toList());
        final List<String> missingNames =
                primaryKey.stream()
                        .filter(index -> !joinKey.contains(index))
                        .map(index -> rightFieldName(join, index))
                        .collect(Collectors.toList());
        final String temporalJoin =
                TemporalTableJoinUtil.isEventTimeTemporalJoin(join.getCondition())
                        ? "An event-time temporal join"
                        : "A temporal join";
        final String fix =
                missingNames.size() == 1 ? "an equality condition" : "equality conditions";
        return String.format(
                "%s must join on the full primary key (%s) of its right side, but the join "
                        + "condition does not use %s. Add %s on %s. To join on columns other than "
                        + "the primary key, use a LATERAL SNAPSHOT join, which joins with all "
                        + "current rows that match and needs an append-only left side.",
                temporalJoin,
                keyList(primaryKeyNames),
                quote(missingNames),
                fix,
                quote(missingNames));
    }

    private static String rightFieldName(Join join, int joinFieldIndex) {
        final int rightIndex = joinFieldIndex - join.getLeft().getRowType().getFieldCount();
        return join.getRight().getRowType().getFieldNames().get(rightIndex);
    }

    private static String rightRowtimeName(RelDataType rightRowType) {
        final List<RelDataTypeField> rowtimes = rowtimeFields(rightRowType);
        if (rowtimes.size() != 1 || isGenerated(rowtimes.get(0).getName())) {
            return "<rowtime>";
        }
        return rowtimes.get(0).getName();
    }

    private static List<RelDataTypeField> rowtimeFields(RelDataType rowType) {
        return rowType.getFieldList().stream()
                .filter(field -> FlinkTypeFactory.isRowtimeIndicatorType(field.getType()))
                .collect(Collectors.toList());
    }

    private static String keyList(List<String> names) {
        if (names.stream().anyMatch(TemporalJoinErrors::isGenerated)) {
            return "<key>";
        }
        return String.join(", ", names);
    }

    private static boolean isGenerated(String name) {
        return GENERATED_COLUMN_NAME.matcher(name).matches();
    }

    private static String regularRowtimeType(RelDataType rowtimeType) {
        return summaryString(((TimeIndicatorRelDataType) rowtimeType).getOriginalType());
    }

    private static String summaryString(RelDataType type) {
        return FlinkTypeFactory.toLogicalType(type).copy(true).asSummaryString();
    }

    private static String quote(List<String> names) {
        return names.stream().map(name -> "'" + name + "'").collect(Collectors.joining(", "));
    }
}
