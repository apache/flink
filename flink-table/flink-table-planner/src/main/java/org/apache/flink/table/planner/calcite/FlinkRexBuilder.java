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

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A slim extension over a {@link RexBuilder}. See the overridden methods for more explanation. */
public final class FlinkRexBuilder extends RexBuilder {
    public FlinkRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    /**
     * Compared to the original method we adjust the nullability of the nested column based on the
     * nullability of the enclosing type.
     *
     * <p>If the fields type is NOT NULL, but the enclosing ROW is nullable we still can produce
     * nulls.
     */
    @Override
    public RexNode makeFieldAccess(RexNode expr, String fieldName, boolean caseSensitive) {
        RexNode field = super.makeFieldAccess(expr, fieldName, caseSensitive);
        if (expr.getType().isNullable() && !field.getType().isNullable()) {
            return makeCast(
                    typeFactory.createTypeWithNullability(field.getType(), true), field, true);
        }

        return field;
    }

    /**
     * Compared to the original method we adjust the nullability of the nested column based on the
     * nullability of the enclosing type.
     *
     * <p>If the fields type is NOT NULL, but the enclosing ROW is nullable we still can produce
     * nulls.
     */
    @Override
    public RexNode makeFieldAccess(RexNode expr, int i) {
        RexNode field = super.makeFieldAccess(expr, i);
        if (expr.getType().isNullable() && !field.getType().isNullable()) {
            return makeCast(
                    typeFactory.createTypeWithNullability(field.getType(), true), field, true);
        }

        return field;
    }

    /**
     * Creates a literal of the default value for the given type.
     *
     * <p>This value is:
     *
     * <ul>
     *   <li>0 for numeric types;
     *   <li>FALSE for BOOLEAN;
     *   <li>The epoch for TIMESTAMP and DATE;
     *   <li>Midnight for TIME;
     *   <li>The empty string for string types (CHAR, BINARY, VARCHAR, VARBINARY).
     * </ul>
     *
     * <p>Uses '1970-01-01 00:00:00'(epoch 0 second) as zero value for TIMESTAMP_LTZ, the zero value
     * '0000-00-00 00:00:00' in Calcite is an invalid time whose month and day is invalid, we
     * workaround here. Stop overriding once CALCITE-4555 fixed.
     *
     * @param type Type
     * @return Simple literal, or cast simple literal
     */
    @Override
    public RexNode makeZeroLiteral(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return makeLiteral(new TimestampString(1970, 1, 1, 0, 0, 0), type, false);
            default:
                return super.makeZeroLiteral(type);
        }
    }

    /**
     * Convert the conditions into the {@code IN} and fix [CALCITE-4888]: Unexpected {@link RexNode}
     * when call {@link RelBuilder#in} to create an {@code IN} predicate with a list of varchar
     * literals which have different length in {@link RexBuilder#makeIn}.
     *
     * <p>The bug is because the origin implementation doesn't take {@link
     * FlinkTypeSystem#shouldConvertRaggedUnionTypesToVarying} into consideration. When this is
     * true, the behaviour should not padding char. Please see
     * https://issues.apache.org/jira/browse/CALCITE-4590 and
     * https://issues.apache.org/jira/browse/CALCITE-2321. Please refer to {@code
     * org.apache.calcite.rex.RexSimplify.RexSargBuilder#getType} for the correct behaviour.
     *
     * <p>Once CALCITE-4888 is fixed, this method (and related methods) should be removed.
     */
    @Override
    @SuppressWarnings("unchecked")
    public RexNode makeIn(RexNode arg, List<? extends RexNode> ranges) {
        if (areAssignable(arg, ranges)) {
            // Fix calcite doesn't check literal whether is NULL here
            List<RexNode> rangeWithoutNull = new ArrayList<>();
            boolean containsNull = false;
            for (RexNode node : ranges) {
                if (isNull(node)) {
                    containsNull = true;
                } else {
                    rangeWithoutNull.add(node);
                }
            }
            final Sarg sarg = toSarg(Comparable.class, rangeWithoutNull, containsNull);
            if (sarg != null) {
                List<RelDataType> distinctTypes =
                        Util.distinctList(
                                ranges.stream().map(RexNode::getType).collect(Collectors.toList()));
                RelDataType commonType = getTypeFactory().leastRestrictive(distinctTypes);
                return makeCall(
                        SqlStdOperatorTable.SEARCH,
                        arg,
                        makeSearchArgumentLiteral(sarg, commonType));
            }
        }
        return RexUtil.composeDisjunction(
                this,
                ranges.stream()
                        .map(r -> makeCall(SqlStdOperatorTable.EQUALS, arg, r))
                        .collect(Util.toImmutableList()));
    }

    private boolean isNull(RexNode node) {
        if (node instanceof RexLiteral) {
            return ((RexLiteral) node).isNull();
        }
        return false;
    }

    /** Copied from the {@link RexBuilder} to fix the {@link RexBuilder#makeIn}. */
    private boolean areAssignable(RexNode arg, List<? extends RexNode> bounds) {
        for (RexNode bound : bounds) {
            if (!SqlTypeUtil.inSameFamily(arg.getType(), bound.getType())
                    && !(arg.getType().isStruct() && bound.getType().isStruct())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Converts a list of expressions to a search argument, or returns null if not possible.
     *
     * <p>Copied from the {@link RexBuilder} to fix the {@link RexBuilder#makeIn}.
     */
    @SuppressWarnings("UnstableApiUsage")
    private static <C extends Comparable<C>> Sarg<C> toSarg(
            Class<C> clazz, List<? extends RexNode> ranges, boolean containsNull) {
        if (ranges.isEmpty()) {
            // Cannot convert an empty list to a Sarg (by this interface, at least)
            // because we use the type of the first element.
            return null;
        }
        final com.google.common.collect.RangeSet<C> rangeSet =
                com.google.common.collect.TreeRangeSet.create();
        for (RexNode range : ranges) {
            final C value = toComparable(clazz, range);
            if (value == null) {
                return null;
            }
            rangeSet.add(com.google.common.collect.Range.singleton(value));
        }
        return Sarg.of(containsNull, rangeSet);
    }

    /** Copied from the {@link RexBuilder} to fix the {@link RexBuilder#makeIn}. */
    @SuppressWarnings("rawtypes")
    private static <C extends Comparable<C>> C toComparable(Class<C> clazz, RexNode point) {
        switch (point.getKind()) {
            case LITERAL:
                final RexLiteral literal = (RexLiteral) point;
                return literal.getValueAs(clazz);

            case ROW:
                final RexCall call = (RexCall) point;
                final ImmutableList.Builder<Comparable> b = ImmutableList.builder();
                for (RexNode operand : call.operands) {
                    //noinspection unchecked
                    final Comparable value = toComparable(Comparable.class, operand);
                    if (value == null) {
                        return null; // not a constant value
                    }
                    b.add(value);
                }
                return clazz.cast(FlatLists.ofComparable(b.build()));

            default:
                return null; // not a constant value
        }
    }
}
