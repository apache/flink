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

import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;

import javax.annotation.Nonnull;

/**
 * Utility for temporal table join which will gradually replace the scala class {@link
 * TemporalJoinUtil}.
 */
public class TemporalTableJoinUtil {

    /**
     * Check if the given join condition is an initial temporal join condition or a rewrote join
     * condition on event time.
     */
    public static boolean isEventTimeTemporalJoin(@Nonnull RexNode joinCondition) {
        RexVisitor<Void> temporalConditionFinder =
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCall(RexCall call) {
                        if ((call.getOperator()
                                                == TemporalJoinUtil
                                                        .INITIAL_TEMPORAL_JOIN_CONDITION()
                                        && TemporalJoinUtil.isInitialRowTimeTemporalTableJoin(call))
                                || isRowTimeTemporalTableJoinCondition(call)) {
                            // has initial temporal join condition or
                            throw new Util.FoundOne(call);
                        }
                        return super.visitCall(call);
                    }
                };
        try {
            joinCondition.accept(temporalConditionFinder);
        } catch (Util.FoundOne found) {
            return true;
        }
        return false;
    }

    /** Check if the given rexCall is a rewrote join condition on event time. */
    public static boolean isRowTimeTemporalTableJoinCondition(RexCall call) {
        // (LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, LEFT_KEY, RIGHT_KEY, PRIMARY_KEY)
        return call.getOperator() == TemporalJoinUtil.TEMPORAL_JOIN_CONDITION()
                && call.operands.size() == 5;
    }

    public static boolean isTemporalJoinSupportPeriod(RexNode period) {
        // it should be left table's field and is a time attribute
        if (period instanceof RexFieldAccess) {
            RexFieldAccess rexFieldAccess = (RexFieldAccess) period;
            return rexFieldAccess.getType() instanceof TimeIndicatorRelDataType
                    && rexFieldAccess.getReferenceExpr() instanceof RexCorrelVariable;
        }
        return false;
    }
}
