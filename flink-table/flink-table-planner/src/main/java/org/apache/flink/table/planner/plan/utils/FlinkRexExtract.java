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

import org.apache.flink.annotation.Internal;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Extracts sub-condition according AND/OR structure of condition. We must be able to extract at
 * least one sub-condition from each of the arms of the OR, else the result will be true literal. It
 * is assumed that condition has already been simplified (e.g. by FlinkRexUtil.simplify) in such a
 * way that there is no NOT before AND/OR.
 */
@Internal
public final class FlinkRexExtract {
    private final RexBuilder rexBuilder;

    public FlinkRexExtract(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    public RexNode extract(RexNode rex, Predicate<RexNode> extractable) {
        return extractRecur(rex, extractable);
    }

    private RexNode extractRecur(RexNode rex, Predicate<RexNode> extractable) {
        if (rex.isA(SqlKind.OR) || rex.isA(SqlKind.AND)) {
            Stream<RexNode> ops = operands(rex).map(r -> extractRecur(r, extractable));
            if (rex.isA(SqlKind.OR)) {
                return or(ops);
            }
            return and(ops);
        }
        if (extractable.test(rex)) {
            return rex;
        }
        return rexBuilder.makeLiteral(true);
    }

    private Stream<RexNode> operands(RexNode rex) {
        return ((RexCall) rex).getOperands().stream();
    }

    private RexNode or(Stream<RexNode> rexs) {
        return RexUtil.composeDisjunction(rexBuilder, rexs.collect(Collectors.toList()));
    }

    private RexNode and(Stream<RexNode> rexs) {
        return RexUtil.composeConjunction(rexBuilder, rexs.collect(Collectors.toList()));
    }
}
