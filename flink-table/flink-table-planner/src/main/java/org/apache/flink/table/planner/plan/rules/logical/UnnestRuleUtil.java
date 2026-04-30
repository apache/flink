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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 * Helpers for rules that target the {@link Correlate} produced by Flink's UNNEST rewrite.
 *
 * <p>{@link LogicalUnnestRule} converts {@code UNNEST} into {@code Correlate(left,
 * LogicalTableFunctionScan(INTERNAL_UNNEST_ROWS[_WITH_ORDINALITY](...)))}. Other Correlate shapes
 * (lateral table functions, temporal joins, vector search, ML predict, Python UDTFs) deliberately
 * fall outside this matcher.
 */
final class UnnestRuleUtil {

    private UnnestRuleUtil() {}

    /** Returns whether the right input of {@code correlate} is a Flink UNNEST table function. */
    static boolean isUnnestCorrelate(Correlate correlate) {
        RelNode right = unwrap(correlate.getRight());
        if (!(right instanceof LogicalTableFunctionScan)) {
            return false;
        }
        return isUnnestCall(((LogicalTableFunctionScan) right).getCall());
    }

    private static boolean isUnnestCall(RexNode call) {
        if (!(call instanceof RexCall)) {
            return false;
        }
        SqlOperator op = ((RexCall) call).getOperator();
        if (!(op instanceof BridgingSqlFunction)) {
            return false;
        }
        FunctionDefinition def = ((BridgingSqlFunction) op).getDefinition();
        return def == BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS
                || def == BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS_WITH_ORDINALITY;
    }

    private static RelNode unwrap(RelNode rel) {
        if (rel instanceof HepRelVertex) {
            return unwrap(((HepRelVertex) rel).getCurrentRel());
        }
        if (rel instanceof RelSubset) {
            RelSubset subset = (RelSubset) rel;
            RelNode best = subset.getBest();
            if (best != null) {
                return unwrap(best);
            }
            RelNode original = subset.getOriginal();
            if (original != null) {
                return unwrap(original);
            }
        }
        return rel;
    }
}
