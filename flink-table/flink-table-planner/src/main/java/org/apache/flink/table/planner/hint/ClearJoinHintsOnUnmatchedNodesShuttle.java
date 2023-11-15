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

package org.apache.flink.table.planner.hint;

import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.hint.FlinkHints.resolveSubQuery;

/**
 * Clear the invalid join hints in the unmatched nodes. For example, a join hint may be attached in
 * the Project node at first. After accepting this shuttle, the join hint in the Project node will
 * be cleared.
 *
 * <p>See more at {@link FlinkHintStrategies}.
 *
 * <p>Tips, hints about view and alias will not be cleared.
 */
public class ClearJoinHintsOnUnmatchedNodesShuttle extends RelHomogeneousShuttle {
    private final HintStrategyTable hintStrategyTable;

    public ClearJoinHintsOnUnmatchedNodesShuttle(HintStrategyTable hintStrategyTable) {
        this.hintStrategyTable = hintStrategyTable;
    }

    @Override
    public RelNode visit(RelNode other) {
        if (FlinkRelOptUtil.containsSubQuery(other)) {
            other = resolveSubQuery(other, relNode -> relNode.accept(this));
        }

        if (other instanceof Hintable) {
            List<RelHint> originHints = ((Hintable) other).getHints();
            // 1. classify the hints and separate out the join hints
            List<RelHint> joinHints =
                    originHints.stream()
                            .filter(h -> JoinStrategy.isJoinStrategy(h.hintName))
                            .collect(Collectors.toList());

            List<RelHint> remainHints = new ArrayList<>(originHints);
            remainHints.removeAll(joinHints);

            // 2. use hintStrategyTable#apply to determine whether the join hint can be attached
            // to the current node
            // If it cannot be attached, it means that the join hint on the current node needs to
            // be removed.
            List<RelHint> hintsCanApply = hintStrategyTable.apply(joinHints, other);
            if (hintsCanApply.size() != joinHints.size()) {
                hintsCanApply.addAll(remainHints);
                // As a result, the remaining hints will be attached.
                other = ((Hintable) other).withHints(hintsCanApply);
            }
        }

        return super.visit(other);
    }
}
