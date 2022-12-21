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

package org.apache.flink.table.planner.alias;

import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.hint.JoinStrategy;

import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Due to Calcite will expand the whole SQL RelNode tree that contains query block, join hints will
 * be propagated from root to leaves in the whole RelNode tree. This shuttle is used to clear the
 * join hints that are propagated into the query block incorrectly.
 *
 * <p>See more at {@see org.apache.calcite.sql2rel.SqlToRelConverter#convertFrom()}.
 *
 * <p>TODO some node will be attached join hints when parse SqlNode to RelNode such as Project and
 * etc. The join hints on these node can also be cleared.
 */
public class ClearJoinHintWithInvalidPropagationShuttle extends RelShuttleImpl {

    @Override
    public RelNode visit(LogicalJoin join) {
        return visitBiRel(join);
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return visitBiRel(correlate);
    }

    private RelNode visitBiRel(BiRel biRel) {
        List<RelHint> hints = ((Hintable) biRel).getHints();

        Set<String> allHintNames =
                hints.stream().map(hint -> hint.hintName).collect(Collectors.toSet());

        // there are no join hints on this Join/Correlate node
        if (allHintNames.stream().noneMatch(JoinStrategy::isJoinStrategy)) {
            return super.visit(biRel);
        }

        Optional<RelHint> firstAliasHint =
                hints.stream()
                        .filter(hint -> FlinkHints.HINT_ALIAS.equals(hint.hintName))
                        .findFirst();

        // there are no alias hints on this Join/Correlate node
        if (!firstAliasHint.isPresent()) {
            return super.visit(biRel);
        }

        List<RelHint> joinHintsFromOuterQueryBlock =
                hints.stream()
                        .filter(
                                hint ->
                                        JoinStrategy.isJoinStrategy(hint.hintName)
                                                // if the size of inheritPath is bigger than 0, it
                                                // means that this join hint is propagated from its
                                                // parent
                                                && hint.inheritPath.size()
                                                        > firstAliasHint.get().inheritPath.size())
                        .collect(Collectors.toList());

        if (joinHintsFromOuterQueryBlock.isEmpty()) {
            return super.visit(biRel);
        }

        RelNode newJoin = biRel;
        ClearOuterJoinHintShuttle clearOuterJoinHintShuttle;

        for (RelHint outerJoinHint : joinHintsFromOuterQueryBlock) {
            clearOuterJoinHintShuttle = new ClearOuterJoinHintShuttle(outerJoinHint);
            newJoin = newJoin.accept(clearOuterJoinHintShuttle);
        }

        return super.visit(newJoin);
    }

    /**
     * A shuttle to clean the join hints which are in outer query block and should not affect the
     * query-block inside.
     */
    private static class ClearOuterJoinHintShuttle extends RelShuttleImpl {
        // the current inheritPath about the join hint that need be removed
        private final Deque<Integer> currentInheritPath;

        // the join hint that need be removed
        private final RelHint joinHintNeedRemove;

        public ClearOuterJoinHintShuttle(RelHint joinHintNeedRemove) {
            this.joinHintNeedRemove = joinHintNeedRemove;
            this.currentInheritPath = new ArrayDeque<>();
            this.currentInheritPath.addAll(joinHintNeedRemove.inheritPath);
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            currentInheritPath.addLast(i);
            RelNode newNode = super.visitChild(parent, i, child);
            currentInheritPath.removeLast();
            return newNode;
        }

        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            return visitBiRel(correlate);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            return visitBiRel(join);
        }

        private RelNode visitBiRel(BiRel biRel) {
            Hintable hBiRel = (Hintable) biRel;
            List<RelHint> hints = new ArrayList<>(hBiRel.getHints());
            Optional<RelHint> invalidJoinHint = getInvalidJoinHint(hints);

            // if this join node contains the join hint that needs to be removed
            if (invalidJoinHint.isPresent()) {
                hints.remove(invalidJoinHint.get());
                return super.visit(hBiRel.withHints(hints));
            }

            return super.visit(biRel);
        }

        /**
         * Get the invalid join hint in this node.
         *
         * <p>The invalid join meets the following requirement:
         *
         * <p>1. This hint name is same with the join hint that needs to be removed
         *
         * <p>2.The length of this hint should be same with the length of propagating this removed
         * join hint.
         *
         * <p>3. The inherited path of this hint should match the inherited path of this removed
         * join hint.
         *
         * @param hints all hints
         * @return return the invalid join hint if exists, else return empty
         */
        private Optional<RelHint> getInvalidJoinHint(List<RelHint> hints) {
            for (RelHint hint : hints) {
                if (hint.hintName.equals(joinHintNeedRemove.hintName)
                        && isMatchInvalidInheritPath(
                                new ArrayList<>(currentInheritPath), hint.inheritPath)) {
                    return Optional.of(hint);
                }
            }
            return Optional.empty();
        }

        private boolean isMatchInvalidInheritPath(
                List<Integer> invalidInheritPath, List<Integer> checkedInheritPath) {
            if (invalidInheritPath.size() != checkedInheritPath.size()) {
                return false;
            }

            for (int i = 0; i < invalidInheritPath.size(); i++) {
                if (!Objects.equals(invalidInheritPath.get(i), checkedInheritPath.get(i))) {
                    return false;
                }
            }
            return true;
        }
    }
}
