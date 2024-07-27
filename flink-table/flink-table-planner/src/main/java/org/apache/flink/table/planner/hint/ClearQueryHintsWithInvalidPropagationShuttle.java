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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Due to Calcite will expand the whole SQL RelNode tree that contains query block, query hints
 * (including join hints and state ttl hints) will be propagated from root to leaves in the whole
 * RelNode tree. This shuttle is used to clear the query hints that are propagated into the query
 * block incorrectly.
 *
 * <p>See more at {@link
 * org.apache.calcite.sql2rel.SqlToRelConverter#convertFrom(SqlToRelConverter.Blackboard, SqlNode,
 * List)}.
 */
public class ClearQueryHintsWithInvalidPropagationShuttle extends QueryHintsRelShuttle {

    @Override
    protected RelNode doVisit(RelNode node) {
        List<RelHint> hints = ((Hintable) node).getHints();

        Set<String> allHintNames =
                hints.stream().map(hint -> hint.hintName).collect(Collectors.toSet());

        // there are no query hints on this Join/Correlate node
        if (allHintNames.stream().noneMatch(FlinkHints::isQueryHint)) {
            return super.visit(node);
        }

        Optional<RelHint> firstAliasHint =
                hints.stream()
                        .filter(hint -> FlinkHints.HINT_ALIAS.equals(hint.hintName))
                        .findFirst();

        // there are no alias hints on this Join/Correlate/Aggregate node
        if (!firstAliasHint.isPresent()) {
            return super.visit(node);
        }

        List<RelHint> queryHintsFromOuterQueryBlock =
                hints.stream()
                        .filter(
                                hint ->
                                        FlinkHints.isQueryHint(hint.hintName)
                                                // if the size of inheritPath is bigger than 0, it
                                                // means that this query hint is propagated from its
                                                // parent
                                                && hint.inheritPath.size()
                                                        > firstAliasHint.get().inheritPath.size())
                        .collect(Collectors.toList());

        if (queryHintsFromOuterQueryBlock.isEmpty()) {
            return super.visit(node);
        }

        RelNode newRelNode = node;
        ClearOuterQueryHintShuttle clearOuterQueryHintShuttle;

        for (RelHint outerQueryHint : queryHintsFromOuterQueryBlock) {
            clearOuterQueryHintShuttle = new ClearOuterQueryHintShuttle(outerQueryHint);
            newRelNode = newRelNode.accept(clearOuterQueryHintShuttle);
        }

        return super.visit(newRelNode);
    }

    /**
     * A shuttle to clean the query hints which are in outer query block and should not affect the
     * query-block inside.
     *
     * <p>Only the nodes that query hints could attach may be cleared. See more at {@link
     * FlinkHintStrategies}.
     */
    private static class ClearOuterQueryHintShuttle extends RelShuttleImpl {
        // the current inheritPath about the query hint that need be removed
        private final Deque<Integer> currentInheritPath;

        // the query hint that need be removed
        private final RelHint queryHintNeedRemove;

        public ClearOuterQueryHintShuttle(RelHint queryHintNeedRemove) {
            this.queryHintNeedRemove = queryHintNeedRemove;
            this.currentInheritPath = new ArrayDeque<>();
            this.currentInheritPath.addAll(queryHintNeedRemove.inheritPath);
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
            return doVisit(correlate);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            return doVisit(join);
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            return doVisit(aggregate);
        }

        private RelNode doVisit(RelNode node) {
            Hintable hNode = (Hintable) node;
            List<RelHint> hints = new ArrayList<>(hNode.getHints());
            Optional<RelHint> invalidQueryHint = getInvalidQueryHint(hints);

            // if this node contains the query hint that needs to be removed
            if (invalidQueryHint.isPresent()) {
                hints.remove(invalidQueryHint.get());
                return super.visit(hNode.withHints(hints));
            }

            return super.visit(node);
        }

        /**
         * Get the invalid query hint in this node.
         *
         * <p>The invalid node meets the following requirement:
         *
         * <p>1. This hint name is same with the query hint that needs to be removed
         *
         * <p>2.The length of this hint should be same with the length of propagating this removed
         * query hint.
         *
         * <p>3. The inherited path of this hint should match the inherited path of this removed
         * query hint.
         *
         * @param hints all hints
         * @return return the invalid query hint if exists, else return empty
         */
        private Optional<RelHint> getInvalidQueryHint(List<RelHint> hints) {
            for (RelHint hint : hints) {
                if (hint.hintName.equals(queryHintNeedRemove.hintName)
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
