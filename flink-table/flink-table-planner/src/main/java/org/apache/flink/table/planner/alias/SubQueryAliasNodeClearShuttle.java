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
import org.apache.flink.table.planner.plan.nodes.calcite.SubQueryAlias;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import java.util.Collections;

/** A shuttle to remove sub-query alias node and add the alias hint on the child node. */
public class SubQueryAliasNodeClearShuttle extends RelShuttleImpl {
    @Override
    public RelNode visit(RelNode node) {
        if (node instanceof SubQueryAlias) {
            RelHint aliasTag =
                    RelHint.builder(FlinkHints.HINT_ALIAS)
                            .hintOption(((SubQueryAlias) node).getAliasName())
                            .build();
            RelNode newNode =
                    ((Hintable) ((SubQueryAlias) node).getInput())
                            .attachHints(Collections.singletonList(aliasTag));
            return super.visit(newNode);
        }

        return super.visit(node);
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        filter =
                filter.copy(
                        filter.getTraitSet(),
                        filter.getInput(),
                        filter.getCondition().accept(new RexSubQueryAliasClearShuffle()));
        return super.visit(filter);
    }

    private static class RexSubQueryAliasClearShuffle extends RexShuttle {
        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            return subQuery.clone(subQuery.rel.accept(new SubQueryAliasNodeClearShuttle()));
        }
    }
}
