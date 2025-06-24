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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.table.planner.hint.FlinkHints;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A shuttle to remove query block alias hint. */
public class ClearQueryBlockAliasResolver extends RelShuttleImpl {

    public List<RelNode> resolve(List<RelNode> roots) {
        return roots.stream().map(node -> node.accept(this)).collect(Collectors.toList());
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
        RelNode newParent = clearQueryBlockAlias(parent);
        return super.visitChild(newParent, i, child);
    }

    @Override
    public RelNode visit(TableScan scan) {
        RelNode newScan = clearQueryBlockAlias(scan);
        return super.visit(newScan);
    }

    private RelNode clearQueryBlockAlias(RelNode relNode) {
        if (!(relNode instanceof Hintable)) {
            return relNode;
        }

        List<RelHint> hints = ((Hintable) relNode).getHints();
        List<RelHint> newHints = new ArrayList<>();
        for (RelHint hint : hints) {
            if (!FlinkHints.HINT_ALIAS.equals(hint.hintName)) {
                newHints.add(hint);
            }
        }

        if (newHints.size() != hints.size()) {
            return ((Hintable) relNode).withHints(newHints);
        }

        return relNode;
    }
}
