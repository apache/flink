/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;

import static org.apache.flink.table.planner.hint.FlinkHints.resolveSubQuery;
import static org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil.containsSubQuery;

/** An abstract shuttle for each shuttle used for query hint. */
public abstract class QueryHintsRelShuttle extends RelShuttleImpl {

    @Override
    public RelNode visit(LogicalJoin join) {
        if (containsSubQuery(join)) {
            join = (LogicalJoin) resolveSubQuery(join, relNode -> relNode.accept(this));
        }
        return doVisit(join);
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return doVisit(correlate);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return doVisit(aggregate);
    }

    protected abstract RelNode doVisit(RelNode node);

    @Override
    public RelNode visit(LogicalFilter filter) {
        if (containsSubQuery(filter)) {
            filter = (LogicalFilter) resolveSubQuery(filter, relNode -> relNode.accept(this));
        }
        return super.visit(filter);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        if (containsSubQuery(project)) {
            project = (LogicalProject) resolveSubQuery(project, relNode -> relNode.accept(this));
        }
        return super.visit(project);
    }
}
