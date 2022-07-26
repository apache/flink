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

package org.apache.flink.table.planner.plan.nodes.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;

/**
 * Sub-class of {@link SubQueryAlias} that is a relational operator which that records the names of
 * the sub-query. This class corresponds to Calcite logical rel.
 */
public class LogicalSubQueryAlias extends SubQueryAlias {

    public LogicalSubQueryAlias(
            RelOptCluster cluster, RelTraitSet traits, RelNode input, String aliasName) {
        super(cluster, traits, input, aliasName, com.google.common.collect.ImmutableList.of());
    }

    public LogicalSubQueryAlias(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            String aliasName,
            com.google.common.collect.ImmutableList<RelHint> hints) {
        super(cluster, traits, input, aliasName, hints);
    }

    public static LogicalSubQueryAlias create(RelNode input, SqlIdentifier aliasName) {
        return new LogicalSubQueryAlias(
                input.getCluster(), input.getTraitSet(), input, aliasName.toString());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("aliasName", getAliasName());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalSubQueryAlias(getCluster(), traitSet, sole(inputs), getAliasName());
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return new LogicalSubQueryAlias(
                getCluster(),
                traitSet,
                input,
                aliasName,
                (com.google.common.collect.ImmutableList<RelHint>) hintList);
    }
}
