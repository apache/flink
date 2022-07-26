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
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

/**
 * Relational operator that records the names of the sub-query.
 *
 * <p>This class implements {@link Hintable} to access the function {@see
 * org.apache.calcite.plan.RelOptUtil#visitHintable}, so the join hints in current query level will
 * not be propagated into the sub-query.
 */
public abstract class SubQueryAlias extends SingleRel implements Hintable {

    protected final String aliasName;

    protected final com.google.common.collect.ImmutableList<RelHint> hints;

    protected SubQueryAlias(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            String aliasName,
            com.google.common.collect.ImmutableList<RelHint> hints) {
        super(cluster, traits, input);
        this.aliasName = aliasName;
        this.hints = hints;
    }

    public String getAliasName() {
        return aliasName;
    }

    @Override
    public com.google.common.collect.ImmutableList<RelHint> getHints() {
        return hints;
    }
}
