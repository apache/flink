/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.hive;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

/**
 * LogicalDistribution is used to represent the expected distribution of the data, similar to Hive's
 * SORT BY, DISTRIBUTE BY, and CLUSTER BY semantics.
 */
public class LogicalDistribution extends SingleRel {

    // distribution keys
    private final List<Integer> distKeys;
    // sort collation
    private final RelCollation collation;

    private LogicalDistribution(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RelCollation collation,
            List<Integer> distKeys) {
        super(cluster, traits, child);
        this.distKeys = distKeys;
        this.collation = collation;
    }

    public static LogicalDistribution create(
            RelNode input, RelCollation collation, List<Integer> distKeys) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traitSet = input.getTraitSet().replace(Convention.NONE).replace(collation);
        return new LogicalDistribution(cluster, traitSet, input, collation, distKeys);
    }

    public List<Integer> getDistKeys() {
        return distKeys;
    }

    public RelCollation getCollation() {
        return collation;
    }

    @Override
    public LogicalDistribution copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalDistribution(getCluster(), traitSet, inputs.get(0), collation, distKeys);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("collation", collation);
        pw.item("dist", distKeys);
        return pw;
    }
}
