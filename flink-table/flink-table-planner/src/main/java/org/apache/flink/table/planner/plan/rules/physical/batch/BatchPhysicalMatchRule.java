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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.logical.MatchRecognize;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalMatch;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalMatch;
import org.apache.flink.table.planner.plan.rules.physical.common.CommonPhysicalMatchRule;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * The physical rule is responsible for convert {@link FlinkLogicalMatch} to {@link
 * BatchPhysicalMatch}.
 */
public class BatchPhysicalMatchRule extends CommonPhysicalMatchRule {

    public static final RelOptRule INSTANCE = new BatchPhysicalMatchRule();

    private BatchPhysicalMatchRule() {
        super(
                FlinkLogicalMatch.class,
                FlinkConventions.LOGICAL(),
                FlinkConventions.BATCH_PHYSICAL(),
                "BatchPhysicalMatchRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        return super.convert(rel, FlinkConventions.BATCH_PHYSICAL());
    }

    @Override
    protected RelNode convertToPhysicalMatch(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode convertInput,
            MatchRecognize matchRecognize,
            RelDataType rowType) {
        return new BatchPhysicalMatch(cluster, traitSet, convertInput, matchRecognize, rowType);
    }
}
