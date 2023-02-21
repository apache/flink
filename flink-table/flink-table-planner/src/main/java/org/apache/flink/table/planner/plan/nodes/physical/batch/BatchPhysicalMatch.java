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

package org.apache.flink.table.planner.plan.nodes.physical.batch;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.logical.MatchRecognize;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMatch;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalMatch;
import org.apache.flink.table.planner.plan.utils.MatchUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/** Batch physical RelNode which matches along with MATCH_RECOGNIZE. */
public class BatchPhysicalMatch extends CommonPhysicalMatch implements BatchPhysicalRel {

    public BatchPhysicalMatch(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode inputNode,
            MatchRecognize logicalMatch,
            RelDataType outputRowType) {
        super(cluster, traitSet, inputNode, logicalMatch, outputRowType);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new BatchPhysicalMatch(
                getCluster(), traitSet, inputs.get(0), getLogicalMatch(), deriveRowType());
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        return new BatchExecMatch(
                unwrapTableConfig(this),
                MatchUtil.createMatchSpec(getLogicalMatch()),
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }
}
