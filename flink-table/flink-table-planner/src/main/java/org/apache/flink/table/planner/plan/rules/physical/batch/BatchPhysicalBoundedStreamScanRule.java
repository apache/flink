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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalDataStreamTableScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalBoundedStreamScan;
import org.apache.flink.table.planner.plan.schema.DataStreamTable;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule that converts {@link FlinkLogicalDataStreamTableScan} to @{@link
 * BatchPhysicalBoundedStreamScan}.
 */
public class BatchPhysicalBoundedStreamScanRule extends ConverterRule {

    public static final RelOptRule INSTANCE =
            new BatchPhysicalBoundedStreamScanRule(
                    Config.INSTANCE
                            .withConversion(
                                    FlinkLogicalDataStreamTableScan.class,
                                    FlinkConventions.LOGICAL(),
                                    FlinkConventions.BATCH_PHYSICAL(),
                                    "BatchPhysicalBoundedStreamScanRule")
                            .withRuleFactory(BatchPhysicalBoundedStreamScanRule::new));

    protected BatchPhysicalBoundedStreamScanRule(Config config) {
        super(config);
    }

    /** If the input is not a DataStreamTable, we want the TableScanRule to match instead */
    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalDataStreamTableScan scan = call.rel(0);
        DataStreamTable dataStreamTable = scan.getTable().unwrap(DataStreamTable.class);
        return dataStreamTable != null;
    }

    @Override
    public RelNode convert(RelNode rel) {
        FlinkLogicalDataStreamTableScan scan = (FlinkLogicalDataStreamTableScan) rel;
        RelTraitSet newTrait = rel.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
        return new BatchPhysicalBoundedStreamScan(
                rel.getCluster(), newTrait, scan.getHints(), scan.getTable(), rel.getRowType());
    }
}
