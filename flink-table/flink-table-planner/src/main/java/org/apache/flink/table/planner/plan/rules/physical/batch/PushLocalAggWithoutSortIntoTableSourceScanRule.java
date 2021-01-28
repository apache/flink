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

import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Planner rule that tries to push a local hash or sort aggregate which without sort into a {@link
 * BatchPhysicalTableSourceScan} which table is a {@link TableSourceTable}. And the table source in
 * the table is a {@link SupportsAggregatePushDown}.
 *
 * <p>When the {@code OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED} is
 * true, we have the original physical plan:
 *
 * <pre>{@code
 * BatchPhysicalHashAggregate (global)
 * +- BatchPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *    +- BatchPhysicalGroupAggregateBase (local)
 *       +- BatchPhysicalTableSourceScan
 * }</pre>
 *
 * <p>This physical plan will be rewritten to:
 *
 * <pre>{@code
 * BatchPhysicalHashAggregate (global)
 * +- BatchPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *    +- BatchPhysicalTableSourceScan (with local aggregate pushed down)
 * }</pre>
 */
public class PushLocalAggWithoutSortIntoTableSourceScanRule
        extends PushLocalAggIntoTableSourceScanRuleBase {
    public static final PushLocalAggWithoutSortIntoTableSourceScanRule INSTANCE =
            new PushLocalAggWithoutSortIntoTableSourceScanRule();

    public PushLocalAggWithoutSortIntoTableSourceScanRule() {
        super(
                operand(
                        BatchPhysicalExchange.class,
                        operand(
                                BatchPhysicalGroupAggregateBase.class,
                                operand(BatchPhysicalTableSourceScan.class, none()))),
                "PushLocalAggWithoutSortIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        BatchPhysicalGroupAggregateBase localAggregate = call.rel(1);
        BatchPhysicalTableSourceScan tableSourceScan = call.rel(2);
        return isMatch(call, localAggregate, tableSourceScan);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        BatchPhysicalGroupAggregateBase localHashAgg = call.rel(1);
        BatchPhysicalTableSourceScan oldScan = call.rel(2);
        pushLocalAggregateIntoScan(call, localHashAgg, oldScan);
    }
}
