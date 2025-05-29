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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.legacy.sources.LimitableTableSource;
import org.apache.flink.table.legacy.sources.TableSource;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.Collections;

/**
 * Planner rule that tries to push limit into a {@link LimitableTableSource}. The original limit
 * will still be retained.
 *
 * <p>The reasons why the limit still be retained: 1.If the source is required to return the exact
 * number of limit number, the implementation of the source is highly required. The source is
 * required to accurately control the record number of split, and the parallelism setting also need
 * to be adjusted accordingly. 2.When remove the limit, maybe filter will be pushed down to the
 * source after limit pushed down. The source need know it should do limit first and do the filter
 * later, it is hard to implement. 3.We can support limit with offset, we can push down offset +
 * fetch to table source.
 */
@Internal
@Value.Enclosing
public class PushLimitIntoLegacyTableSourceScanRule
        extends RelRule<
                PushLimitIntoLegacyTableSourceScanRule
                        .PushLimitIntoLegacyTableSourceScanRuleConfig> {

    public static final PushLimitIntoLegacyTableSourceScanRule INSTANCE =
            PushLimitIntoLegacyTableSourceScanRuleConfig.DEFAULT.toRule();

    protected PushLimitIntoLegacyTableSourceScanRule(
            PushLimitIntoLegacyTableSourceScanRuleConfig config) {
        super(config);
    }

    public boolean matches(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        final boolean onlyLimit =
                sort.getCollation().getFieldCollations().isEmpty() && sort.fetch != null;
        if (onlyLimit) {
            LegacyTableSourceTable table =
                    call.rel(1).getTable().unwrap(LegacyTableSourceTable.class);
            if (table != null) {
                TableSource tableSource = table.tableSource();
                return tableSource instanceof LimitableTableSource
                        && !((LimitableTableSource) tableSource).isLimitPushedDown();
            }
        }
        return false;
    }

    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        FlinkLogicalLegacyTableSourceScan scan = call.rel(1);
        LegacyTableSourceTable tableSourceTable =
                scan.getTable().unwrap(LegacyTableSourceTable.class);
        int offset = (sort.offset == null) ? 0 : RexLiteral.intValue(sort.offset);
        int limit = offset + RexLiteral.intValue(sort.fetch);
        RelBuilder relBuilder = call.builder();
        LegacyTableSourceTable newRelOptTable = applyLimit(limit, tableSourceTable);
        FlinkLogicalLegacyTableSourceScan newScan = scan.copy(scan.getTraitSet(), newRelOptTable);

        TableSource newTableSource =
                newRelOptTable.unwrap(LegacyTableSourceTable.class).tableSource();
        TableSource oldTableSource =
                tableSourceTable.unwrap(LegacyTableSourceTable.class).tableSource();

        if (((LimitableTableSource) newTableSource).isLimitPushedDown()
                && newTableSource.explainSource().equals(oldTableSource.explainSource())) {
            throw new TableException(
                    "Failed to push limit into table source! "
                            + "table source with pushdown capability must override and change "
                            + "explainSource() API to explain the pushdown applied!");
        }

        call.transformTo(sort.copy(sort.getTraitSet(), Collections.singletonList(newScan)));
    }

    private LegacyTableSourceTable applyLimit(long limit, FlinkPreparingTableBase relOptTable) {
        LegacyTableSourceTable tableSourceTable = relOptTable.unwrap(LegacyTableSourceTable.class);
        LimitableTableSource limitedSource = (LimitableTableSource) tableSourceTable.tableSource();
        TableSource newTableSource = limitedSource.applyLimit(limit);

        FlinkStatistic statistic = relOptTable.getStatistic();
        long newRowCount =
                (statistic.getRowCount() != null)
                        ? Math.min(limit, statistic.getRowCount().longValue())
                        : limit;
        // Update TableStats after limit push down
        TableStats newTableStats = new TableStats(newRowCount);
        FlinkStatistic newStatistic =
                FlinkStatistic.builder().statistic(statistic).tableStats(newTableStats).build();
        return tableSourceTable.copy(newTableSource, newStatistic);
    }

    /** Configuration for {@link PushLimitIntoLegacyTableSourceScanRule}. */
    @Value.Immutable(singleton = false)
    public interface PushLimitIntoLegacyTableSourceScanRuleConfig extends RelRule.Config {
        PushLimitIntoLegacyTableSourceScanRule.PushLimitIntoLegacyTableSourceScanRuleConfig
                DEFAULT =
                        ImmutablePushLimitIntoLegacyTableSourceScanRule
                                .PushLimitIntoLegacyTableSourceScanRuleConfig.builder()
                                .operandSupplier(
                                        b0 ->
                                                b0.operand(FlinkLogicalSort.class)
                                                        .oneInput(
                                                                b1 ->
                                                                        b1.operand(
                                                                                        FlinkLogicalLegacyTableSourceScan
                                                                                                .class)
                                                                                .noInputs()))
                                .description("PushLimitIntoLegacyTableSourceScanRule")
                                .build()
                                .as(PushLimitIntoLegacyTableSourceScanRuleConfig.class);

        @Override
        default PushLimitIntoLegacyTableSourceScanRule toRule() {
            return new PushLimitIntoLegacyTableSourceScanRule(this);
        }
    }
}
