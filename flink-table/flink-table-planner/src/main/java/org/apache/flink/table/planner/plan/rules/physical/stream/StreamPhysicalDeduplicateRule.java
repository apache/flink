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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.RankUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.immutables.value.Value;

/**
 * Rule that matches {@link StreamPhysicalRank} which is sorted by time attribute and limits 1 and
 * its rank type is ROW_NUMBER and input doesn't produce changelog, and converts it to {@link
 * StreamPhysicalDeduplicate}.
 *
 * <p>NOTES: Queries that can be converted to {@link StreamPhysicalDeduplicate} could be converted
 * to {@link StreamPhysicalRank} too. {@link StreamPhysicalDeduplicate} is more efficient than
 * {@link StreamPhysicalRank} due to mini-batch and less state access.
 *
 * <p>e.g. 1. {@code SELECT a, b, c FROM ( SELECT a, b, c, proctime, ROW_NUMBER() OVER (PARTITION BY
 * a ORDER BY proctime ASC) as row_num FROM MyTable ) WHERE row_num <= 1 } will be converted to
 * StreamExecDeduplicate which keeps first row in proctime.
 *
 * <p>2. {@code SELECT a, b, c FROM ( SELECT a, b, c, rowtime, ROW_NUMBER() OVER (PARTITION BY a
 * ORDER BY rowtime DESC) as row_num FROM MyTable ) WHERE row_num <= 1 } will be converted to
 * StreamExecDeduplicate which keeps last row in rowtime.
 */
@Value.Enclosing
public class StreamPhysicalDeduplicateRule
        extends RelRule<StreamPhysicalDeduplicateRule.StreamPhysicalDeduplicateRuleConfig> {

    public static final StreamPhysicalDeduplicateRule INSTANCE =
            StreamPhysicalDeduplicateRule.StreamPhysicalDeduplicateRuleConfig.DEFAULT.toRule();

    private StreamPhysicalDeduplicateRule(StreamPhysicalDeduplicateRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        StreamPhysicalRank rank = call.rel(0);
        return ChangelogPlanUtils.inputInsertOnly(rank) && RankUtil.canConvertToDeduplicate(rank);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalRank rank = call.rel(0);

        // order by timeIndicator desc ==> lastRow, otherwise is firstRow
        RelFieldCollation fieldCollation = rank.orderKey().getFieldCollations().get(0);
        boolean isLastRow = fieldCollation.direction.isDescending();

        RelDataType fieldType =
                rank.getInput()
                        .getRowType()
                        .getFieldList()
                        .get(fieldCollation.getFieldIndex())
                        .getType();
        boolean isRowtime = FlinkTypeFactory.isRowtimeIndicatorType(fieldType);

        StreamPhysicalDeduplicate deduplicate =
                new StreamPhysicalDeduplicate(
                        rank.getCluster(),
                        rank.getTraitSet(),
                        rank.getInput(),
                        rank.partitionKey().toArray(),
                        isRowtime,
                        isLastRow);
        call.transformTo(deduplicate);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface StreamPhysicalDeduplicateRuleConfig extends RelRule.Config {
        StreamPhysicalDeduplicateRule.StreamPhysicalDeduplicateRuleConfig DEFAULT =
                ImmutableStreamPhysicalDeduplicateRule.StreamPhysicalDeduplicateRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(StreamPhysicalRank.class).anyInputs())
                        .withDescription("StreamPhysicalDeduplicateRule");

        @Override
        default StreamPhysicalDeduplicateRule toRule() {
            return new StreamPhysicalDeduplicateRule(this);
        }
    }
}
