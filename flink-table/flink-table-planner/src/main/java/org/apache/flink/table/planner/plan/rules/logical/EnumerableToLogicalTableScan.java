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

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.immutables.value.Value;

/**
 * Rule that converts an EnumerableTableScan into a LogicalTableScan. We need this rule because
 * Calcite creates an EnumerableTableScan when parsing a SQL query. We convert it into a
 * LogicalTableScan so we can merge the optimization process with any plan that might be created by
 * the Table API.
 */
@Value.Enclosing
public class EnumerableToLogicalTableScan
        extends RelRule<EnumerableToLogicalTableScan.EnumerableToLogicalTableScanConfig> {

    public static final EnumerableToLogicalTableScan INSTANCE =
            EnumerableToLogicalTableScan.EnumerableToLogicalTableScanConfig.DEFAULT.toRule();

    private EnumerableToLogicalTableScan(EnumerableToLogicalTableScanConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableScan oldRel = call.rel(0);
        RelOptTable table = oldRel.getTable();
        LogicalTableScan newRel =
                LogicalTableScan.create(oldRel.getCluster(), table, oldRel.getHints());
        call.transformTo(newRel);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface EnumerableToLogicalTableScanConfig extends RelRule.Config {
        EnumerableToLogicalTableScan.EnumerableToLogicalTableScanConfig DEFAULT =
                ImmutableEnumerableToLogicalTableScan.EnumerableToLogicalTableScanConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 -> b0.operand(EnumerableTableScan.class).anyInputs())
                        .withDescription("EnumerableToLogicalTableScan");

        @Override
        default EnumerableToLogicalTableScan toRule() {
            return new EnumerableToLogicalTableScan(this);
        }
    }
}
