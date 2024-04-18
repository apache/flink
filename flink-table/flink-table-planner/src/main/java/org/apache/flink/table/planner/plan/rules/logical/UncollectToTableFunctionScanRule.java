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

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.functions.table.UnnestRowsFunction;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.immutables.value.Value;

import java.util.Collections;

import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toRowType;

/**
 * Planner rule that converts {@link Uncollect} values to {@link
 * org.apache.calcite.rel.core.TableFunctionScan}.
 */
@Value.Enclosing
public class UncollectToTableFunctionScanRule
        extends RelRule<UncollectToTableFunctionScanRule.UncollectToTableFunctionScanRuleConfig> {
    public static final UncollectToTableFunctionScanRule INSTANCE =
            UncollectToTableFunctionScanRule.UncollectToTableFunctionScanRuleConfig.DEFAULT
                    .toRule();

    public UncollectToTableFunctionScanRule(
            UncollectToTableFunctionScanRule.UncollectToTableFunctionScanRuleConfig config) {
        super(config);
    }

    private boolean isProjectFilterValues(RelNode relNode) {
        if (relNode instanceof LogicalProject) {
            return isProjectFilterValues(((LogicalProject) relNode).getInput());
        } else if (relNode instanceof LogicalFilter) {
            return isProjectFilterValues(((LogicalFilter) relNode).getInput());
        } else if (relNode instanceof HepRelVertex) {
            return isProjectFilterValues(((HepRelVertex) relNode).getCurrentRel());
        } else {
            return relNode instanceof LogicalValues;
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Uncollect array = call.rel(0);
        return isProjectFilterValues(array.getInput());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Uncollect array = call.rel(0);
        RelNode tableFunctionScan = convertUncollect(array);
        call.transformTo(tableFunctionScan);
    }

    private RelNode convertUncollect(Uncollect uc) {
        RelOptCluster cluster = uc.getCluster();
        FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(cluster);
        RelDataType relDataType = uc.getInput().getRowType().getFieldList().get(0).getValue();
        LogicalType logicalType = FlinkTypeFactory.toLogicalType(relDataType);

        SqlFunction sqlFunction =
                BridgingSqlFunction.of(cluster, BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS);

        RexNode rexCall =
                cluster.getRexBuilder()
                        .makeCall(
                                typeFactory.createFieldTypeFromLogicalType(
                                        toRowType(UnnestRowsFunction.getUnnestedType(logicalType))),
                                sqlFunction,
                                ((LogicalProject) getRel(uc.getInput())).getProjects());
        return new LogicalTableFunctionScan(
                cluster,
                uc.getTraitSet(),
                Collections.emptyList(),
                rexCall,
                null,
                rexCall.getType(),
                null);
    }

    private RelNode getRel(RelNode rel) {
        if (rel instanceof HepRelVertex) {
            return ((HepRelVertex) rel).getCurrentRel();
        }
        return rel;
    }

    @Value.Immutable(singleton = false)
    public interface UncollectToTableFunctionScanRuleConfig extends RelRule.Config {
        UncollectToTableFunctionScanRule.UncollectToTableFunctionScanRuleConfig DEFAULT =
                ImmutableUncollectToTableFunctionScanRule.UncollectToTableFunctionScanRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(Uncollect.class).anyInputs())
                        .withDescription("UncollectToTableFunctionScanRule");

        @Override
        default UncollectToTableFunctionScanRule toRule() {
            return new UncollectToTableFunctionScanRule(this);
        }
    }
}
