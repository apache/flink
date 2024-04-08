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

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toRowType;

/**
 * Planner rule that rewrites UNNEST to explode function.
 *
 * <p>Note: This class can only be used in HepPlanner.
 */
@Value.Enclosing
public class LogicalUnnestRule extends RelRule<LogicalUnnestRule.LogicalUnnestRuleConfig> {

    public static final LogicalUnnestRule INSTANCE = LogicalUnnestRuleConfig.DEFAULT.toRule();

    public LogicalUnnestRule(LogicalUnnestRule.LogicalUnnestRuleConfig config) {
        super(config);
    }

    public boolean matches(RelOptRuleCall call) {
        LogicalCorrelate join = call.rel(0);
        RelNode right = getRel(join.getRight());
        if (right instanceof LogicalFilter) {
            LogicalFilter logicalFilter = (LogicalFilter) right;
            RelNode relNode = getRel(logicalFilter.getInput());
            if (relNode instanceof Uncollect) {
                return !((Uncollect) relNode).withOrdinality;
            } else if (relNode instanceof LogicalProject) {
                LogicalProject logicalProject = (LogicalProject) relNode;
                relNode = getRel(logicalProject.getInput());
                if (relNode instanceof Uncollect) {
                    return !((Uncollect) relNode).withOrdinality;
                }
                return false;
            }
        } else if (right instanceof LogicalProject) {
            LogicalProject logicalProject = (LogicalProject) right;
            RelNode relNode = getRel(logicalProject.getInput());
            if (relNode instanceof Uncollect) {
                Uncollect uncollect = (Uncollect) relNode;
                return !uncollect.withOrdinality;
            }
            return false;
        } else if (right instanceof Uncollect) {
            Uncollect uncollect = (Uncollect) right;
            return !uncollect.withOrdinality;
        }
        return false;
    }

    public void onMatch(RelOptRuleCall call) {
        LogicalCorrelate correlate = call.rel(0);
        RelNode outer = getRel(correlate.getLeft());
        RelNode array = getRel(correlate.getRight());

        // convert unnest into table function scan
        RelNode tableFunctionScan = convert(array, correlate);
        // create correlate with table function scan as input
        Correlate newCorrelate =
                correlate.copy(correlate.getTraitSet(), ImmutableList.of(outer, tableFunctionScan));
        call.transformTo(newCorrelate);
    }

    private RelNode convert(RelNode relNode, LogicalCorrelate correlate) {
        if (relNode instanceof HepRelVertex) {
            HepRelVertex hepRelVertex = (HepRelVertex) relNode;
            relNode = convert(getRel(hepRelVertex), correlate);
        }
        if (relNode instanceof LogicalProject) {
            LogicalProject logicalProject = (LogicalProject) relNode;
            return logicalProject.copy(
                    logicalProject.getTraitSet(),
                    ImmutableList.of(convert(getRel(logicalProject.getInput()), correlate)));
        }
        if (relNode instanceof LogicalFilter) {
            LogicalFilter logicalFilter = (LogicalFilter) relNode;
            return logicalFilter.copy(
                    logicalFilter.getTraitSet(),
                    ImmutableList.of(convert(getRel(logicalFilter.getInput()), correlate)));
        }
        if (relNode instanceof Uncollect) {
            Uncollect uncollect = (Uncollect) relNode;
            RelOptCluster cluster = correlate.getCluster();
            FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(cluster);
            RelDataType relDataType =
                    (RelDataType)
                            ((Map.Entry) uncollect.getInput().getRowType().getFieldList().get(0))
                                    .getValue();
            LogicalType logicalType = FlinkTypeFactory.toLogicalType(relDataType);
            BridgingSqlFunction sqlFunction =
                    BridgingSqlFunction.of(
                            cluster, BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS);
            RexNode rexCall =
                    cluster.getRexBuilder()
                            .makeCall(
                                    typeFactory.createFieldTypeFromLogicalType(
                                            toRowType(
                                                    UnnestRowsFunction.getUnnestedType(
                                                            logicalType))),
                                    sqlFunction,
                                    ((LogicalProject) getRel(uncollect.getInput())).getProjects());
            return new LogicalTableFunctionScan(
                    cluster,
                    correlate.getTraitSet(),
                    Collections.emptyList(),
                    rexCall,
                    null,
                    rexCall.getType(),
                    null);
        } else {
            throw new IllegalArgumentException("Unexpected input: " + relNode);
        }
    }

    private RelNode getRel(RelNode rel) {
        if (rel instanceof HepRelVertex) {
            return ((HepRelVertex) rel).getCurrentRel();
        }
        return rel;
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface LogicalUnnestRuleConfig extends RelRule.Config {
        LogicalUnnestRuleConfig DEFAULT =
                ImmutableLogicalUnnestRule.LogicalUnnestRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(LogicalCorrelate.class).anyInputs())
                        .withDescription("LogicalUnnestRule");

        @Override
        default LogicalUnnestRule toRule() {
            return new LogicalUnnestRule(this);
        }
    }
}
