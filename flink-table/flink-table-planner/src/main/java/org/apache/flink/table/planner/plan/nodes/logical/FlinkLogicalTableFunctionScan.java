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

package org.apache.flink.table.planner.plan.nodes.logical;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Subclass of {@link TableFunctionScan} that is a relational expression which calls a {@link
 * FunctionKind#TABLE} or {@link FunctionKind#PROCESS_TABLE} in Flink.
 */
@Internal
public class FlinkLogicalTableFunctionScan extends TableFunctionScan implements FlinkLogicalRel {

    public static final Converter CONVERTER =
            new Converter(
                    ConverterRule.Config.INSTANCE.withConversion(
                            LogicalTableFunctionScan.class,
                            Convention.NONE,
                            FlinkConventions.LOGICAL(),
                            "FlinkLogicalTableFunctionScanConverter"));

    public FlinkLogicalTableFunctionScan(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RexNode rexCall,
            @Nullable Type elementType,
            RelDataType rowType,
            @Nullable Set<RelColumnMapping> columnMappings) {
        super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);
    }

    @Override
    public TableFunctionScan copy(
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RexNode rexCall,
            @Nullable Type elementType,
            RelDataType rowType,
            @Nullable Set<RelColumnMapping> columnMappings) {
        return new FlinkLogicalTableFunctionScan(
                getCluster(), traitSet, inputs, rexCall, elementType, rowType, columnMappings);
    }

    @Internal
    public static class Converter extends ConverterRule {

        protected Converter(Config config) {
            super(config);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final LogicalTableFunctionScan functionScan = call.rel(0);
            final FunctionDefinition functionDefinition =
                    ShortcutUtils.unwrapFunctionDefinition(functionScan.getCall());
            if (functionDefinition == null) {
                // For Calcite stack functions
                return true;
            }
            final boolean isTableFunction =
                    functionDefinition.getKind() == FunctionKind.TABLE
                            || functionDefinition.getKind() == FunctionKind.PROCESS_TABLE;
            return isTableFunction && !(functionDefinition instanceof TemporalTableFunction);
        }

        @Override
        public @Nullable RelNode convert(RelNode rel) {
            final LogicalTableFunctionScan functionScan = (LogicalTableFunctionScan) rel;
            final RelTraitSet traitSet =
                    rel.getTraitSet().replace(FlinkConventions.LOGICAL()).simplify();
            final List<RelNode> newInputs =
                    functionScan.getInputs().stream()
                            .map(input -> RelOptRule.convert(input, FlinkConventions.LOGICAL()))
                            .collect(Collectors.toList());
            final RexCall rexCall = (RexCall) functionScan.getCall();
            return new FlinkLogicalTableFunctionScan(
                    functionScan.getCluster(),
                    traitSet,
                    newInputs,
                    rexCall,
                    functionScan.getElementType(),
                    functionScan.getRowType(),
                    functionScan.getColumnMappings());
        }
    }
}
