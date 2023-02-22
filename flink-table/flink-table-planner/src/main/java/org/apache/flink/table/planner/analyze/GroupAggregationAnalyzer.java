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

package org.apache.flink.table.planner.analyze;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.rules.physical.stream.TwoStageOptimizedAggregateRule;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.TableConfigUtils.getAggPhaseStrategy;

/**
 * An implementation of {@link PlanAnalyzer} to analyze whether group aggregation can be optimized
 * to local-global two_phase aggregation by tuning table configurations.
 */
@Internal
public class GroupAggregationAnalyzer implements PlanAnalyzer {

    public static final GroupAggregationAnalyzer INSTANCE = new GroupAggregationAnalyzer();

    private GroupAggregationAnalyzer() {}

    @Override
    public Optional<AnalyzedResult> analyze(FlinkRelNode rel) {
        TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(rel);
        List<Integer> targetRelIds = new ArrayList<>();
        if (rel instanceof FlinkPhysicalRel) {
            rel.accept(
                    new RelShuttleImpl() {
                        @Override
                        public RelNode visit(RelNode other) {
                            if (other instanceof StreamPhysicalGroupAggregate) {
                                if (((TwoStageOptimizedAggregateRule)
                                                TwoStageOptimizedAggregateRule.INSTANCE())
                                        .matchesTwoStage(
                                                (StreamPhysicalGroupAggregate) other,
                                                other.getInput(0).getInput(0))) {
                                    targetRelIds.add(other.getId());
                                }
                            }
                            return super.visit(other);
                        }
                    });
            if (!targetRelIds.isEmpty()) {
                return Optional.of(
                        new AnalyzedResult() {
                            @Override
                            public PlanAdvice getAdvice() {
                                return new PlanAdvice(
                                        PlanAdvice.Kind.ADVICE,
                                        PlanAdvice.Scope.NODE_LEVEL,
                                        getAdviceContent(tableConfig));
                            }

                            @Override
                            public List<Integer> getTargetIds() {
                                return targetRelIds;
                            }
                        });
            }
        }
        return Optional.empty();
    }

    private String getAdviceContent(TableConfig tableConfig) {
        boolean isMiniBatchEnabled =
                tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED);
        AggregatePhaseStrategy aggStrategy = getAggPhaseStrategy(tableConfig);
        long miniBatchLatency =
                tableConfig
                        .get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY)
                        .toMillis();
        long miniBatchSize = tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE);
        Map<ConfigOption<?>, String> tuningConfigs = new LinkedHashMap<>();
        if (aggStrategy == AggregatePhaseStrategy.ONE_PHASE) {
            tuningConfigs.put(
                    OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                    String.format(
                            "'%s'",
                            OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY
                                    .defaultValue()));
        }
        if (!isMiniBatchEnabled) {
            tuningConfigs.put(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, "'true'");
        }
        if (miniBatchLatency <= 0) {
            tuningConfigs.put(
                    ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                    "a positive long value");
        }
        if (miniBatchSize <= 0) {
            tuningConfigs.put(
                    ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, "a positive long value");
        }
        return String.format(
                "You might want to enable local-global two-phase optimization by configuring %s.",
                tuningConfigs.entrySet().stream()
                        .map(
                                entry ->
                                        String.format(
                                                "'%s' to %s",
                                                entry.getKey().key(), entry.getValue()))
                        .collect(Collectors.joining(", ", "(", ")")));
    }
}
