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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.optimize.StreamNonDeterministicUpdatePlanVisitor;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY;

/**
 * An implementation of {@link PlanAnalyzer} to analyze the potential risk of non-deterministic
 * update when {@link OptimizerConfigOptions#TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY} is
 * ignored.
 */
@Internal
public class NonDeterministicUpdateAnalyzer implements PlanAnalyzer {

    public static final NonDeterministicUpdateAnalyzer INSTANCE =
            new NonDeterministicUpdateAnalyzer();

    private static final StreamNonDeterministicUpdatePlanVisitor NDU_VISITOR =
            new StreamNonDeterministicUpdatePlanVisitor();

    private NonDeterministicUpdateAnalyzer() {}

    @Override
    public Optional<AnalyzedResult> analyze(FlinkRelNode rel) {
        boolean ignoreNDU =
                ShortcutUtils.unwrapTableConfig(rel)
                                .get(TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY)
                        == OptimizerConfigOptions.NonDeterministicUpdateStrategy.IGNORE;
        if (rel instanceof StreamPhysicalRel && ignoreNDU) {
            try {
                StreamPhysicalRel originalRel = (StreamPhysicalRel) rel;
                StreamPhysicalRel resolvedRel = NDU_VISITOR.visit(originalRel);
                List<Boolean> originalRequirement = requireUpsertMaterialize(originalRel);
                List<Boolean> resolvedRequirement = requireUpsertMaterialize(resolvedRel);
                boolean mismatch =
                        IntStream.range(0, originalRequirement.size())
                                .filter(
                                        i ->
                                                !originalRequirement
                                                        .get(i)
                                                        .equals(resolvedRequirement.get(i)))
                                .findAny()
                                .isPresent();
                if (mismatch) {
                    return getAdvice(
                            String.format(
                                    "You might want to enable upsert materialization for look up join operator by configuring ('%s' to '%s')"
                                            + " to resolve the correctness issue caused by 'Non-Deterministic Updates' (NDU) in a changelog pipeline.",
                                    TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY.key(),
                                    OptimizerConfigOptions.NonDeterministicUpdateStrategy
                                            .TRY_RESOLVE));
                }
            } catch (TableException e) {
                return getAdvice(e.getMessage());
            }
        }
        return Optional.empty();
    }

    private Optional<AnalyzedResult> getAdvice(String content) {
        return Optional.of(
                new AnalyzedResult() {
                    @Override
                    public PlanAdvice getAdvice() {
                        return new PlanAdvice(
                                PlanAdvice.Kind.WARNING, PlanAdvice.Scope.QUERY_LEVEL, content);
                    }

                    @Override
                    public List<Integer> getTargetIds() {
                        return Collections.emptyList();
                    }
                });
    }

    private List<Boolean> requireUpsertMaterialize(StreamPhysicalRel rel) {
        List<Boolean> requireUpsertMaterialize = new ArrayList<>();
        rel.accept(
                new RelShuttleImpl() {
                    @Override
                    public RelNode visit(RelNode other) {
                        if (other instanceof StreamPhysicalLookupJoin) {
                            requireUpsertMaterialize.add(
                                    ((StreamPhysicalLookupJoin) other).upsertMaterialize());
                        }
                        return super.visit(other);
                    }
                });
        return requireUpsertMaterialize;
    }
}
