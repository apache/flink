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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The {@link StreamNonDeterministicPhysicalPlanResolver} tries to resolve the correctness issue
 * caused by 'Non-Deterministic Updates' (NDU) in a changelog pipeline. Changelog may contain kinds
 * of message types: Insert (I), Delete (D), Update_Before (UB), Update_After (UA).
 *
 * <p>There's no NDU problem in an insert-only changelog pipeline.
 *
 * <p>For the updates, there are two cases, with and without upsertKey(a metadata from {@link
 * org.apache.flink.table.planner.plan.metadata.FlinkRelMdUpsertKeys} , consider it as the primary
 * key of the changelog). The upsertKey can be always treated as deterministic, so if all the
 * pipeline operators can transmit upsertKey normally (include working with sink's primary key),
 * everything goes well.
 *
 * <p>The key issue is upsertKey can be easily lost in a pipeline or does not exist from the source
 * or at the sink. All stateful operators can only process an update (D/UB/UA) message by comparing
 * the complete row (retract by row) if without a key identifier, also include a sink without
 * primary key definition that works as retractSink. So under the 'retract by row' mode, a stateful
 * operator requires no non-deterministic column disturb the original changelog row. There are three
 * killers:
 *
 * <p>1. Non-deterministic functions(include scalar, table, aggregate functions, builtin or custom
 * ones)
 *
 * <p>2. LookupJoin on an evolving source
 *
 * <p>3. Cdc-source carries metadata fields(system columns, not belongs to the entity row itself)
 *
 * <p>For the first step, this resolver automatically enables the materialization for
 * No.2(LookupJoin) if needed, and gives the detail error message for No.1 (Non-deterministic
 * functions) and No.3(Cdc-source with metadata) which we think it is relatively easy to change the
 * SQL(add materialization is not a good idea for now, it has very high cost and will bring too much
 * complexity to the operators).
 *
 * <p>Why not do this validation and rewriting in physical-rewrite phase, like {@link
 * org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram} does?
 * Because the physical plan may be changed a lot after physical-rewrite being done, we should check
 * the 'final' plan instead.
 *
 * <p>Some specific plan patterns:
 *
 * <p>1. Non-deterministic scalar function calls
 *
 * <pre>{@code
 * Sink
 *   |
 * Project1{select col1,col2,now(),...}
 *   |
 * Scan1
 * }</pre>
 *
 * <p>2. Non-deterministic table function calls
 *
 * <pre>{@code
 *      Sink
 *        |
 *     Correlate
 *     /      \
 * Project1  TableFunctionScan1
 *    |
 *  Scan1
 * }</pre>
 *
 * <p>3. lookup join: lookup a source which data may change over time
 *
 * <pre>{@code
 *      Sink
 *        |
 *     LookupJoin
 *     /      \
 * Filter1  Source2
 *    |
 * Project1
 *    |
 *  Scan1
 * }</pre>
 *
 * <p>3.1 lookup join: an inner project with non-deterministic function calls or remaining join
 * condition is non-deterministic
 *
 * <pre>{@code
 *      Sink
 *        |
 *     LookupJoin
 *     /      \
 * Filter1  Project2
 *    |        |
 * Project1   Source2
 *    |
 *  Scan1
 * }</pre>
 *
 * <p>4. cdc source with metadata
 *
 * <pre>{@code
 *     Sink
 *       | no upsertKey can be inferred
 *   Correlate
 *     /      \
 *   /       TableFunctionScan1(deterministic)
 * Project1 {select id,name,attr1,op_time}
 *   |
 * Scan {cdc source <id,name,attr1,op_type,op_time> }
 * }</pre>
 *
 * <p>4.1 cdc source with metadata
 *
 * <pre>{@code
 *     Sink
 *       | no upsertKey can be inferred
 *   LookupJoin {lookup key not contains the lookup source's pk}
 *     /      \
 *   /       Source2
 * Project1 {select id,name,attr1,op_time}
 *   |
 * Scan {cdc source <id,name,attr1,op_type,op_time> }
 * }</pre>
 *
 * <p>CDC source with metadata is another form of non-deterministic update.
 *
 * <p>5. grouping keys with non-deterministic column
 *
 * <pre>{@code
 * Sink{pk=(c3,day)}
 *   | upsertKey=(c3,day)
 *  GroupAgg{group by c3, day}
 *   |
 * Project{select c1,c2,DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') day,...}
 *   |
 * Deduplicate{keep last row, dedup on c1,c2}
 *   |
 *  Scan
 * }</pre>
 */
public class StreamNonDeterministicPhysicalPlanResolver {

    /**
     * Try to resolve the NDU problem if configured {@link
     * OptimizerConfigOptions#TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY} is in `TRY_RESOLVE`
     * mode. Will raise an error if the NDU issues in the given plan can not be completely solved.
     */
    public static List<RelNode> resolvePhysicalPlan(
            List<RelNode> expanded, TableConfig tableConfig) {
        OptimizerConfigOptions.NonDeterministicUpdateStrategy handling =
                tableConfig
                        .getConfiguration()
                        .get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY);
        if (handling == OptimizerConfigOptions.NonDeterministicUpdateStrategy.TRY_RESOLVE) {
            Preconditions.checkArgument(
                    expanded.stream().allMatch(rel -> rel instanceof StreamPhysicalRel));
            StreamNonDeterministicUpdatePlanVisitor planResolver =
                    new StreamNonDeterministicUpdatePlanVisitor();

            return expanded.stream()
                    .map(rel -> (StreamPhysicalRel) rel)
                    .map(planResolver::visit)
                    .collect(Collectors.toList());
        }
        // do nothing, return original relNodes
        return expanded;
    }
}
