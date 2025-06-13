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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.sql.SqlExplainLevel;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A validator to check whether there is at least one {@link StreamPhysicalDeltaJoin} when {@link
 * OptimizerConfigOptions#TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY} is set to {@link
 * OptimizerConfigOptions.DeltaJoinStrategy#FORCE}.
 *
 * <p>If there is a {@link StreamPhysicalJoin} but no {@link StreamPhysicalDeltaJoin}, an exception
 * will be thrown.
 */
public class StreamPhysicalDeltaJoinForceValidator {

    /** Validate the roots if there is at least one {@link StreamPhysicalDeltaJoin}. */
    public static List<RelNode> validatePhysicalPlan(List<RelNode> roots, TableConfig tableConfig) {
        OptimizerConfigOptions.DeltaJoinStrategy deltaJoinStrategy =
                tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY);
        if (OptimizerConfigOptions.DeltaJoinStrategy.FORCE != deltaJoinStrategy) {
            return roots;
        }

        DeltaJoinFinder finder = new DeltaJoinFinder();
        roots.forEach(finder::go);

        if (!finder.regularJoinExists && !finder.deltaJoinExists) {
            return roots;
        }
        if (finder.deltaJoinExists) {
            return roots;
        }

        // TODO FLINK-37954 full the exception message
        throw new ValidationException(
                String.format(
                        "The current sql doesn't support to do delta join optimization. The plan is:\n\n%s",
                        roots.stream()
                                .map(StreamPhysicalDeltaJoinForceValidator::getPlanAsString)
                                .collect(Collectors.joining("\n"))));
    }

    private static String getPlanAsString(RelNode root) {
        return FlinkRelOptUtil.toString(
                root,
                SqlExplainLevel.DIGEST_ATTRIBUTES,
                false, // withIdPrefix
                true, // withChangelogTraits
                false, // withRowType
                false, // withUpsertKey
                false, // withQueryBlockAlias
                true // withDuplicateChangesTrait
                );
    }

    private static class DeltaJoinFinder extends RelVisitor {

        private boolean deltaJoinExists = false;
        private boolean regularJoinExists = false;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (node instanceof StreamPhysicalJoin) {
                regularJoinExists = true;
            } else if (node instanceof StreamPhysicalDeltaJoin) {
                deltaJoinExists = true;
            }
            super.visit(node, ordinal, parent);
        }
    }
}
