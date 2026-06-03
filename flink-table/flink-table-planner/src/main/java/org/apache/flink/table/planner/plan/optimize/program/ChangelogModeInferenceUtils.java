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

package org.apache.flink.table.planner.plan.optimize.program;

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalcBase;
import org.apache.flink.table.planner.plan.trait.DeleteKind;
import org.apache.flink.table.planner.plan.trait.DeleteKindTrait;
import org.apache.flink.table.planner.plan.trait.DeleteKindTraitDef;
import org.apache.flink.table.planner.plan.trait.ModifyKindSet;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTrait;
import org.apache.flink.table.planner.plan.trait.ModifyKindSetTraitDef;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.Set;

/**
 * Shared helper methods for changelog mode inference, used by {@link
 * FlinkChangelogModeInferenceProgram} and its trait visitors.
 */
final class ChangelogModeInferenceUtils {

    private ChangelogModeInferenceUtils() {}

    /**
     * Whether the condition of the given calc only references non-upsert-key columns. If so, the
     * calc can forward whatever changelog mode is required, because records are filtered based on
     * columns that don't take part in the upsert key.
     */
    static boolean isNonUpsertKeyCondition(StreamPhysicalCalcBase calc) {
        RexProgram program = calc.getProgram();
        if (program.getCondition() == null) {
            return false;
        }

        RexNode condition = program.expandLocalRef(program.getCondition());
        FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(calc.getCluster().getMetadataQuery());
        Set<ImmutableBitSet> upsertKeys = fmq.getUpsertKeys(calc.getInput());
        if (upsertKeys == null || upsertKeys.isEmpty()) {
            // there are no upsert keys, so all columns are non-primary key columns
            return true;
        }

        int[] inputRefIndices =
                RexNodeExtractor.extractRefInputFields(Collections.singletonList(condition));
        ImmutableBitSet inputRefSet = ImmutableBitSet.of(inputRefIndices);
        return upsertKeys.stream().noneMatch(uk -> uk.contains(inputRefSet));
    }

    static ModifyKindSet getModifyKindSet(RelNode node) {
        ModifyKindSetTrait modifyKindSetTrait =
                node.getTraitSet().getTrait(ModifyKindSetTraitDef.INSTANCE());
        return modifyKindSetTrait.modifyKindSet();
    }

    static DeleteKind getDeleteKind(RelNode node) {
        DeleteKindTrait deleteKindTrait =
                node.getTraitSet().getTrait(DeleteKindTraitDef.INSTANCE());
        return deleteKindTrait.deleteKind();
    }
}
