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

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.trait.DuplicateChanges;
import org.apache.flink.table.planner.plan.trait.DuplicateChangesTrait;

import org.apache.calcite.rel.RelNode;

/**
 * A {@link FlinkOptimizeProgram} that does some initialization for {@link DuplicateChanges}
 * inference.
 */
public class FlinkDuplicateChangesTraitInitProgram
        implements FlinkOptimizeProgram<StreamOptimizeContext> {

    @Override
    public RelNode optimize(RelNode root, StreamOptimizeContext context) {
        DuplicateChangesTrait trait;
        if (isSink(root)) {
            trait = DuplicateChangesTrait.NONE;
        } else if (context.isAllowDuplicateChanges()) {
            trait = DuplicateChangesTrait.ALLOW;
        } else {
            trait = DuplicateChangesTrait.DISALLOW;
        }
        return root.copy(root.getTraitSet().plus(trait), root.getInputs());
    }

    private boolean isSink(RelNode root) {
        return root instanceof StreamPhysicalSink || root instanceof StreamPhysicalLegacySink;
    }
}
