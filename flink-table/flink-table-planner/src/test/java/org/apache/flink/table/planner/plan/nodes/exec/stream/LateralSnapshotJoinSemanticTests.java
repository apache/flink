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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.planner.plan.nodes.exec.testutils.SemanticTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.List;

/** Semantic tests for {@link StreamExecLateralSnapshotJoin}. */
public class LateralSnapshotJoinSemanticTests extends SemanticTestBase {
    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                LateralSnapshotJoinSemanticTestPrograms.INNER_JOIN,
                LateralSnapshotJoinSemanticTestPrograms.LEFT_JOIN,
                LateralSnapshotJoinSemanticTestPrograms.SELECT_STAR,
                LateralSnapshotJoinSemanticTestPrograms.COMPOSITE_KEYS,
                LateralSnapshotJoinSemanticTestPrograms.NON_EQUI,
                LateralSnapshotJoinSemanticTestPrograms.EMPTY_BUILD_INNER,
                LateralSnapshotJoinSemanticTestPrograms.EMPTY_BUILD_LEFT,
                LateralSnapshotJoinSemanticTestPrograms.FLIP_AT_END,
                LateralSnapshotJoinSemanticTestPrograms.DEFAULT_COMPILE_TIME,
                LateralSnapshotJoinSemanticTestPrograms.LIVE_JOIN,
                LateralSnapshotJoinSemanticTestPrograms.BUFFERED_THEN_DRAINED);
    }
}
