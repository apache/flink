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

import org.apache.flink.table.planner.plan.nodes.exec.testutils.RestoreTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.Arrays;
import java.util.List;

/** Restore tests for {@link StreamExecDeltaJoin}. */
public class DeltaJoinRestoreTest extends RestoreTestBase {

    public DeltaJoinRestoreTest() {
        super(StreamExecDeltaJoin.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_JOIN_KEY_EQUALS_INDEX,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_JOIN_KEY_CONTAINS_INDEX,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_NON_EQUIV_CONDITION,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_CALC_ON_SOURCE,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_CALC_ON_SOURCE_AND_FILTER_PUSHED_DOWN,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_CACHE,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_CACHE_AND_CALC_ON_SOURCE,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_CDC_SOURCE_WITHOUT_DELETE,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_CALC_ON_CDC_SOURCE_WITHOUT_DELETE,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_CACHE_AND_CDC_SOURCE_WITHOUT_DELETE,
                DeltaJoinTestPrograms.DELTA_JOIN_WITH_CACHE_AND_CALC_ON_CDC_SOURCE_WITHOUT_DELETE);
    }
}
