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

/** Restore tests for {@link StreamExecJoin}. */
public class JoinRestoreTest extends RestoreTestBase {

    public JoinRestoreTest() {
        super(StreamExecJoin.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                JoinTestPrograms.NON_WINDOW_INNER_JOIN,
                JoinTestPrograms.NON_WINDOW_INNER_JOIN_WITH_NULL,
                JoinTestPrograms.CROSS_JOIN,
                JoinTestPrograms.JOIN_WITH_FILTER,
                JoinTestPrograms.INNER_JOIN_WITH_DUPLICATE_KEY,
                JoinTestPrograms.INNER_JOIN_WITH_NON_EQUI_JOIN,
                JoinTestPrograms.INNER_JOIN_WITH_EQUAL_PK,
                JoinTestPrograms.INNER_JOIN_WITH_PK,
                JoinTestPrograms.FULL_OUTER,
                JoinTestPrograms.LEFT_JOIN,
                JoinTestPrograms.RIGHT_JOIN,
                JoinTestPrograms.SEMI_JOIN,
                JoinTestPrograms.ANTI_JOIN,
                JoinTestPrograms.JOIN_WITH_STATE_TTL_HINT);
    }
}
