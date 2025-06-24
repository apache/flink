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

/** Restore tests for {@link StreamExecWindowJoin}. */
public class WindowJoinEventTimeRestoreTest extends RestoreTestBase {

    public WindowJoinEventTimeRestoreTest() {
        super(StreamExecWindowJoin.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                WindowJoinTestPrograms.WINDOW_JOIN_INNER_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_LEFT_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_RIGHT_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_FULL_OUTER_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_SEMI_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_ANTI_TUMBLE_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_HOP_EVENT_TIME,
                WindowJoinTestPrograms.WINDOW_JOIN_CUMULATE_EVENT_TIME);
    }
}
