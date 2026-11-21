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

import java.util.Collections;
import java.util.List;

/**
 * Restore tests for {@link StreamExecIntervalJoin} early-firing on processing time with async
 * state, from the plan and savepoint of {@link IntervalJoinProcTimeEarlyFireRestoreTest}, which
 * were generated without async state. Do not generate test files with this class, as it shares them
 * with {@link IntervalJoinProcTimeEarlyFireRestoreTest}.
 */
public class IntervalJoinProcTimeEarlyFireAsyncStateRestoreTest extends RestoreTestBase {

    public IntervalJoinProcTimeEarlyFireAsyncStateRestoreTest() {
        super(StreamExecIntervalJoin.class, AfterRestoreSource.INFINITE);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Collections.singletonList(
                IntervalJoinTestPrograms.INTERVAL_JOIN_PROC_TIME_EARLY_FIRE_ASYNC_STATE);
    }
}
