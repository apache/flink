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

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.RestoreTestBase;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Restore tests for {@link StreamExecIntervalJoin} early-firing on processing time.
 *
 * <p>The early-fire timer lives on the wall clock while the join keeps its row-time cleanup, so the
 * speculative pad is due at a processing-time instant recorded in the savepoint. The restored job
 * ingests no data at all: the pad is emitted purely from restored state once that instant passes,
 * which needs a source that stays open rather than one that ends input and closes the window.
 */
public class IntervalJoinProcTimeEarlyFireRestoreTest extends RestoreTestBase {

    private static final long SAVEPOINT_READY_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);

    public IntervalJoinProcTimeEarlyFireRestoreTest() {
        super(StreamExecIntervalJoin.class, AfterRestoreSource.INFINITE);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Collections.singletonList(
                IntervalJoinTestPrograms.INTERVAL_JOIN_PROC_TIME_EARLY_FIRE);
    }

    @Override
    protected void awaitSavepointReady(TableTestProgram program, List<CompletableFuture<?>> futures)
            throws Exception {
        // The join emits nothing before the savepoint, so the default sink-based trigger would fire
        // immediately. Gate on the sources instead: stop-with-savepoint then drains their rows into
        // keyed state, capturing the still-pending early-fire schedule.
        for (SourceTestStep source : program.getSetupSourceTestSteps()) {
            final int count = source.dataBeforeRestore.size();
            if (count > 0) {
                TestValuesTableFactory.awaitSourceEmitted(source.name, count)
                        .get(SAVEPOINT_READY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            }
        }
    }
}
