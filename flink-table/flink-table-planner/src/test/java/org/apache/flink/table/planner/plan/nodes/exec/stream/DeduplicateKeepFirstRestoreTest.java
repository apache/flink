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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Restore tests for the built-in DEDUPLICATE_KEEP_FIRST PTF. */
public class DeduplicateKeepFirstRestoreTest extends RestoreTestBase {

    // Bounds the wait for the pending-candidate savepoint trigger so a stuck source fails fast.
    private static final long SAVEPOINT_READY_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);

    public DeduplicateKeepFirstRestoreTest() {
        super(StreamExecProcessTableFunction.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return List.of(
                DeduplicateKeepFirstRestoreTestPrograms.KEYED_RESTORE,
                DeduplicateKeepFirstRestoreTestPrograms.EVENT_TIME_PENDING_RESTORE,
                DeduplicateKeepFirstRestoreTestPrograms.TTL_EXPIRED_RESTORE,
                DeduplicateKeepFirstRestoreTestPrograms.GLOBAL_TTL_EXPIRED_RESTORE);
    }

    @Override
    protected void awaitSavepointReady(TableTestProgram program, List<CompletableFuture<?>> futures)
            throws Exception {
        if (program != DeduplicateKeepFirstRestoreTestPrograms.EVENT_TIME_PENDING_RESTORE) {
            super.awaitSavepointReady(program, futures);
            return;
        }
        // The candidate is buffered but not emitted before restore, so there is no sink output to
        // gate on. Instead wait until the source has emitted its before-restore rows;
        // stop-with-savepoint then drains them into keyed state before snapshotting.
        for (SourceTestStep source : program.getSetupSourceTestSteps()) {
            final int count = source.dataBeforeRestore.size();
            if (count > 0) {
                TestValuesTableFactory.awaitSourceEmitted(source.name, count)
                        .get(SAVEPOINT_READY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            }
        }
    }
}
