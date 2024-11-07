/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb.sstmerge;

import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.Test;

import java.util.Collections;

class CompactionSchedulerTest {
    @Test
    void testClose() throws InterruptedException {
        RocksDBManualCompactionConfig config = RocksDBManualCompactionConfig.getDefault();
        ManuallyTriggeredScheduledExecutorService scheduledExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(
                        config,
                        ioExecutor,
                        new CompactionTaskProducer(
                                Collections::emptyList, config, new ColumnFamilyLookup()),
                        new Compactor(Compactor.CompactionTarget.NO_OP, 1L),
                        new CompactionTracker(config, ign -> 0L),
                        scheduledExecutor);
        compactionScheduler.start();
        scheduledExecutor.triggerScheduledTasks();
        compactionScheduler.stop();
        ioExecutor.triggerAll(); // should not fail e.g. because compactionScheduler was stopped
        ioExecutor.shutdown();
    }
}
