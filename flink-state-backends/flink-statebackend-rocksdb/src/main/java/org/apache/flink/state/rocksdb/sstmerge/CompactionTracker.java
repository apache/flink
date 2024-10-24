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

import org.apache.flink.util.function.RunnableWithException;

import org.rocksdb.ColumnFamilyHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Function;

/**
 * Tracks the number of pending/running compactions (manual and automatic) and the DB status. Used
 * concurrently by different compaction threads and by SST scanning threads.
 */
@ThreadSafe
class CompactionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionTracker.class);

    private final Function<ColumnFamilyHandle, Long> runningAutoCompactions;
    private final int maxManualCompactions;
    private final int maxAutoCompactions;
    private int pendingManualCompactions;
    private int runningManualCompactions;
    private boolean isShuttingDown;

    public CompactionTracker(
            RocksDBManualCompactionConfig settings,
            Function<ColumnFamilyHandle, Long> runningAutoCompactions) {
        this.maxManualCompactions = settings.maxManualCompactions;
        this.maxAutoCompactions = settings.maxAutoCompactions;
        this.runningAutoCompactions = runningAutoCompactions;
        this.isShuttingDown = false;
    }

    private synchronized void complete() {
        runningManualCompactions--;
    }

    private synchronized void cancel() {
        pendingManualCompactions--;
    }

    private synchronized boolean tryStart(ColumnFamilyHandle cf) {
        if (runningManualCompactions >= maxManualCompactions) {
            return false;
        }
        if (isShuttingDown()) {
            return false;
        }
        if (runningAutoCompactions.apply(cf) >= maxAutoCompactions) {
            return false;
        }
        // all good
        pendingManualCompactions--;
        runningManualCompactions++;
        return true;
    }

    private synchronized void runIfNoManualCompactions(Runnable runnable) {
        if (!haveManualCompactions()) {
            runnable.run();
        }
    }

    public synchronized boolean haveManualCompactions() {
        return runningManualCompactions > 0 || pendingManualCompactions > 0;
    }

    public synchronized boolean isShuttingDown() {
        return isShuttingDown;
    }

    public synchronized void close() {
        isShuttingDown = true;
    }

    @Override
    public String toString() {
        return "CompactionTracker{"
                + "maxManualCompactions="
                + maxManualCompactions
                + ", maxAutoCompactions="
                + maxAutoCompactions
                + ", pendingManualCompactions="
                + pendingManualCompactions
                + ", runningManualCompactions="
                + runningManualCompactions
                + ", isShuttingDown="
                + isShuttingDown
                + '}';
    }

    void runWithTracking(
            ColumnFamilyHandle columnFamily,
            RunnableWithException compaction,
            Runnable lastCompactionPostAction) {
        if (tryStart(columnFamily)) {
            try {
                compaction.run();
            } catch (Exception e) {
                LOG.warn("Unable to compact {} (concurrent compaction?)", compaction, e);
            }
            complete();
        } else {
            // drop this task - new will be created with a fresh set of files
            cancel();
        }
        // we were the last manual compaction - schedule the scan
        runIfNoManualCompactions(lastCompactionPostAction);
    }
}
