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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

/**
 * Schedules manual compactions of small disjoint SST files created by RocksDB. It does so
 * periodically while maintaining {@link RocksDBManualCompactionOptions#MIN_INTERVAL} between
 * compaction rounds, where each round is at most {@link
 * RocksDBManualCompactionOptions#MAX_PARALLEL_COMPACTIONS}.
 */
class CompactionScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionScheduler.class);

    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService ioExecutor;
    private final long checkPeriodMs;
    private final CompactionTracker tracker;
    private final Compactor compactor;
    private final CompactionTaskProducer taskProducer;
    private final Object lock = new Object();
    private boolean running = true;

    public CompactionScheduler(
            RocksDBManualCompactionConfig settings,
            ExecutorService ioExecutor,
            CompactionTaskProducer taskProducer,
            Compactor compactor,
            CompactionTracker tracker) {
        this(
                settings,
                ioExecutor,
                taskProducer,
                compactor,
                tracker,
                Executors.newSingleThreadScheduledExecutor());
    }

    public CompactionScheduler(
            RocksDBManualCompactionConfig settings,
            ExecutorService ioExecutor,
            CompactionTaskProducer taskProducer,
            Compactor compactor,
            CompactionTracker tracker,
            ScheduledExecutorService scheduledExecutor) {
        this.ioExecutor = ioExecutor;
        this.scheduledExecutor = scheduledExecutor;
        this.checkPeriodMs = settings.minInterval;
        this.tracker = tracker;
        this.compactor = compactor;
        this.taskProducer = taskProducer;
    }

    public void start() {
        scheduleScan();
    }

    public void stop() throws InterruptedException {
        synchronized (lock) {
            if (running) {
                running = false;
                scheduledExecutor.shutdownNow();
            }
        }
        if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            LOG.warn("Unable to terminate scheduled tasks in 5s");
        }
    }

    public void scheduleScan() {
        synchronized (lock) {
            if (running) {
                LOG.trace("Schedule SST scan in {} ms", checkPeriodMs);
                scheduledExecutor.schedule(
                        () -> ioExecutor.execute(this::maybeScan),
                        checkPeriodMs,
                        TimeUnit.MILLISECONDS);
            } else {
                LOG.debug("Not scheduling next scan: shutting down");
            }
        }
    }

    public void maybeScan() {
        LOG.trace("Starting SST scan");
        if (tracker.haveManualCompactions() || tracker.isShuttingDown()) {
            LOG.trace("Skip SST scan {}", tracker);
            // nothing to do:
            // previous compactions didn't finish yet
            // the last one will reschedule this task
            return;
        }

        final List<CompactionTask> targets = scan();
        LOG.trace("SST scan resulted in targets {}", targets);
        if (targets.isEmpty()) {
            scheduleScan();
            return;
        }

        for (CompactionTask target : targets) {
            ioExecutor.execute(
                    () ->
                            tracker.runWithTracking(
                                    target.columnFamilyHandle,
                                    () ->
                                            compactor.compact(
                                                    target.columnFamilyHandle,
                                                    target.level,
                                                    target.files),
                                    this::scheduleScan));
        }
    }

    private List<CompactionTask> scan() {
        try {
            return taskProducer.produce();
        } catch (Exception e) {
            LOG.warn("Unable to scan for compaction targets", e);
            return emptyList();
        }
    }
}
