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

import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.state.rocksdb.RocksDBProperty;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/** Default implementation of {@link RocksDBManualCompactionManager}. */
class RocksDBManualCompactionManagerImpl implements RocksDBManualCompactionManager {
    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBManualCompactionManagerImpl.class);

    private final ColumnFamilyLookup lookup;
    private final CompactionScheduler scheduler;
    private final CompactionTracker tracker;

    public RocksDBManualCompactionManagerImpl(
            RocksDB db, RocksDBManualCompactionConfig settings, ExecutorService ioExecutor) {
        this.lookup = new ColumnFamilyLookup();
        this.tracker = new CompactionTracker(settings, cf -> getNumAutoCompactions(db, cf));
        this.scheduler =
                new CompactionScheduler(
                        settings,
                        ioExecutor,
                        new CompactionTaskProducer(db, settings, lookup),
                        new Compactor(db, settings.maxOutputFileSize.getBytes()),
                        tracker);
    }

    @Override
    public void start() {
        scheduler.start();
    }

    @Override
    public void register(RocksDbKvStateInfo stateInfo) {
        LOG.debug("Register state for manual compactions: '{}'", stateInfo.metaInfo.getName());
        lookup.add(stateInfo.columnFamilyHandle);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Stopping RocksDBManualCompactionManager");
        tracker.close();
        try {
            scheduler.stop();
        } catch (Exception e) {
            LOG.warn("Unable to stop compaction scheduler {}", scheduler, e);
        }
    }

    private static long getNumAutoCompactions(RocksDB db, ColumnFamilyHandle columnFamily) {
        try {
            return db.getLongProperty(
                    columnFamily, RocksDBProperty.NumRunningCompactions.getRocksDBProperty());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
