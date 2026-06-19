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

import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * Manages compactions of small and disjoint RocksDB SST files that otherwise would not be merged to
 * reduce write amplification.
 *
 * <p>Such files are usually small and are inlined into the Checkpoint metadata. Which might lead to
 * exceeding RPC message size on checkpoint ACK or recovery.
 *
 * <p>This class manages compactions of one or more Column Families of a single RocksDB instance.
 *
 * <p>Note that "manual" means that the compactions are <b>requested</b> manually (by Flink), but
 * they are still executed by RocksDB.
 */
public interface RocksDBManualCompactionManager extends AutoCloseable {
    Logger LOG = LoggerFactory.getLogger(RocksDBManualCompactionManager.class);

    static RocksDBManualCompactionManager create(
            RocksDB db, RocksDBManualCompactionConfig settings, ExecutorService ioExecutor) {
        LOG.info("Creating RocksDBManualCompactionManager with settings: {}", settings);
        return settings.minInterval <= 0
                ? NO_OP
                : new RocksDBManualCompactionManagerImpl(db, settings, ioExecutor);
    }

    RocksDBManualCompactionManager NO_OP =
            new RocksDBManualCompactionManager() {
                @Override
                public void register(RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo) {}

                @Override
                public void close() {}

                @Override
                public void start() {}
            };

    void register(RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo);

    @Override
    void close() throws Exception;

    void start();
}
