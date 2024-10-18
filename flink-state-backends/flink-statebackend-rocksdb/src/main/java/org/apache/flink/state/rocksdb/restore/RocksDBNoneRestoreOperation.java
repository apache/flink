/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb.restore;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions;
import org.apache.flink.state.rocksdb.ttl.RocksDbTtlCompactFiltersManager;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.Map;
import java.util.function.Function;

/** Encapsulates the process of initiating a RocksDB instance without restore. */
public class RocksDBNoneRestoreOperation<K> implements RocksDBRestoreOperation {
    private final RocksDBHandle rocksHandle;

    public RocksDBNoneRestoreOperation(
            Map<String, RocksDbKvStateInfo> kvStateInformation,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            RocksDBNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            Long writeBufferManagerCapacity) {
        this.rocksHandle =
                new RocksDBHandle(
                        kvStateInformation,
                        instanceRocksDBPath,
                        dbOptions,
                        columnFamilyOptionsFactory,
                        nativeMetricOptions,
                        metricGroup,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity);
    }

    @Override
    public RocksDBRestoreResult restore() throws Exception {
        this.rocksHandle.openDB();
        return new RocksDBRestoreResult(
                this.rocksHandle.getDb(),
                this.rocksHandle.getDefaultColumnFamilyHandle(),
                this.rocksHandle.getNativeMetricMonitor(),
                -1,
                null,
                null,
                null);
    }

    @Override
    public void close() throws Exception {
        this.rocksHandle.close();
    }
}
