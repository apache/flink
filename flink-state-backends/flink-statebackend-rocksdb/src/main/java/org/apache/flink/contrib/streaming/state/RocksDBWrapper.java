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

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

/** Wrapper of {@link RocksDB} and {@link RocksDBAccessMetric}. */
public class RocksDBWrapper implements AutoCloseable {
    /**
     * Our RocksDB database, this is used to store state. The different k/v states that we have will
     * have their own RocksDB instance and columnFamilyHandle.
     */
    private final RocksDB db;

    @Nullable private final RocksDBAccessMetric accessMetric;

    private final boolean trackLatencyEnabled;

    public RocksDBWrapper(RocksDB db) {
        this(db, null);
    }

    public RocksDBWrapper(RocksDB db, @Nullable RocksDBAccessMetric.Builder accessMetricBuilder) {
        this.db = db;
        this.accessMetric = accessMetricBuilder != null ? accessMetricBuilder.build() : null;
        this.trackLatencyEnabled = accessMetric != null;
    }

    public RocksDB getDb() {
        return db;
    }

    public RocksDBAccessMetric getAccessMetric() {
        return accessMetric;
    }

    public ColumnFamilyHandle createColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor)
            throws RocksDBException {
        return db.createColumnFamily(columnFamilyDescriptor);
    }

    public void put(
            final ColumnFamilyHandleWrapper columnFamilyHandle,
            final WriteOptions writeOpt,
            final byte[] key,
            final byte[] value)
            throws RocksDBException {
        if (trackLatencyEnabled) {
            putAndUpdateMetric(columnFamilyHandle, writeOpt, key, value);
        } else {
            originalPut(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key, value);
        }
    }

    /**
     * Since {@code WriteBatch} might contain several column families only during restore. We
     * currently does not track latency metrics for that part operations if no column family handle
     * provided.
     */
    public void write(
            @Nullable final ColumnFamilyHandleWrapper columnFamilyHandle,
            final WriteOptions writeOpt,
            final WriteBatch writeBatch)
            throws RocksDBException {
        if (columnFamilyHandle == null) {
            originalWrite(writeOpt, writeBatch);
        } else if (trackLatencyEnabled) {
            writeAndUpdateMetric(columnFamilyHandle, writeOpt, writeBatch);
        } else {
            originalWrite(writeOpt, writeBatch);
        }
    }

    public byte[] get(final ColumnFamilyHandleWrapper columnFamilyHandle, final byte[] key)
            throws RocksDBException {
        if (trackLatencyEnabled) {
            return getAndUpdateMetric(columnFamilyHandle, key);
        } else {
            return originalGet(columnFamilyHandle.getColumnFamilyHandle(), key);
        }
    }

    public void delete(
            final ColumnFamilyHandleWrapper columnFamilyHandle,
            final WriteOptions writeOpt,
            final byte[] key)
            throws RocksDBException {
        if (trackLatencyEnabled) {
            deleteAndUpdateMetric(columnFamilyHandle, writeOpt, key);
        } else {
            originalDelete(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key);
        }
    }

    public void merge(
            final ColumnFamilyHandleWrapper columnFamilyHandle,
            final WriteOptions writeOpt,
            final byte[] key,
            final byte[] value)
            throws RocksDBException {
        if (trackLatencyEnabled) {
            mergeAndUpdateMetric(columnFamilyHandle, writeOpt, key, value);
        } else {
            originalMerge(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key, value);
        }
    }

    private byte[] getAndUpdateMetric(
            final ColumnFamilyHandleWrapper columnFamilyHandle, final byte[] key)
            throws RocksDBException {
        if (accessMetric.checkAndUpdateGetCounter(columnFamilyHandle.getColumnFamilyId())) {
            long start = System.nanoTime();
            byte[] result = originalGet(columnFamilyHandle.getColumnFamilyHandle(), key);
            long end = System.nanoTime();
            accessMetric.updateHistogram(
                    columnFamilyHandle.getColumnFamilyId(),
                    RocksDBAccessMetric.GET_LATENCY,
                    end - start);
            return result;
        } else {
            return originalGet(columnFamilyHandle.getColumnFamilyHandle(), key);
        }
    }

    private void deleteAndUpdateMetric(
            final ColumnFamilyHandleWrapper columnFamilyHandle,
            final WriteOptions writeOpt,
            final byte[] key)
            throws RocksDBException {
        if (accessMetric.checkAndUpdateDeleteCounter(columnFamilyHandle.getColumnFamilyId())) {
            long start = System.nanoTime();
            originalDelete(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key);
            long end = System.nanoTime();
            accessMetric.updateHistogram(
                    columnFamilyHandle.getColumnFamilyId(),
                    RocksDBAccessMetric.DELETE_LATENCY,
                    end - start);
        } else {
            originalDelete(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key);
        }
    }

    private void putAndUpdateMetric(
            final ColumnFamilyHandleWrapper columnFamilyHandle,
            final WriteOptions writeOpt,
            final byte[] key,
            final byte[] value)
            throws RocksDBException {
        if (accessMetric.checkAndUpdatePutCounter(columnFamilyHandle.getColumnFamilyId())) {
            long start = System.nanoTime();
            originalPut(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key, value);
            long end = System.nanoTime();
            accessMetric.updateHistogram(
                    columnFamilyHandle.getColumnFamilyId(),
                    RocksDBAccessMetric.PUT_LATENCY,
                    end - start);
        } else {
            originalPut(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key, value);
        }
    }

    private void writeAndUpdateMetric(
            final ColumnFamilyHandleWrapper columnFamilyHandle,
            final WriteOptions writeOpt,
            final WriteBatch writeBatch)
            throws RocksDBException {
        if (accessMetric.checkAndUpdateWriteBatchCounter(columnFamilyHandle.getColumnFamilyId())) {
            long start = System.nanoTime();
            originalWrite(writeOpt, writeBatch);
            long end = System.nanoTime();
            accessMetric.updateHistogram(
                    columnFamilyHandle.getColumnFamilyId(),
                    RocksDBAccessMetric.WRITE_BATCH_LATENCY,
                    end - start);
        } else {
            originalWrite(writeOpt, writeBatch);
        }
    }

    private void mergeAndUpdateMetric(
            final ColumnFamilyHandleWrapper columnFamilyHandle,
            final WriteOptions writeOpt,
            final byte[] key,
            final byte[] value)
            throws RocksDBException {
        if (accessMetric.checkAndUpdateMergeCounter(columnFamilyHandle.getColumnFamilyId())) {
            long start = System.nanoTime();
            originalMerge(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key, value);
            long end = System.nanoTime();
            accessMetric.updateHistogram(
                    columnFamilyHandle.getColumnFamilyId(),
                    RocksDBAccessMetric.MERGE_LATENCY,
                    end - start);
        } else {
            originalMerge(columnFamilyHandle.getColumnFamilyHandle(), writeOpt, key, value);
        }
    }

    private byte[] originalGet(final ColumnFamilyHandle columnFamilyHandle, final byte[] key)
            throws RocksDBException {
        return db.get(columnFamilyHandle, key);
    }

    private void originalDelete(
            final ColumnFamilyHandle columnFamilyHandle,
            final WriteOptions writeOpt,
            final byte[] key)
            throws RocksDBException {
        db.delete(columnFamilyHandle, writeOpt, key);
    }

    private void originalPut(
            final ColumnFamilyHandle columnFamilyHandle,
            final WriteOptions writeOpts,
            final byte[] key,
            final byte[] value)
            throws RocksDBException {
        db.put(columnFamilyHandle, writeOpts, key, value);
    }

    private void originalWrite(final WriteOptions writeOpts, final WriteBatch writeBatch)
            throws RocksDBException {
        db.write(writeOpts, writeBatch);
    }

    private void originalMerge(
            final ColumnFamilyHandle columnFamilyHandle,
            final WriteOptions writeOpts,
            final byte[] key,
            final byte[] value)
            throws RocksDBException {
        db.merge(columnFamilyHandle, writeOpts, key, value);
    }

    @Override
    public void close() {
        if (accessMetric != null) {
            accessMetric.close();
        }
        db.close();
    }
}
