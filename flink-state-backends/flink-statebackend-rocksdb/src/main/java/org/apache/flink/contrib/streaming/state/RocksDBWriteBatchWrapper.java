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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * It's a wrapper class around RocksDB's {@link WriteBatch} for writing in bulk.
 *
 * <p>IMPORTANT: This class is not thread safe.
 */
public class RocksDBWriteBatchWrapper implements AutoCloseable {

    public static final int DEFAULT_CAPACITY = 500;

    private static final int MIN_CAPACITY = 100;
    private static final int MAX_CAPACITY = 1000;
    private static final int PER_RECORD_BYTES = 100;

    private final RocksDBWrapper db;

    private final WriteBatch batch;

    private final WriteOptions options;

    private final int capacity;

    @Nonnegative private final long batchSize;

    /**
     * WriteBatch could be used for multi column family handles, which could happen during restoring
     * or rocksDB timer access, we set the column family handler wrapper as null in these cases.
     */
    @Nullable private final ColumnFamilyHandleWrapper columnFamilyHandleWrapper;

    public RocksDBWriteBatchWrapper(@Nonnull RocksDBWrapper rocksDB, long writeBatchSize) {
        this(rocksDB, null, null, DEFAULT_CAPACITY, writeBatchSize);
    }

    public RocksDBWriteBatchWrapper(
            @Nonnull RocksDBWrapper rocksDB,
            @Nullable ColumnFamilyHandleWrapper columnFamilyHandle,
            @Nullable WriteOptions options,
            int capacity,
            long batchSize) {
        Preconditions.checkArgument(
                capacity >= MIN_CAPACITY && capacity <= MAX_CAPACITY,
                "capacity should be between " + MIN_CAPACITY + " and " + MAX_CAPACITY);
        Preconditions.checkArgument(batchSize >= 0, "Max batch size have to be no negative.");

        this.db = rocksDB;
        this.columnFamilyHandleWrapper = columnFamilyHandle;
        this.options = options;
        this.capacity = capacity;
        this.batchSize = batchSize;
        if (this.batchSize > 0) {
            this.batch =
                    new WriteBatch(
                            (int) Math.min(this.batchSize, this.capacity * PER_RECORD_BYTES));
        } else {
            this.batch = new WriteBatch(this.capacity * PER_RECORD_BYTES);
        }
    }

    public void put(@Nonnull byte[] key, @Nonnull byte[] value) throws RocksDBException {
        Preconditions.checkNotNull(columnFamilyHandleWrapper);
        batch.put(columnFamilyHandleWrapper.getColumnFamilyHandle(), key, value);

        flushIfNeeded();
    }

    public void put(@Nonnull ColumnFamilyHandle handle, @Nonnull byte[] key, @Nonnull byte[] value)
            throws RocksDBException {

        batch.put(handle, key, value);

        flushIfNeeded();
    }

    public void remove(@Nonnull byte[] key) throws RocksDBException {
        Preconditions.checkNotNull(columnFamilyHandleWrapper);
        batch.remove(columnFamilyHandleWrapper.getColumnFamilyHandle(), key);

        flushIfNeeded();
    }

    public void remove(@Nonnull ColumnFamilyHandle handle, @Nonnull byte[] key)
            throws RocksDBException {

        batch.remove(handle, key);

        flushIfNeeded();
    }

    public void flush() throws RocksDBException {
        if (options != null) {
            db.write(columnFamilyHandleWrapper, options, batch);
        } else {
            // use the default WriteOptions, if wasn't provided.
            try (WriteOptions writeOptions = new WriteOptions()) {
                db.write(columnFamilyHandleWrapper, writeOptions, batch);
            }
        }
        batch.clear();
    }

    public WriteOptions getOptions() {
        return options;
    }

    @Override
    public void close() throws RocksDBException {
        if (batch.count() != 0) {
            flush();
        }
        IOUtils.closeQuietly(batch);
    }

    private void flushIfNeeded() throws RocksDBException {
        boolean needFlush =
                batch.count() == capacity || (batchSize > 0 && getDataSize() >= batchSize);
        if (needFlush) {
            flush();
        }
    }

    @VisibleForTesting
    long getDataSize() {
        return batch.getDataSize();
    }
}
