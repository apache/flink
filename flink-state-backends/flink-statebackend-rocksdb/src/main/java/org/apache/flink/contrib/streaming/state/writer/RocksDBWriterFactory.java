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

package org.apache.flink.contrib.streaming.state.writer;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.WRITE_BATCH_SIZE;

/**
 * {@link RocksDBWriterFactory} creates writers for {@link RocksDB}. It is initialized from an
 * application's configuration, and provides a default {@link RocksDBWriter} (for Put operations),
 * as well as specific constructors {@link RocksDBWriteBatchWrapper}.
 */
public class RocksDBWriterFactory {

    private final long writeBatchSize;

    private final File tempDir = null;

    /** Provides a factory for {@link RocksDBWriter}. */
    public RocksDBWriterFactory(long writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }

    // @lgo: fixme used for tests that pull all default configured values.
    public RocksDBWriterFactory() {
        this.writeBatchSize = WRITE_BATCH_SIZE.defaultValue().getBytes();
    }

    /**
     * Returns a new {@link RocksDBWriter}, using the user-configured parameters.
     *
     * @param db the {@link RocksDB} instance to write to.
     * @param writeOptions the {@link WriteOptions} to use when writing (only applicable for {@link
     *     RocksDBWriteBatchWrapper})
     * @return a new {@link RocksDBWriter} for {@code put} writes to the database.
     */
    public RocksDBWriter defaultPutWriter(@Nonnull RocksDB db, @Nullable WriteOptions writeOptions)
            throws IOException {
        return writeBatchWriter(db, writeOptions);
    }

    /**
     * Returns a new {@link RocksDBWriteBatchWrapper}, using the user-configured parameters. The
     * type {@link RocksDBWriteBatchWrapper} is exposed directly because it provides additional
     * {@link RocksDB} write operations.
     *
     * @param db the {@link RocksDB} instance to write to.
     * @return a new {@link RocksDBWriteBatchWrapper} for writing to the {@code db}.
     */
    public RocksDBWriteBatchWrapper writeBatchWriter(@Nonnull RocksDB db) {
        return new RocksDBWriteBatchWrapper(db, writeBatchSize);
    }

    /**
     * Returns a new {@link RocksDBWriteBatchWrapper}.
     *
     * @param db the {@link RocksDB} instance to write to.
     * @param writeOptions the {@link WriteOptions} to use for writing.
     * @return a new {@link RocksDBWriteBatchWrapper} for writing to the {@code db}.
     * @see #writeBatchWriter(RocksDB, WriteOptions)
     */
    public RocksDBWriteBatchWrapper writeBatchWriter(
            @Nonnull RocksDB db, @Nullable WriteOptions writeOptions) {
        return new RocksDBWriteBatchWrapper(db, writeOptions, writeBatchSize);
    }
}
