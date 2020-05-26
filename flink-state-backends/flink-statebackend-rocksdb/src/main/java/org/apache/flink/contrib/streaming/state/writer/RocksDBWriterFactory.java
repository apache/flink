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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
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
 * as well as specific constructors for {@link RocksDBSSTIngestWriter} and {@link
 * RocksDBWriteBatchWrapper} which each provide different capabilities.
 *
 * <p>See {@link RocksDBSSTIngestWriter} and {@link RocksDBWriteBatchWrapper} for details about the
 * writers, and when they should be used.
 */
public class RocksDBWriterFactory {

    private final WriteBatchMechanism mechanism;
    private long writeBatchSize;

    private final File tempDir;

    /**
     * Provides a factory for {@link RocksDBWriter}, including {@link RocksDBWriteBatchWrapper} and
     * {@link RocksDBSSTIngestWriter}. The factory also provides a "default" writer, determined by
     * configuration parameters, so that factory callers do not need to be concerned with the
     * implementation.
     *
     * @param mechanism The {@link WriteBatchMechanism} which determines the default writer returned
     *     by {@link #defaultPutWriter(RocksDB, Options, EnvOptions, WriteOptions)}.
     * @param writeBatchSize A parameter for {@link RocksDBWriteBatchWrapper}.
     * @param tempDir THe temporary directory to write sst files to.
     */
    public RocksDBWriterFactory(
            WriteBatchMechanism mechanism, long writeBatchSize, String tempDir) {
        this.mechanism = mechanism;
        this.writeBatchSize = writeBatchSize;
        this.tempDir = new File(tempDir);
    }

    /**
     * Provides a factory for {@link RocksDBWriter}.
     *
     * <p>This is only used in tests, where the tests pull some default configuration values but
     * provide the mechanism being tested. Tests should parameterize {@link WriteBatchMechanism}.
     */
    @VisibleForTesting
    public RocksDBWriterFactory(WriteBatchMechanism writeBatchMechanism) {
        this.mechanism = writeBatchMechanism;
        this.writeBatchSize = WRITE_BATCH_SIZE.defaultValue().getBytes();
        this.tempDir = null;
    }

    /**
     * Returns a new {@link RocksDBWriter}, using the user-configured parameters. Depending on the
     * configuration, a {@link RocksDBWriteBatchWrapper} or {@link RocksDBSSTIngestWriter} may be
     * used.
     *
     * @param db the {@link RocksDB} instance to write to.
     * @param options the {@link Options} to use when writing (only applicable for {@link
     *     RocksDBSSTIngestWriter})
     * @param envOptions the {@link EnvOptions} to use when writing (only applicable for {@link
     *     RocksDBSSTIngestWriter})
     * @param writeOptions the {@link WriteOptions} to use when writing (only applicable for {@link
     *     RocksDBWriteBatchWrapper})
     * @return a new {@link RocksDBWriter} for {@code put} writes to the database.
     */
    public RocksDBWriter defaultPutWriter(
            @Nonnull RocksDB db,
            @Nullable Options options,
            @Nullable EnvOptions envOptions,
            @Nullable WriteOptions writeOptions)
            throws IOException {
        switch (mechanism) {
            case WRITE_BATCH:
                return writeBatchWriter(db, writeOptions);
            case SST_INGEST:
                return sstIngestWriter(db, options, envOptions);
            default:
                Preconditions.checkState(false, "unexpected WriteBatchMechanism option");
                return null;
        }
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

    /**
     * Returns a new {@link RocksDBSSTIngestWriter}, using the user-configured parameters.
     *
     * @param db the {@link RocksDB} instance to write to.
     * @param options the {@link Options} to use for writing.
     * @param envOptions the {@link EnvOptions} to use for writing.
     * @return a new {@link RocksDBSSTIngestWriter} for writing to the {@code db}.
     */
    public RocksDBSSTIngestWriter sstIngestWriter(
            @Nonnull RocksDB db, @Nullable Options options, @Nullable EnvOptions envOptions)
            throws IOException {
        // @lgo: plumb through parameters.
        final int maxSstSize = 10;
        return new RocksDBSSTIngestWriter(db, maxSstSize, envOptions, options, this.tempDir);
    }

    /**
     * Returns a new {@link RocksDBSSTIngestWriter}, using the user-configured parameters.
     *
     * @param db the {@link RocksDB} instance to write to.
     * @param options the {@link Options} to use for writing.
     * @param envOptions the {@link EnvOptions} to use for writing.
     * @param tempDir the temporary directory to use when constructing sst files.
     * @return a new {@link RocksDBSSTIngestWriter} for writing to the {@code db}.
     */
    public RocksDBSSTIngestWriter sstIngestWriter(
            @Nonnull RocksDB db,
            @Nullable Options options,
            @Nullable EnvOptions envOptions,
            @Nonnull File tempDir)
            throws IOException {
        // @lgo: plumb through parameters.
        final int maxSstSize = 10;
        return new RocksDBSSTIngestWriter(db, maxSstSize, envOptions, options, tempDir);
    }

    /**
     * Sets the write batch size parameter of the factory for constructing {@link
     * RocksDBWriteBatchWrapper}.
     *
     * @param writeBatchSize to use for building {@link RocksDBWriteBatchWrapper}.
     */
    public void setWriteBatchSize(long writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }
}
