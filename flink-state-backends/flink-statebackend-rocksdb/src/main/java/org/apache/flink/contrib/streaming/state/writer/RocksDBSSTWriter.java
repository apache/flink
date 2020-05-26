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

import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

/**
 * {@link RocksDBSSTWriter} provides a way to write and RocksDB's SST files. It only supports
 * writing k/v for a single {@link ColumnFamilyHandle}. If you are looking to write multiple {@link
 * ColumnFamilyHandle} and then ingest them into {@link RocksDB}, see {@link
 * RocksDBSSTIngestWriter}.
 *
 * <p>IMPORTANT: This class is not thread safe.
 */
class RocksDBSSTWriter implements AutoCloseable {

    // @lgo: use better defaults for the options.

    /**
     * {@code DEFAULT_OPTIONS} provides the default RocksDB {@link Options} for {@link
     * SstFileWriter}. It is a singleton so it does not need to be closed.
     */
    private static final Options DEFAULT_OPTIONS = new Options();

    /**
     * {@code DEFAULT_ENV_OPTIONS} provides the default RocksDB {@link EnvOptions} for {@link
     * SstFileWriter}. It is a singleton so it does not need to be closed.
     */
    private static final EnvOptions DEFAULT_ENV_OPTIONS = new EnvOptions();

    /**
     * {@code handle} is the {@link ColumnFamilyHandle} that {@code writer} is using for inserts.
     */
    private final ColumnFamilyHandle handle;

    /** {@code writer} is the active {@link SstFileWriter} that k/v are written to. */
    private final SstFileWriter writer;

    /** {@code file} is the file the {@code writer} is using. */
    private final File file;

    /** {@code finished} indicates whether the {@link RocksDBSSTWriter} has been finished. */
    private boolean finished = false;

    /**
     * Initializes a {@link RocksDBSSTWriter}, for writing SST files.
     *
     * @param envOptions to provide to {@link SstFileWriter}.
     * @param options to provide to {@link SstFileWriter}.
     * @param handle to use for writing with {@link SstFileWriter}.
     * @param file to write to.
     * @throws IOException if {@code file} does not exist.
     */
    public RocksDBSSTWriter(
            @Nullable EnvOptions envOptions,
            @Nullable Options options,
            @Nonnull ColumnFamilyHandle handle,
            @Nonnull File file)
            throws IOException, RocksDBException {
        if (envOptions == null) {
            envOptions = DEFAULT_ENV_OPTIONS;
        }

        if (options == null) {
            options = DEFAULT_OPTIONS;
        }

        // Ensure the file is valid for writing.
        if (file.exists()) {
            throw new IOException(
                    "File provided for sst "
                            + "writing \""
                            + file.getAbsolutePath()
                            + "\" already exists.");
        }

        if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new IOException(
                    "File provided for sst writing \""
                            + file.getAbsolutePath()
                            + "\" could not be used. The parent directory does not exist and could not be created.");
        }

        this.file = file;

        this.writer = new SstFileWriter(envOptions, options);
        this.writer.open(file.getAbsolutePath());

        // Because the rocks jni API does not support initializing the SstFileWriter with a column
        // family handle, we just retain it on the writer class.
        this.handle = handle;
    }

    public void put(@Nonnull byte[] key, @Nonnull byte[] value) throws RocksDBException {
        Preconditions.checkState(
                !finished,
                "attempted to `put` on RocksDBSSTWriter after calling `finish` (or closing)");
        writer.put(key, value);
    }

    /**
     * Finishes the underlying sst and closes the writer.
     *
     * @throws RocksDBException
     */
    public void finish() throws RocksDBException {
        if (finished) {
            return;
        }
        // Finish the sst writer.
        writer.finish();
        finished = true;
    }

    @Override
    public void close() throws RocksDBException {
        IOUtils.closeQuietly(writer);
    }

    /** @return the {@link File} of the sst being written. */
    public File getFile() {
        return file;
    }

    /** @return the {@link ColumnFamilyHandle} for the sst file. */
    public ColumnFamilyHandle getColumnFamilyHandle() {
        return handle;
    }
}
