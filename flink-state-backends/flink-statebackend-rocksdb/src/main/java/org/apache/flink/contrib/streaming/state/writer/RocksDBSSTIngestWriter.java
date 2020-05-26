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

import org.apache.flink.util.AbstractID;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * {@link RocksDBSSTIngestWriter} implements {@link RocksDBWriter}, providing writes by creating sst
 * files and instructing {@link RocksDB} to ingest them (via {@link
 * RocksDB#ingestExternalFile(ColumnFamilyHandle, List, IngestExternalFileOptions)}). It supports
 * writing to multiple {@link ColumnFamilyHandle}, assuming the writes within a {@link
 * ColumnFamilyHandle} are ordered.
 *
 * <p>IMPORTANT: This class is not thread safe.
 *
 * <p>It uses {@link RocksDBSSTWriter} for creating the sst file of a {@link ColumnFamilyHandle}.
 *
 * <p>{@link RocksDBSSTIngestWriter} is for batch write operations. It produces and ingests
 * individual files throughout put operations, so it will not atomically commit data.
 */
public class RocksDBSSTIngestWriter implements RocksDBWriter {

    /**
     * {@code INGEST_EXTERNAL_FILE_OPTIONS} are the {@link IngestExternalFileOptions} provided to
     * RocksDB when calling {@link RocksDB#ingestExternalFile(ColumnFamilyHandle, List,
     * IngestExternalFileOptions)}. We only use one set of options, so we use a singleton that is
     * not closed.
     */
    // @lgo: because flink is using the JNI 5.14, the API for IngestExternalFileOptions is pretty
    // bad and the builder-pattern does not exist.
    private static final IngestExternalFileOptions INGEST_EXTERNAL_FILE_OPTIONS =
            new IngestExternalFileOptions(
                    // Move files rather than copying them. Because we are generating
                    // one-off files for import, it does not matter if the are moved and
                    // import fails.
                    //
                    // @lgo: fixme ensure this does not cause stray RocksDB sst files on a host when
                    // ingestion
                    // fails.
                    /* moveFiles */ true,

                    // Set to the default (true).
                    /* snapshotConsistency */ true,

                    // Set to the default (true).
                    /* allowGlobalSeqNo */ true,

                    // By disallowing a blocking flush, ingestion loudly fails when it
                    // has keys that overlap the RocksDB memtable.
                    /* allowBlockingFlush */ false);

    /** {@code maxSstSize} is the maximum size of an sst file before flushing it. */
    private final long maxSstSize;

    /**
     * {@code envOptions} are the {@link EnvOptions} provided to the underlying {@link
     * RocksDBSSTWriter}.
     */
    // @lgo: plumb through the options into the constructor
    // and set sane options.
    private final EnvOptions envOptions;

    /**
     * {@code options} are the {@link Options} provided to the underlying {@link RocksDBSSTWriter}.
     */
    // @lgo: plumb through the options into the constructor
    // and set sane options.
    private final Options options;

    /** {@code db} is the active RocksDB database to write files to. */
    private final RocksDB db;

    /**
     * {@code sstFileWriters} contains the currently open {@link RocksDBSSTWriter} for each {@link
     * ColumnFamilyHandle}.
     */
    private final HashMap<Integer, RocksDBSSTWriter> columnFamilyWriters = new HashMap<>();

    /**
     * {@code sstFileSizes} contains the current byte-sizes for the currently opened {@link
     * RocksDBSSTWriter}, for each {@link ColumnFamilyHandle}.
     */
    private final HashMap<Integer, Long> sstFileSizes = new HashMap<>();

    /**
     * {@code ingestionTempDir} is the directory used to write temporary sst files before ingesting
     * them into {@link RocksDB}.
     */
    private final File ingestionTempDir;

    public RocksDBSSTIngestWriter(
            @Nonnull RocksDB rocksDB,
            @Nonnegative long maxSstSize,
            @Nullable EnvOptions envOptions,
            @Nullable Options options,
            @Nullable File tempDir)
            throws IOException {
        this.db = rocksDB;
        this.maxSstSize = maxSstSize;
        this.envOptions = envOptions;
        this.options = options;

        // Set up a temporary directory for writing generated SST files.
        // Either, use the provided temporary directory (such as during tests), or create a new one.
        if (tempDir != null) {
            this.ingestionTempDir = tempDir;
        } else {
            // @lgo: clean the temporary folder up. This may not actually be bad considering
            // ingestion will move the temporary sst files out.
            this.ingestionTempDir = Files.createTempDirectory("rocksdb-sst-writer-temp-").toFile();
        }
        maxSstSize = 0;
    }

    public void put(
            @Nonnull ColumnFamilyHandle columnFamilyHandle,
            @Nonnull byte[] key,
            @Nonnull byte[] value)
            throws RocksDBException, IOException {
        // Get the sst writer for the column family.
        RocksDBSSTWriter writer = ensureSSTableWriter(columnFamilyHandle);
        // Insert the k/v.
        writer.put(key, value);
        // Record the byte-size for the written key and value.
        long currentSize = sstFileSizes.getOrDefault(columnFamilyHandle.getID(), 0L);
        currentSize += key.length + value.length;
        sstFileSizes.put(columnFamilyHandle.getID(), currentSize);
        // Flush the sst and ingest it, if it needs to be flushed.
        flushIfNeeded(columnFamilyHandle, writer);
    }

    private RocksDBSSTWriter ensureSSTableWriter(@Nonnull ColumnFamilyHandle columnFamilyHandle)
            throws RocksDBException, IOException {
        // Return an existing writer if there is one, Otherwise prepare a new one.
        if (columnFamilyWriters.containsKey(columnFamilyHandle.getID())) {
            return columnFamilyWriters.get(columnFamilyHandle.getID());
        }

        // Create a new sst file in the temporary folder.
        final String sstFileName = "ingest_" + new AbstractID() + ".sst";
        File sstFile = new File(ingestionTempDir, sstFileName);

        // Initialize the sst writer.
        RocksDBSSTWriter writer =
                new RocksDBSSTWriter(envOptions, options, columnFamilyHandle, sstFile);

        // Store the new writer for the column family.
        columnFamilyWriters.put(columnFamilyHandle.getID(), writer);

        return writer;
    }

    /**
     * Flushes the data for a particular {@link RocksDBSSTWriter} into the {@link RocksDB} instance.
     * After flushing, the {@link ColumnFamilyHandle} will not have an active {@link
     * RocksDBSSTWriter}, and it will need to be initialized by {@link
     * #ensureSSTableWriter(ColumnFamilyHandle)}.
     *
     * @throws RocksDBException
     */
    private void flushAndCloseWriter(@Nonnull RocksDBSSTWriter writer) throws RocksDBException {
        // Finish the sst writer.
        writer.finish();

        // Instruct RocksDB to ingest the sst files. Because all of the ingested files are
        // for different column families and the JNI SstFileWriter does not allow setting the
        // ColumnFamilyHeader when writing the file, need to call ingestExternalFile for one
        // sst file at a time.
        //
        // Because the IngestExternalFileOptions specifies to move the sst file, we do not need
        // to clean up the written file.
        List<String> files = Collections.singletonList(writer.getFile().getAbsolutePath());
        db.ingestExternalFile(writer.getColumnFamilyHandle(), files, INGEST_EXTERNAL_FILE_OPTIONS);

        // Close the writer and remove it from the list of open writers.
        writer.close();
    }

    /**
     * Flushes all in-progress sst and ingests them.
     *
     * @throws RocksDBException
     */
    public void flush() throws RocksDBException {
        for (RocksDBSSTWriter writer : columnFamilyWriters.values()) {
            flushAndCloseWriter(writer);
        }
        columnFamilyWriters.clear();
    }

    @Override
    public void close() throws RocksDBException {
        flush();
        IOUtils.closeAllQuietly(columnFamilyWriters.values());
    }

    /**
     * Conditionally flushes the data for a particular {@link ColumnFamilyHandle} into the {@link
     * RocksDB} instance, if needed. The data will only be flushed if the in-progress {@link
     * RocksDBSSTWriter} exceeds the thresholds for flushing.
     *
     * @param handle to conditionally flush and ingest.
     * @throws RocksDBException
     */
    private void flushIfNeeded(ColumnFamilyHandle handle, RocksDBSSTWriter writer)
            throws RocksDBException {
        // @lgo: fixme this may not be necessary.
        // - sst writer automatically invalidates the pages for the file writer
        //   every 1mb.
        // https://github.com/facebook/rocksdb/blob/096beb787eed5837a2e38a9681ee834e9268c881/table/sst_file_writer.cc#L102
        // - the underlying block builder will flush based on the policy RocksDB
        //   was set up with:
        // https://github.com/facebook/rocksdb/blob/096beb787eed5837a2e38a9681ee834e9268c881/table/block_based/block_based_table_builder.cc#L773
        // - based on table_options.block_size (and block_size_deviation), the FS automatically
        // flushes.
        //
        // I could not actually find where/how this is configured.
        return;
        //		if (sstFileSizes.getOrDefault(handle.getID(), 0L) > maxSstSize) {
        //			flushAndCloseWriter(writer);
        //			columnFamilyWriters.remove(writer.getColumnFamilyHandle().getID());
        //			sstFileSizes.remove(writer.getColumnFamilyHandle().getID());
        //		}
    }
}
