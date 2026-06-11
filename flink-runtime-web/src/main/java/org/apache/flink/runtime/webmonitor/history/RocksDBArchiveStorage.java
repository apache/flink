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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StringUtils;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ArchiveStorage} implementation backed by RocksDB.
 *
 * <p>All archived data is stored as key-value pairs in a single RocksDB instance, avoiding the
 * problem of numerous small files. The key is the request path (e.g. {@code
 * /jobs/xxx/config.json}), and the value is a JSON string.
 */
public class RocksDBArchiveStorage implements ArchiveStorage<String> {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBArchiveStorage.class);

    private final RocksDB db;

    private Options dbOptions;

    private WriteOptions writeOptions;

    private final List<AutoCloseable> handlesToClose;

    /**
     * The temporary directory that holds the extracted RocksDB native library; cleaned up on {@link
     * #close()}.
     */
    private final File nativeLibDir;

    /**
     * Creates a new {@link RocksDBArchiveStorage} instance with default RocksDB options.
     *
     * @param dbPath the RocksDB database directory path
     * @throws IOException if the RocksDB database cannot be opened
     */
    public RocksDBArchiveStorage(File dbPath) throws IOException {
        this(dbPath, new Configuration());
    }

    /**
     * Creates a new {@link RocksDBArchiveStorage} instance.
     *
     * @param dbPath the RocksDB database directory path
     * @param config the configuration used to read RocksDB related options (see {@link
     *     HistoryServerOptions})
     * @throws IOException if the RocksDB native library cannot be loaded or the database cannot be
     *     opened
     */
    public RocksDBArchiveStorage(File dbPath, ReadableConfig config) throws IOException {
        checkNotNull(dbPath, "dbPath cannot be null");
        checkNotNull(config, "config cannot be null");
        this.handlesToClose = new ArrayList<>();
        this.nativeLibDir = new File(dbPath, "rocksdb-lib");

        try {
            loadRocksDBLibrary(this.nativeLibDir);
            loadRocksDBConfiguration();
            this.db = RocksDB.open(dbOptions, dbPath.getAbsolutePath());
            handlesToClose.add(db);
        } catch (Throwable t) {
            close();
            throw new IOException("Failed to initialize RocksDBArchiveStorage", t);
        }
    }

    @Override
    public boolean exists(String key) throws IOException {
        return db.keyExists(key.getBytes(UTF_8));
    }

    @Nullable
    @Override
    public String getEntry(String key) throws IOException {
        try {
            byte[] value = db.get(key.getBytes(UTF_8));
            if (value == null) {
                return null;
            }
            return new String(value, UTF_8);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get key: " + key, e);
        }
    }

    @Override
    public void putArchiveContent(String key, String value) throws IOException {
        try {
            db.put(writeOptions, key.getBytes(UTF_8), value.getBytes(UTF_8));
        } catch (RocksDBException e) {
            throw new IOException("Failed to put key: " + key, e);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        try {
            db.delete(writeOptions, key.getBytes(UTF_8));
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete key: " + key, e);
        }
    }

    @Override
    public void deleteEntriesByPrefix(String keyPrefix) throws IOException {
        if (StringUtils.isNullOrWhitespaceOnly(keyPrefix)) {
            return;
        }

        // Delete all keys that start with the given prefix
        byte[] startKey = keyPrefix.getBytes(UTF_8);
        byte[] endKey = computeNextKey(startKey);
        if (endKey == null) {
            throw new IOException("Failed to compute next key for prefix: " + keyPrefix);
        }

        try {
            db.deleteRange(writeOptions, startKey, endKey);
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete prefix: " + keyPrefix, e);
        }
    }

    @Override
    public List<String> getEntriesByPrefix(String prefix) throws IOException {
        List<String> result = new ArrayList<>();
        if (StringUtils.isNullOrWhitespaceOnly(prefix)) {
            return result;
        }

        byte[] prefixBytes = prefix.getBytes(UTF_8);
        byte[] upperBound = computeNextKey(prefixBytes);
        if (upperBound == null) {
            throw new IOException("Failed to compute next key for prefix: " + prefix);
        }

        try (Slice upperBoundSlice = new Slice(upperBound);
                ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperBoundSlice);
                RocksIterator iterator = db.newIterator(readOptions)) {
            for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
                result.add(new String(iterator.value(), UTF_8));
            }
            try {
                iterator.status();
            } catch (RocksDBException e) {
                throw new IOException(
                        String.format("Error iterating over RocksDB, prefix: %s", prefix), e);
            }
        }

        return result;
    }

    /**
     * Compute the next key in lexicographic order.
     *
     * @param key the current key
     * @return the next key, or null if no such key exists
     */
    @Nullable
    private static byte[] computeNextKey(byte[] key) {
        // Find the last byte that is not 0xFF; everything after it can be dropped.
        int lastIncrementableIndex = -1;
        for (int i = key.length - 1; i >= 0; i--) {
            if ((key[i] & 0xFF) != 0xFF) {
                lastIncrementableIndex = i;
                break;
            }
        }
        if (lastIncrementableIndex < 0) {
            // All bytes are 0xFF.
            return null;
        }
        byte[] nextKey = new byte[lastIncrementableIndex + 1];
        System.arraycopy(key, 0, nextKey, 0, lastIncrementableIndex + 1);
        nextKey[lastIncrementableIndex]++;
        return nextKey;
    }

    @Override
    public String readArchiveContent(String entry) throws IOException {
        return entry;
    }

    @Override
    public void close() {
        Collections.reverse(handlesToClose);
        handlesToClose.forEach(IOUtils::closeQuietly);
        handlesToClose.clear();

        if (nativeLibDir != null) {
            deleteDirectoryQuietly(nativeLibDir);
        }
    }

    /**
     * Loads the RocksDB native library. The default configuration is suitable for most use cases.
     * Custom options can be supported in the future if needed.
     */
    private void loadRocksDBConfiguration() {
        // Use the full (non-block-based) BloomFilter format with 10 bits/key, which is the
        // RocksDB-recommended default and yields ~1% false positive rate.
        // https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#full-filters-new-format
        BloomFilter bloomFilter = new BloomFilter(10.0D, false);
        handlesToClose.add(bloomFilter);

        BlockBasedTableConfig tableFormatConfig =
                new BlockBasedTableConfig()
                        .setFilterPolicy(bloomFilter)
                        .setEnableIndexCompression(false)
                        .setIndexBlockRestartInterval(8)
                        .setFormatVersion(5);

        this.dbOptions =
                new Options()
                        .setCreateIfMissing(true)
                        .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                        .setCompressionType(CompressionType.LZ4_COMPRESSION)
                        .setTableFormatConfig(tableFormatConfig);
        handlesToClose.add(dbOptions);

        this.writeOptions = new WriteOptions().setDisableWAL(true);
        handlesToClose.add(writeOptions);
    }

    private static void loadRocksDBLibrary(File libDir) throws IOException {
        try {
            Files.createDirectories(libDir.toPath());
            LOG.info("Try to load RocksDB native library to '{}'.", libDir.getAbsolutePath());
            NativeLibraryLoader.getInstance().loadLibrary(libDir.getAbsolutePath());
            // Sanity check that the library is fully usable.
            RocksDB.loadLibrary();
            LOG.info(
                    "Successfully loaded RocksDB native library to '{}'.",
                    libDir.getAbsolutePath());
        } catch (Throwable t) {
            LOG.warn("Failed to load RocksDB native library to '{}'.", libDir.getAbsolutePath(), t);
            deleteDirectoryQuietly(libDir);
            throw t;
        }
    }

    private static void deleteDirectoryQuietly(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (Throwable t) {
            LOG.warn("Failed to delete directory: {}", dir.getAbsolutePath(), t);
        }
    }
}
