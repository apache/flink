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

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** External extension for tests that require an instance of RocksDB. */
public class RocksDBExtension implements BeforeEachCallback, AfterEachCallback {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBExtension.class);

    /** Factory for {@link DBOptions} and {@link ColumnFamilyOptions}. */
    private final RocksDBOptionsFactory optionsFactory;

    private final boolean enableStatistics;

    /** Temporary folder that provides the working directory for the RocksDB instance. */
    private TemporaryFolder temporaryFolder;

    /** The options for the RocksDB instance. */
    private DBOptions dbOptions;

    /** The options for column families created with the RocksDB instance. */
    private ColumnFamilyOptions columnFamilyOptions;

    /** The options for writes. */
    private WriteOptions writeOptions;

    /** The options for reads. */
    private ReadOptions readOptions;

    /** The RocksDB instance object. */
    private RocksDB rocksDB;

    /** List of all column families that have been created with the RocksDB instance. */
    private List<ColumnFamilyHandle> columnFamilyHandles;

    /** Wrapper for batched writes to the RocksDB instance. */
    private RocksDBWriteBatchWrapper batchWrapper;

    /** Resources to close. */
    private final ArrayList<AutoCloseable> handlesToClose = new ArrayList<>();

    public RocksDBExtension() {
        this(false);
    }

    public RocksDBExtension(boolean enableStatistics) {
        this(
                new RocksDBOptionsFactory() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        // close it before reuse the reference.
                        try {
                            currentOptions.close();
                        } catch (Exception e) {
                            LOG.error("Close previous DBOptions's instance failed.", e);
                        }

                        return new DBOptions()
                                .setMaxBackgroundJobs(4)
                                .setUseFsync(false)
                                .setMaxOpenFiles(-1)
                                .setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
                                .setStatsDumpPeriodSec(0);
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        // close it before reuse the reference.
                        try {
                            currentOptions.close();
                        } catch (Exception e) {
                            LOG.error("Close previous ColumnOptions's instance failed.", e);
                        }

                        return new ColumnFamilyOptions().optimizeForPointLookup(40960);
                    }
                },
                enableStatistics);
    }

    public RocksDBExtension(
            @Nonnull RocksDBOptionsFactory optionsFactory, boolean enableStatistics) {
        this.optionsFactory = optionsFactory;
        this.enableStatistics = enableStatistics;
    }

    public ColumnFamilyHandle getDefaultColumnFamily() {
        return columnFamilyHandles.get(0);
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    public RocksDB getRocksDB() {
        return rocksDB;
    }

    public ReadOptions getReadOptions() {
        return readOptions;
    }

    public DBOptions getDbOptions() {
        return dbOptions;
    }

    public RocksDBWriteBatchWrapper getBatchWrapper() {
        return batchWrapper;
    }

    /** Creates and returns a new column family with the given name. */
    public ColumnFamilyHandle createNewColumnFamily(String name) {
        try {
            final ColumnFamilyHandle columnFamily =
                    rocksDB.createColumnFamily(
                            new ColumnFamilyDescriptor(name.getBytes(), columnFamilyOptions));
            columnFamilyHandles.add(columnFamily);
            return columnFamily;
        } catch (Exception ex) {
            throw new FlinkRuntimeException("Could not create column family.", ex);
        }
    }

    public void before() throws Exception {
        this.temporaryFolder = new TemporaryFolder();
        this.temporaryFolder.create();
        final File rocksFolder = temporaryFolder.newFolder();
        this.dbOptions =
                optionsFactory
                        .createDBOptions(
                                new DBOptions()
                                        .setUseFsync(false)
                                        .setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
                                        .setStatsDumpPeriodSec(0),
                                handlesToClose)
                        .setCreateIfMissing(true);
        if (enableStatistics) {
            Statistics statistics = new Statistics();
            dbOptions.setStatistics(statistics);
            handlesToClose.add(statistics);
        }
        this.columnFamilyOptions =
                optionsFactory.createColumnOptions(new ColumnFamilyOptions(), handlesToClose);
        this.writeOptions = new WriteOptions();
        this.writeOptions.disableWAL();
        this.readOptions = new ReadOptions();
        this.columnFamilyHandles = new ArrayList<>(1);
        this.rocksDB =
                RocksDB.open(
                        dbOptions,
                        rocksFolder.getAbsolutePath(),
                        Collections.singletonList(
                                new ColumnFamilyDescriptor(
                                        "default".getBytes(), columnFamilyOptions)),
                        columnFamilyHandles);
        this.batchWrapper = new RocksDBWriteBatchWrapper(rocksDB, writeOptions);
    }

    public void after() throws Exception {
        // destruct in reversed order of creation.
        IOUtils.closeQuietly(this.batchWrapper);
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            IOUtils.closeQuietly(columnFamilyHandle);
        }
        IOUtils.closeQuietly(this.rocksDB);
        IOUtils.closeQuietly(this.readOptions);
        IOUtils.closeQuietly(this.writeOptions);
        IOUtils.closeQuietly(this.columnFamilyOptions);
        IOUtils.closeQuietly(this.dbOptions);
        handlesToClose.forEach(IOUtils::closeQuietly);
        temporaryFolder.delete();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        before();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        after();
    }
}
