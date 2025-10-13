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

package org.apache.flink.state.forst;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;

import org.forstdb.ColumnFamilyDescriptor;
import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.forstdb.InfoLogLevel;
import org.forstdb.ReadOptions;
import org.forstdb.RocksDB;
import org.forstdb.WriteOptions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** External extension for tests that require an instance of ForSt. */
public class ForStExtension implements BeforeEachCallback, AfterEachCallback {
    private static final Logger LOG = LoggerFactory.getLogger(ForStExtension.class);

    /** Factory for {@link DBOptions} and {@link ColumnFamilyOptions}. */
    private final ForStOptionsFactory optionsFactory;

    private final boolean enableStatistics;

    /** Temporary folder that provides the working directory for the ForSt instance. */
    private TemporaryFolder temporaryFolder;

    /** The options for the ForSt instance. */
    private DBOptions dbOptions;

    /** The options for column families created with the ForSt instance. */
    private ColumnFamilyOptions columnFamilyOptions;

    /** The options for writes. */
    private WriteOptions writeOptions;

    /** The options for reads. */
    private ReadOptions readOptions;

    /** The ForSt instance object. */
    private RocksDB db;

    private ResourceGuard resourceGuard;
    private ForStResourceContainer resourceContainer;

    /** List of all column families that have been created with the ForSt instance. */
    private List<ColumnFamilyHandle> columnFamilyHandles;

    public ForStExtension() {
        this(false);
    }

    public ForStExtension(boolean enableStatistics) {
        this(
                new ForStOptionsFactory() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions
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

    public ForStExtension(@Nonnull ForStOptionsFactory optionsFactory, boolean enableStatistics) {
        this.optionsFactory = optionsFactory;
        this.enableStatistics = enableStatistics;
    }

    public ColumnFamilyHandle getDefaultColumnFamily() {
        return columnFamilyHandles.get(0);
    }

    public RocksDB getDB() {
        return db;
    }

    public DBOptions getDbOptions() {
        return dbOptions;
    }

    public ResourceGuard getResourceGuard() {
        return resourceGuard;
    }

    public ForStResourceContainer getResourceContainer() {
        return resourceContainer;
    }

    /** Creates and returns a new column family with the given name. */
    public ColumnFamilyHandle createNewColumnFamily(String name) {
        try {
            final ColumnFamilyHandle columnFamily =
                    db.createColumnFamily(
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

        resourceGuard = new ResourceGuard();
        File localWorkingDir = TempDirUtils.newFolder(rocksFolder.toPath(), "local-working-dir");
        Path localJobPath = new Path(localWorkingDir.getAbsolutePath());
        Path localBasePath = new Path(localJobPath, "base");
        this.resourceContainer =
                new ForStResourceContainer(
                        new Configuration(),
                        optionsFactory,
                        null,
                        ForStPathContainer.ofLocal(localJobPath, localBasePath),
                        null,
                        null,
                        null,
                        enableStatistics);
        resourceContainer.prepareDirectories();

        this.dbOptions = resourceContainer.getDbOptions();

        this.columnFamilyOptions = resourceContainer.getColumnOptions();

        this.writeOptions = resourceContainer.getWriteOptions();
        this.readOptions = resourceContainer.getReadOptions();

        // Currently, ForStDB does not support mixing local-dir and remote-dir, and ForStDB will
        // concatenates the dfs directory with the local directory as working dir when using flink
        // env. We expect to directly use the dfs directory in flink env or local directory as
        // working dir. We will implement this in ForStDB later, but before that, we achieved this
        // by setting the dbPath to "/" when the dfs directory existed.
        // TODO: use localForStPath as dbPath after ForSt Support mixing local-dir and remote-dir
        ForStPathContainer pathContainer = resourceContainer.getPathContainer();
        Path instanceForStPath =
                pathContainer.getRemoteForStPath() == null
                        ? pathContainer.getLocalForStPath()
                        : new Path("/");

        this.columnFamilyHandles = new ArrayList<>(1);
        this.db =
                RocksDB.open(
                        dbOptions,
                        instanceForStPath.getPath(),
                        Collections.singletonList(
                                new ColumnFamilyDescriptor(
                                        "default".getBytes(), columnFamilyOptions)),
                        columnFamilyHandles);
    }

    public void after() {
        // destruct in reversed order of creation.
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            IOUtils.closeQuietly(columnFamilyHandle);
        }
        IOUtils.closeQuietly(this.db);
        IOUtils.closeQuietly(this.resourceContainer);
        IOUtils.closeQuietly(this.resourceGuard);
        temporaryFolder.delete();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        before();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        after();
    }
}
