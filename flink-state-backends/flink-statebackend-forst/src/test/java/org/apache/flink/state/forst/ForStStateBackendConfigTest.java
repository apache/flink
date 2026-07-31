/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageAccess;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FileUtils;

import org.apache.commons.lang3.RandomUtils;
import org.forstdb.BlockBasedTableConfig;
import org.forstdb.BloomFilter;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.CompactionStyle;
import org.forstdb.CompressionType;
import org.forstdb.DBOptions;
import org.forstdb.FlushOptions;
import org.forstdb.InfoLogLevel;
import org.forstdb.util.SizeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.state.forst.ForStStateBackend.LOCAL_DIR_AS_PRIMARY_SHORTCUT;
import static org.apache.flink.state.forst.ForStTestUtils.createKeyedStateBackend;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Tests for configuring the ForSt State Backend. */
class ForStStateBackendConfigTest {

    @TempDir java.nio.file.Path tempFolder;

    // ------------------------------------------------------------------------
    //  default values
    // ------------------------------------------------------------------------

    @Test
    void testDefaultDbLogDir() throws Exception {
        final ForStStateBackend backend = new ForStStateBackend();
        final File logFile = File.createTempFile(getClass().getSimpleName() + "-", ".log");
        // set the environment variable 'log.file' with the Flink log file location
        System.setProperty("log.file", logFile.getPath());
        try (ForStResourceContainer container =
                backend.createOptionsAndResourceContainer(new Path(tempFolder.toString()))) {
            assertThat(container.getDbOptions().infoLogLevel())
                    .isEqualTo(ForStConfigurableOptions.LOG_LEVEL.defaultValue());
            assertThat(container.getDbOptions().dbLogDir()).isEqualTo(logFile.getParent());
        } finally {
            logFile.delete();
        }

        StringBuilder longInstanceBasePath =
                new StringBuilder(TempDirUtils.newFolder(tempFolder).getAbsolutePath());
        while (longInstanceBasePath.length() < 255) {
            longInstanceBasePath.append("/append-for-long-path");
        }
        try (ForStResourceContainer container =
                backend.createOptionsAndResourceContainer(
                        new Path(longInstanceBasePath.toString()))) {
            assertThat(container.getDbOptions().dbLogDir()).isEmpty();
        } finally {
            logFile.delete();
        }
    }

    // ------------------------------------------------------------------------
    //  ForSt local file directory
    // ------------------------------------------------------------------------

    /** This test checks the behavior for basic setting of local DB directories. */
    @Test
    void testSetDbPath() throws Exception {
        final ForStStateBackend forStStateBackend = new ForStStateBackend();

        final String testDir1 = TempDirUtils.newFolder(tempFolder).getAbsolutePath();
        final String testDir2 = TempDirUtils.newFolder(tempFolder).getAbsolutePath();

        assertThat(forStStateBackend.getLocalDbStoragePaths()).isNull();

        forStStateBackend.setLocalDbStoragePath(testDir1);
        assertThat(forStStateBackend.getLocalDbStoragePaths()).containsExactly(testDir1);

        forStStateBackend.setLocalDbStoragePath(null);
        assertThat(forStStateBackend.getLocalDbStoragePaths()).isNull();

        forStStateBackend.setLocalDbStoragePaths(testDir1, testDir2);
        assertThat(forStStateBackend.getLocalDbStoragePaths()).containsExactly(testDir1, testDir2);

        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        final ForStKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(forStStateBackend, env, IntSerializer.INSTANCE);

        try {
            Path instanceBasePath = keyedBackend.getLocalBasePath();
            assertThat(instanceBasePath.getPath())
                    .satisfiesAnyOf(
                            p -> assertThat(p).startsWith(testDir1),
                            p -> assertThat(p).startsWith(testDir2));

            //noinspection NullArgumentToVariableArgMethod
            forStStateBackend.setLocalDbStoragePaths(null);
            assertThat(forStStateBackend.getLocalDbStoragePaths()).isNull();
        } finally {
            keyedBackend.dispose();
            keyedBackend.close();
            env.close();
        }
    }

    @Test
    void testConfigureForStCompressionPerLevel() throws Exception {
        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        CompressionType[] compressionTypes = {
            CompressionType.NO_COMPRESSION, CompressionType.SNAPPY_COMPRESSION
        };
        Configuration conf = new Configuration();
        conf.set(
                ForStConfigurableOptions.COMPRESSION_PER_LEVEL,
                new ArrayList<>(Arrays.asList(compressionTypes)));

        forStStateBackend =
                forStStateBackend.configure(conf, Thread.currentThread().getContextClassLoader());

        ForStResourceContainer resourceContainer =
                forStStateBackend.createOptionsAndResourceContainer(
                        new Path(TempDirUtils.newFile(tempFolder).getAbsolutePath()));
        ColumnFamilyOptions columnFamilyOptions = resourceContainer.getColumnOptions();
        assertThat(columnFamilyOptions.compressionPerLevel().toArray()).isEqualTo(compressionTypes);

        resourceContainer.close();
        env.close();
    }

    @Test
    void testStoragePathWithFilePrefix() throws Exception {
        final File folder = TempDirUtils.newFolder(tempFolder);
        final String dbStoragePath = new Path(folder.toURI().toString()).toString();

        assertThat(dbStoragePath).startsWith("file:");

        testLocalDbPaths(dbStoragePath, folder);
    }

    @Test
    void testWithDefaultFsSchemeNoStoragePath() throws Exception {
        try {
            // set the default file system scheme
            Configuration config = new Configuration();
            config.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "file:///mydomain.com:8020/flink");
            FileSystem.initialize(config);
            testLocalDbPaths(null, tempFolder.toFile());
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }

    @Test
    void testWithDefaultFsSchemeAbsoluteStoragePath() throws Exception {
        final File folder = TempDirUtils.newFolder(tempFolder);
        final String dbStoragePath = folder.getAbsolutePath();

        try {
            // set the default file system scheme
            Configuration config = new Configuration();
            config.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "file:///mydomain.com:8020/flink");
            FileSystem.initialize(config);

            testLocalDbPaths(dbStoragePath, folder);
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }

    private void testLocalDbPaths(String configuredPath, File expectedPath) throws Exception {
        final ForStStateBackend forStBackend = new ForStStateBackend();
        forStBackend.setLocalDbStoragePath(configuredPath);

        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        ForStKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(forStBackend, env, IntSerializer.INSTANCE);

        try {
            Path instanceBasePath = keyedBackend.getLocalBasePath();
            assertThat(instanceBasePath.getPath()).startsWith(expectedPath.getAbsolutePath());

            //noinspection NullArgumentToVariableArgMethod
            forStBackend.setLocalDbStoragePaths(null);
            assertThat(forStBackend.getLocalDbStoragePaths()).isNull();
        } finally {
            keyedBackend.dispose();
            keyedBackend.close();
            env.close();
        }
    }

    /** Validates that empty arguments for the local DB path are invalid. */
    @Test
    void testSetEmptyPaths() {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        assertThatThrownBy(forStStateBackend::setLocalDbStoragePaths)
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Validates that schemes other than 'file:/' are not allowed. */
    @Test
    void testNonFileSchemePath() {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        assertThatThrownBy(
                        () ->
                                forStStateBackend.setLocalDbStoragePath(
                                        "hdfs:///some/path/to/perdition"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDbPathRelativePaths() {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        assertThatThrownBy(() -> forStStateBackend.setLocalDbStoragePath("relative/path"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Timeout(value = 60)
    void testCleanRelocatedDbLogs() throws Exception {
        final File folder = TempDirUtils.newFolder(tempFolder);
        final File relocatedDBLogDir = TempDirUtils.newFolder(tempFolder, "db_logs");
        final File logFile = new File(relocatedDBLogDir, "taskManager.log");
        Files.createFile(logFile.toPath());
        System.setProperty("log.file", logFile.getAbsolutePath());

        Configuration conf = new Configuration();
        conf.set(ForStConfigurableOptions.LOG_LEVEL, InfoLogLevel.DEBUG_LEVEL);
        conf.set(ForStConfigurableOptions.LOG_FILE_NUM, 4);
        conf.set(ForStConfigurableOptions.LOG_MAX_FILE_SIZE, MemorySize.parse("1kb"));
        conf.set(ForStOptions.PRIMARY_DIRECTORY, LOCAL_DIR_AS_PRIMARY_SHORTCUT);
        final ForStStateBackend forStBackend =
                new ForStStateBackend().configure(conf, getClass().getClassLoader());
        final String dbStoragePath = new Path(folder.toURI().toString()).toString();
        forStBackend.setLocalDbStoragePath(dbStoragePath);

        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        ForStKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(forStBackend, env, IntSerializer.INSTANCE);

        Path localBasePath = keyedBackend.getLocalBasePath();
        Path localForStPath = new Path(localBasePath, "db");

        // avoid tests without relocate.
        assumeTrue(localForStPath.getPath().length() <= 255 - "_LOG".length());

        java.nio.file.Path[] relocatedDbLogs;
        try {
            relocatedDbLogs = FileUtils.listDirectory(relocatedDBLogDir.toPath());
            while (relocatedDbLogs.length <= 2) {
                // If the default number of log files in ForSt is not enough, add more logs.
                try (FlushOptions flushOptions = new FlushOptions()) {
                    keyedBackend.db.put(RandomUtils.nextBytes(32), RandomUtils.nextBytes(512));
                    keyedBackend.db.flush(flushOptions);
                }
                relocatedDbLogs = FileUtils.listDirectory(relocatedDBLogDir.toPath());
            }
        } finally {
            keyedBackend.dispose();
            keyedBackend.close();
            env.close();
        }

        relocatedDbLogs = FileUtils.listDirectory(relocatedDBLogDir.toPath());
        assertThat(relocatedDbLogs).hasSize(1);
        assertThat(relocatedDbLogs[0].toFile().getName()).isEqualTo("taskManager.log");
    }

    // ------------------------------------------------------------------------
    //  ForSt local file automatic from temp directories
    // ------------------------------------------------------------------------

    /**
     * This tests whether the ForSt backends uses the temp directories that are provided from the
     * {@link Environment} when no db storage path is set.
     */
    @Test
    void testUseTempDirectories() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();

        File dir1 = TempDirUtils.newFolder(tempFolder);

        assertThat(forStStateBackend.getLocalDbStoragePaths()).isNull();

        final MockEnvironment env = getMockEnvironment(dir1);
        JobID jobID = env.getJobID();
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
        TaskKvStateRegistry kvStateRegistry = env.getTaskKvStateRegistry();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        ForStKeyedStateBackend<Integer> keyedBackend =
                forStStateBackend.createAsyncKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                jobID,
                                "test_op",
                                IntSerializer.INSTANCE,
                                1,
                                keyGroupRange,
                                kvStateRegistry,
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                cancelStreamRegistry));

        try {
            Path instanceBasePath = keyedBackend.getLocalBasePath();
            assertThat(instanceBasePath.getPath()).startsWith(dir1.getAbsolutePath());
        } finally {
            keyedBackend.dispose();
            keyedBackend.close();
            env.close();
        }
    }

    // ------------------------------------------------------------------------
    //  ForSt local file directory initialization
    // ------------------------------------------------------------------------

    @Test
    void testFailWhenNoLocalStorageDir() throws Exception {
        final File targetDir = TempDirUtils.newFolder(tempFolder);
        assumeTrue(targetDir.setWritable(false, false), "Cannot mark directory non-writable");

        ForStStateBackend forStStateBackend = new ForStStateBackend();

        try (MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder))) {
            forStStateBackend.setLocalDbStoragePath(targetDir.getAbsolutePath());

            boolean hasFailure = false;
            try {
                JobID jobID = env.getJobID();
                KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
                TaskKvStateRegistry kvStateRegistry =
                        new KvStateRegistry().createTaskRegistry(env.getJobID(), new JobVertexID());
                CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
                forStStateBackend.createAsyncKeyedStateBackend(
                        new KeyedStateBackendParametersImpl<>(
                                env,
                                jobID,
                                "foobar",
                                IntSerializer.INSTANCE,
                                1,
                                keyGroupRange,
                                kvStateRegistry,
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                cancelStreamRegistry));
            } catch (Exception e) {
                assertThat(e.getMessage())
                        .contains("No local storage directories available")
                        .contains(targetDir.getAbsolutePath());
                hasFailure = true;
            }
            assertThat(hasFailure)
                    .as("We must see a failure because no storaged directory is feasible.")
                    .isTrue();
        } finally {
            //noinspection ResultOfMethodCallIgnored
            targetDir.setWritable(true, false);
        }
    }

    @Test
    void testContinueOnSomeDbDirectoriesMissing() throws Exception {
        final File targetDir1 = TempDirUtils.newFolder(tempFolder);
        final File targetDir2 = TempDirUtils.newFolder(tempFolder);
        assumeTrue(targetDir1.setWritable(false, false), "Cannot mark directory non-writable");

        ForStStateBackend forStStateBackend = new ForStStateBackend();

        try (MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder))) {
            forStStateBackend.setLocalDbStoragePaths(
                    targetDir1.getAbsolutePath(), targetDir2.getAbsolutePath());

            try {
                JobID jobID = env.getJobID();
                KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
                TaskKvStateRegistry kvStateRegistry =
                        new KvStateRegistry().createTaskRegistry(env.getJobID(), new JobVertexID());
                CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
                ForStKeyedStateBackend<Integer> keyedStateBackend =
                        forStStateBackend.createAsyncKeyedStateBackend(
                                new KeyedStateBackendParametersImpl<>(
                                        env,
                                        jobID,
                                        "foobar",
                                        IntSerializer.INSTANCE,
                                        1,
                                        keyGroupRange,
                                        kvStateRegistry,
                                        TtlTimeProvider.DEFAULT,
                                        new UnregisteredMetricsGroup(),
                                        Collections.emptyList(),
                                        cancelStreamRegistry));

                keyedStateBackend.dispose();
                keyedStateBackend.close();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Backend initialization failed even though some paths were available");
            }
        } finally {
            //noinspection ResultOfMethodCallIgnored
            targetDir1.setWritable(true, false);
        }
    }

    // ------------------------------------------------------------------------
    //  ForSt Options
    // ------------------------------------------------------------------------
    @Test
    void testConfigurableOptionsFromConfig() throws Exception {
        Configuration configuration = new Configuration();

        // verify illegal configuration
        {
            verifyIllegalArgument(ForStConfigurableOptions.MAX_BACKGROUND_THREADS, "-1");
            verifyIllegalArgument(ForStConfigurableOptions.LOG_LEVEL, "DEBUG");
            verifyIllegalArgument(ForStConfigurableOptions.LOG_DIR, "tmp/rocksdb-logs/");
            verifyIllegalArgument(ForStConfigurableOptions.LOG_DIR, "");
            verifyIllegalArgument(ForStConfigurableOptions.LOG_FILE_NUM, "0");
            verifyIllegalArgument(ForStConfigurableOptions.LOG_FILE_NUM, "-1");
            verifyIllegalArgument(ForStConfigurableOptions.LOG_MAX_FILE_SIZE, "-1KB");
            verifyIllegalArgument(ForStConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, "-1");
            verifyIllegalArgument(ForStConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE, "-1");

            verifyIllegalArgument(ForStConfigurableOptions.TARGET_FILE_SIZE_BASE, "0KB");
            verifyIllegalArgument(ForStConfigurableOptions.MAX_SIZE_LEVEL_BASE, "1BB");
            verifyIllegalArgument(ForStConfigurableOptions.WRITE_BUFFER_SIZE, "-1KB");
            verifyIllegalArgument(ForStConfigurableOptions.BLOCK_SIZE, "0MB");
            verifyIllegalArgument(ForStConfigurableOptions.METADATA_BLOCK_SIZE, "0MB");
            verifyIllegalArgument(ForStConfigurableOptions.BLOCK_CACHE_SIZE, "0");

            verifyIllegalArgument(ForStConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, "1");

            verifyIllegalArgument(ForStConfigurableOptions.COMPACTION_STYLE, "LEV");
            verifyIllegalArgument(ForStConfigurableOptions.COMPRESSION_PER_LEVEL, "SNAP");
            verifyIllegalArgument(ForStConfigurableOptions.USE_BLOOM_FILTER, "NO");
            verifyIllegalArgument(ForStConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE, "YES");
            verifyIllegalArgument(
                    ForStConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME, "-1s");
            verifyIllegalArgument(
                    ForStConfigurableOptions.COMPACT_FILTER_QUERY_TIME_AFTER_NUM_ENTRIES, "1.1");
        }

        // verify legal configuration
        {
            configuration.setString(ForStConfigurableOptions.LOG_LEVEL.key(), "DEBUG_LEVEL");
            configuration.setString(ForStConfigurableOptions.LOG_DIR.key(), "/tmp/ForSt-logs/");
            configuration.setString(ForStConfigurableOptions.LOG_FILE_NUM.key(), "10");
            configuration.setString(ForStConfigurableOptions.LOG_MAX_FILE_SIZE.key(), "2MB");
            configuration.setString(ForStConfigurableOptions.COMPACTION_STYLE.key(), "level");
            configuration.setString(
                    ForStConfigurableOptions.COMPRESSION_PER_LEVEL.key(),
                    "no_compression;snappy_compression;lz4_compression");
            configuration.setString(ForStConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE.key(), "TRUE");
            configuration.setString(ForStConfigurableOptions.TARGET_FILE_SIZE_BASE.key(), "8 mb");
            configuration.setString(ForStConfigurableOptions.MAX_SIZE_LEVEL_BASE.key(), "128MB");
            configuration.setString(ForStConfigurableOptions.MAX_BACKGROUND_THREADS.key(), "4");
            configuration.setString(ForStConfigurableOptions.MAX_WRITE_BUFFER_NUMBER.key(), "4");
            configuration.setString(
                    ForStConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key(), "2");
            configuration.setString(ForStConfigurableOptions.WRITE_BUFFER_SIZE.key(), "64 MB");
            configuration.setString(ForStConfigurableOptions.BLOCK_SIZE.key(), "4 kb");
            configuration.setString(ForStConfigurableOptions.METADATA_BLOCK_SIZE.key(), "8 kb");
            configuration.setString(ForStConfigurableOptions.BLOCK_CACHE_SIZE.key(), "512 mb");
            configuration.setString(ForStConfigurableOptions.USE_BLOOM_FILTER.key(), "TRUE");
            configuration.setString(
                    ForStConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME.key(), "1h");

            try (ForStResourceContainer optionsContainer =
                    new ForStResourceContainer(
                            configuration,
                            null,
                            null,
                            ForStPathContainer.empty(),
                            null,
                            null,
                            null,
                            false)) {

                DBOptions dbOptions = optionsContainer.getDbOptions();
                assertThat(dbOptions.maxOpenFiles()).isEqualTo(-1);
                assertThat(dbOptions.infoLogLevel()).isEqualTo(InfoLogLevel.DEBUG_LEVEL);
                assertThat(dbOptions.dbLogDir()).isEqualTo("/tmp/ForSt-logs/");
                assertThat(dbOptions.keepLogFileNum()).isEqualTo(10);
                assertThat(dbOptions.maxLogFileSize()).isEqualTo(2 * SizeUnit.MB);

                ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();
                assertThat(columnOptions.compactionStyle()).isEqualTo(CompactionStyle.LEVEL);
                assertThat(columnOptions.levelCompactionDynamicLevelBytes()).isTrue();
                assertThat(columnOptions.targetFileSizeBase()).isEqualTo(8 * SizeUnit.MB);
                assertThat(columnOptions.maxBytesForLevelBase()).isEqualTo(128 * SizeUnit.MB);
                assertThat(columnOptions.maxWriteBufferNumber()).isEqualTo(4);
                assertThat(columnOptions.minWriteBufferNumberToMerge()).isEqualTo(2);
                assertThat(columnOptions.writeBufferSize()).isEqualTo(64 * SizeUnit.MB);
                assertThat(columnOptions.compressionPerLevel())
                        .containsExactly(
                                CompressionType.NO_COMPRESSION,
                                CompressionType.SNAPPY_COMPRESSION,
                                CompressionType.LZ4_COMPRESSION);
                assertThat(columnOptions.periodicCompactionSeconds()).isEqualTo(3600);

                BlockBasedTableConfig tableConfig =
                        (BlockBasedTableConfig) columnOptions.tableFormatConfig();
                assertThat(tableConfig.blockSize()).isEqualTo(4 * SizeUnit.KB);
                assertThat(tableConfig.metadataBlockSize()).isEqualTo(8 * SizeUnit.KB);
                assertThat(tableConfig.blockCacheSize()).isEqualTo(512 * SizeUnit.MB);
                assertThat(tableConfig.filterPolicy()).isInstanceOf(BloomFilter.class);
            }
        }
    }

    @Test
    void testOptionsFactory() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();

        // verify that user-defined options factory could be configured via config.yaml
        Configuration config = new Configuration();
        config.setString(ForStOptions.OPTIONS_FACTORY.key(), TestOptionsFactory.class.getName());
        config.setString(TestOptionsFactory.BACKGROUND_JOBS_OPTION.key(), "4");

        forStStateBackend = forStStateBackend.configure(config, getClass().getClassLoader());

        assertThat(forStStateBackend.getForStOptions()).isInstanceOf(TestOptionsFactory.class);

        try (ForStResourceContainer optionsContainer =
                forStStateBackend.createOptionsAndResourceContainer(null)) {
            DBOptions dbOptions = optionsContainer.getDbOptions();
            assertThat(dbOptions.maxBackgroundJobs()).isEqualTo(4);
        }

        // verify that user-defined options factory could be set programmatically and override
        // pre-configured one.
        forStStateBackend.setForStOptions(
                new ForStOptionsFactory() {
                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions;
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        return currentOptions.setCompactionStyle(CompactionStyle.FIFO);
                    }
                });

        try (ForStResourceContainer optionsContainer =
                forStStateBackend.createOptionsAndResourceContainer(null)) {
            ColumnFamilyOptions colCreated = optionsContainer.getColumnOptions();
            assertThat(colCreated.compactionStyle()).isEqualTo(CompactionStyle.FIFO);
        }
    }

    @Test
    void testConfigurableOptions() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ForStConfigurableOptions.COMPACTION_STYLE, CompactionStyle.UNIVERSAL);
        try (final ForStResourceContainer optionsContainer =
                new ForStResourceContainer(
                        configuration,
                        null,
                        null,
                        ForStPathContainer.empty(),
                        null,
                        null,
                        null,
                        false)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertThat(columnFamilyOptions).isNotNull();
            assertThat(columnFamilyOptions.compactionStyle()).isEqualTo(CompactionStyle.UNIVERSAL);
        }

        try (final ForStResourceContainer optionsContainer =
                new ForStResourceContainer(
                        new Configuration(),
                        null,
                        null,
                        ForStPathContainer.empty(),
                        null,
                        null,
                        null,
                        false)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertThat(columnFamilyOptions).isNotNull();
            assertThat(columnFamilyOptions.compactionStyle()).isEqualTo(CompactionStyle.LEVEL);
        }
    }

    @Test
    void testPredefinedAndOptionsFactory() throws Exception {
        final ForStOptionsFactory optionsFactory =
                new ForStOptionsFactory() {
                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions;
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        return currentOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
                    }
                };

        try (final ForStResourceContainer optionsContainer =
                new ForStResourceContainer(optionsFactory)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertThat(columnFamilyOptions).isNotNull();
            assertThat(columnFamilyOptions.compactionStyle()).isEqualTo(CompactionStyle.UNIVERSAL);
        }
    }

    // ------------------------------------------------------------------------
    //  Reconfiguration
    // ------------------------------------------------------------------------

    @Test
    void testForStReconfigurationCopiesExistingValues() throws Exception {
        final ForStStateBackend original = new ForStStateBackend();
        final ForStOptionsFactory optionsFactory = new TestOptionsFactory();
        original.setForStOptions(optionsFactory);

        final String[] localDirs =
                new String[] {
                    TempDirUtils.newFolder(tempFolder).getAbsolutePath(),
                    TempDirUtils.newFolder(tempFolder).getAbsolutePath()
                };
        original.setLocalDbStoragePaths(localDirs);

        ForStStateBackend copy =
                original.configure(
                        new Configuration(), Thread.currentThread().getContextClassLoader());

        assertThat(copy.getLocalDbStoragePaths()).isEqualTo(original.getLocalDbStoragePaths());
        assertThat(copy.getForStOptions()).isEqualTo(original.getForStOptions());
    }

    // ------------------------------------------------------------------------
    //  ForSt Memory Control
    // ------------------------------------------------------------------------

    @Test
    void testDefaultMemoryControlParameters() {
        ForStMemoryConfiguration memSettings = new ForStMemoryConfiguration();
        assertThat(memSettings.isUsingManagedMemory()).isTrue();
        assertThat(memSettings.isUsingFixedMemoryPerSlot()).isFalse();
        assertThat(memSettings.getHighPriorityPoolRatio())
                .isEqualTo(ForStOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue());
        assertThat(memSettings.getWriteBufferRatio())
                .isEqualTo(ForStOptions.WRITE_BUFFER_RATIO.defaultValue());

        ForStMemoryConfiguration configured =
                ForStMemoryConfiguration.fromOtherAndConfiguration(
                        memSettings, new Configuration());
        assertThat(configured.isUsingManagedMemory()).isTrue();
        assertThat(configured.isUsingFixedMemoryPerSlot()).isFalse();
        assertThat(configured.getHighPriorityPoolRatio())
                .isEqualTo(ForStOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue());
        assertThat(configured.getWriteBufferRatio())
                .isEqualTo(ForStOptions.WRITE_BUFFER_RATIO.defaultValue());
    }

    @Test
    void testConfigureManagedMemory() {
        final Configuration config = new Configuration();
        config.set(ForStOptions.USE_MANAGED_MEMORY, true);

        final ForStMemoryConfiguration memSettings =
                ForStMemoryConfiguration.fromOtherAndConfiguration(
                        new ForStMemoryConfiguration(), config);

        assertThat(memSettings.isUsingManagedMemory()).isTrue();
    }

    @Test
    void testConfigureIllegalMemoryControlParameters() {
        ForStMemoryConfiguration memSettings = new ForStMemoryConfiguration();

        verifySetParameter(() -> memSettings.setFixedMemoryPerSlot("-1B"));
        verifySetParameter(() -> memSettings.setHighPriorityPoolRatio(-0.1));
        verifySetParameter(() -> memSettings.setHighPriorityPoolRatio(1.1));
        verifySetParameter(() -> memSettings.setWriteBufferRatio(-0.1));
        verifySetParameter(() -> memSettings.setWriteBufferRatio(1.1));

        memSettings.setFixedMemoryPerSlot("128MB");
        memSettings.setWriteBufferRatio(0.6);
        memSettings.setHighPriorityPoolRatio(0.6);

        // sum of writeBufferRatio and highPriPoolRatio larger than 1.0
        assertThatThrownBy(memSettings::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPrimaryDirectory() throws Exception {
        FileSystem.initialize(new Configuration(), null);
        Configuration configuration = new Configuration();
        configuration.set(
                ForStOptions.PRIMARY_DIRECTORY,
                TempDirUtils.newFolder(tempFolder).toURI().toString());
        ForStStateBackend forStStateBackend =
                new ForStStateBackend().configure(configuration, null);
        ForStKeyedStateBackend<Integer> keyedBackend = null;
        try {
            keyedBackend =
                    createKeyedStateBackend(
                            forStStateBackend,
                            getMockEnvironment(TempDirUtils.newFolder(tempFolder)),
                            IntSerializer.INSTANCE);
            assertThat(keyedBackend.getRemoteBasePath().toString())
                    .startsWith(configuration.get(ForStOptions.PRIMARY_DIRECTORY));
        } finally {
            if (keyedBackend != null) {
                keyedBackend.dispose();
                keyedBackend.close();
            }
        }
    }

    @Test
    void testSupportSavepoint() {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        assertThat(forStStateBackend.supportsSavepointFormat(SavepointFormatType.CANONICAL))
                .isFalse();
        assertThat(forStStateBackend.supportsSavepointFormat(SavepointFormatType.NATIVE)).isTrue();
    }

    @Test
    void testConfigurePeriodicCompactionTime() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        Configuration configuration = new Configuration();
        configuration.setString(
                ForStConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME.key(), "1d");
        forStStateBackend = forStStateBackend.configure(configuration, getClass().getClassLoader());
        try (ForStResourceContainer resourceContainer =
                forStStateBackend.createOptionsAndResourceContainer(null)) {
            assertThat(resourceContainer.getPeriodicCompactionTime()).isEqualTo(Duration.ofDays(1));
        }
    }

    @Test
    void testConfigureQueryTimeAfterNumEntries() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        Configuration configuration = new Configuration();
        configuration.setString(
                ForStConfigurableOptions.COMPACT_FILTER_QUERY_TIME_AFTER_NUM_ENTRIES.key(), "100");
        forStStateBackend = forStStateBackend.configure(configuration, getClass().getClassLoader());
        try (ForStResourceContainer resourceContainer =
                forStStateBackend.createOptionsAndResourceContainer(null)) {
            assertThat(resourceContainer.getQueryTimeAfterNumEntries().longValue()).isEqualTo(100L);
        }
    }

    private void verifySetParameter(Runnable setter) {
        assertThatThrownBy(setter::run).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    static MockEnvironment getMockEnvironment(File tempDir) throws IOException {
        MockEnvironment env =
                MockEnvironment.builder()
                        .setUserCodeClassLoader(ForStStateBackendConfigTest.class.getClassLoader())
                        .setTaskManagerRuntimeInfo(
                                new TestingTaskManagerRuntimeInfo(new Configuration(), tempDir))
                        .build();
        CheckpointStorageAccess checkpointStorageAccess =
                new FsCheckpointStorageAccess(
                        new Path(tempDir.getPath(), "checkpoint"),
                        null,
                        env.getJobID(),
                        1024,
                        4096);
        env.setCheckpointStorageAccess(checkpointStorageAccess);
        return env;
    }

    private void verifyIllegalArgument(ConfigOption<?> configOption, String configValue) {
        Configuration configuration = new Configuration();
        configuration.setString(configOption.key(), configValue);

        ForStStateBackend stateBackend = new ForStStateBackend();
        assertThatThrownBy(() -> stateBackend.configure(configuration, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** An implementation of options factory for testing. */
    public static class TestOptionsFactory implements ConfigurableForStOptionsFactory {
        public static final ConfigOption<Integer> BACKGROUND_JOBS_OPTION =
                ConfigOptions.key("my.custom.forst.backgroundJobs").intType().defaultValue(2);

        private int backgroundJobs = BACKGROUND_JOBS_OPTION.defaultValue();

        @Override
        public DBOptions createDBOptions(
                DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
            return currentOptions.setMaxBackgroundJobs(backgroundJobs);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(
                ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
            return currentOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
        }

        @Override
        public ForStOptionsFactory configure(ReadableConfig configuration) {
            this.backgroundJobs = configuration.get(BACKGROUND_JOBS_OPTION);
            return this;
        }
    }
}
