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

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.state.rocksdb.RocksDBTestUtils.createKeyedStateBackend;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for configuring the RocksDB State Backend. */
@SuppressWarnings("serial")
class RocksDBStateBackendConfigTest {

    @TempDir private java.nio.file.Path tempFolder;

    // ------------------------------------------------------------------------
    //  default values
    // ------------------------------------------------------------------------

    @Test
    void testDefaultsInSync() {
        final boolean defaultIncremental =
                CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        assertThat(backend.isIncrementalCheckpointsEnabled()).isEqualTo(defaultIncremental);
    }

    @Test
    void testDefaultDbLogDir() throws Exception {
        final EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        final File logFile = File.createTempFile(getClass().getSimpleName() + "-", ".log");
        // set the environment variable 'log.file' with the Flink log file location
        System.setProperty("log.file", logFile.getPath());
        try (RocksDBResourceContainer container = backend.createOptionsAndResourceContainer(null)) {
            assertThat(container.getDbOptions().infoLogLevel())
                    .isEqualTo(RocksDBConfigurableOptions.LOG_LEVEL.defaultValue());
            assertThat(container.getDbOptions().dbLogDir()).isEqualTo(logFile.getParent());
        } finally {
            logFile.delete();
        }

        StringBuilder longInstanceBasePath =
                new StringBuilder(TempDirUtils.newFolder(tempFolder).getAbsolutePath());
        while (longInstanceBasePath.length() < 255) {
            longInstanceBasePath.append("/append-for-long-path");
        }
        try (RocksDBResourceContainer container =
                backend.createOptionsAndResourceContainer(
                        new File(longInstanceBasePath.toString()))) {
            assertThat(container.getDbOptions().dbLogDir()).isEmpty();
        } finally {
            logFile.delete();
        }
    }

    // ------------------------------------------------------------------------
    //  RocksDB local file directory
    // ------------------------------------------------------------------------

    /** This test checks the behavior for basic setting of local DB directories. */
    @Test
    void testSetDbPath() throws Exception {
        final EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        final String testDir1 = TempDirUtils.newFolder(tempFolder).getAbsolutePath();
        final String testDir2 = TempDirUtils.newFolder(tempFolder).getAbsolutePath();

        assertThat(rocksDbBackend.getDbStoragePaths()).isNull();

        rocksDbBackend.setDbStoragePath(testDir1);
        assertThat(rocksDbBackend.getDbStoragePaths()).containsExactly(testDir1);

        rocksDbBackend.setDbStoragePath(null);
        assertThat(rocksDbBackend.getDbStoragePaths()).isNull();

        rocksDbBackend.setDbStoragePaths(testDir1, testDir2);
        assertThat(rocksDbBackend.getDbStoragePaths()).containsExactly(testDir1, testDir2);

        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        final RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);

        try {
            File instanceBasePath = keyedBackend.getInstanceBasePath();
            assertThat(instanceBasePath.getAbsolutePath())
                    .satisfiesAnyOf(
                            p -> assertThat(p).startsWith(testDir1),
                            p -> assertThat(p).startsWith(testDir2));

            //noinspection NullArgumentToVariableArgMethod
            rocksDbBackend.setDbStoragePaths(null);
            assertThat(rocksDbBackend.getDbStoragePaths()).isNull();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
            env.close();
        }
    }

    @Test
    void testConfigureTimerService() throws Exception {

        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));

        // Fix the option key string
        assertThat(RocksDBOptions.TIMER_SERVICE_FACTORY.key())
                .isEqualTo("state.backend.rocksdb.timer-service.factory");

        // Fix the option value string and ensure all are covered
        assertThat(EmbeddedRocksDBStateBackend.PriorityQueueStateType.values()).hasSize(2);
        assertThat(EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB)
                .hasToString("ROCKSDB");
        assertThat(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP).hasToString("HEAP");

        // Fix the default
        assertThat(RocksDBOptions.TIMER_SERVICE_FACTORY.defaultValue())
                .isEqualTo(EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);

        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);
        assertThat(keyedBackend.getPriorityQueueFactory())
                .isExactlyInstanceOf(RocksDBPriorityQueueSetFactory.class);
        keyedBackend.dispose();

        Configuration conf = new Configuration();
        conf.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);

        rocksDbBackend =
                rocksDbBackend.configure(conf, Thread.currentThread().getContextClassLoader());
        keyedBackend = createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);
        assertThat(keyedBackend.getPriorityQueueFactory())
                .isExactlyInstanceOf(HeapPriorityQueueSetFactory.class);
        keyedBackend.dispose();
        env.close();
    }

    @Test
    void testConfigureRocksDBPriorityQueueFactoryCacheSize() throws Exception {
        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();
        int cacheSize = 512;
        Configuration conf = new Configuration();
        conf.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
        conf.set(RocksDBOptions.ROCKSDB_TIMER_SERVICE_FACTORY_CACHE_SIZE, cacheSize);

        rocksDbBackend =
                rocksDbBackend.configure(conf, Thread.currentThread().getContextClassLoader());

        RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);

        assertThat(keyedBackend.getPriorityQueueFactory())
                .isExactlyInstanceOf(RocksDBPriorityQueueSetFactory.class);
        assertThat(
                        ((RocksDBPriorityQueueSetFactory) keyedBackend.getPriorityQueueFactory())
                                .getCacheSize())
                .isEqualTo(cacheSize);
        keyedBackend.dispose();
        env.close();
    }

    /** Validates that user custom configuration from code should override the config.yaml. */
    @Test
    void testConfigureTimerServiceLoadingFromApplication() throws Exception {
        final MockEnvironment env = new MockEnvironmentBuilder().build();

        // priorityQueueStateType of the job backend
        final EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);

        // priorityQueueStateType in the cluster config
        final Configuration configFromConfFile = new Configuration();
        configFromConfFile.setString(
                RocksDBOptions.TIMER_SERVICE_FACTORY.key(),
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString());

        // configure final backend from job and cluster config
        final EmbeddedRocksDBStateBackend configuredRocksDBStateBackend =
                backend.configure(
                        configFromConfFile, Thread.currentThread().getContextClassLoader());
        final RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(configuredRocksDBStateBackend, env, IntSerializer.INSTANCE);

        // priorityQueueStateType of the job backend should be preserved
        assertThat(keyedBackend.getPriorityQueueFactory())
                .isInstanceOf(HeapPriorityQueueSetFactory.class);

        keyedBackend.close();
        keyedBackend.dispose();
        env.close();
    }

    @Test
    void testConfigureRocksDBCompressionPerLevel() throws Exception {
        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();
        CompressionType[] compressionTypes = {
            CompressionType.NO_COMPRESSION, CompressionType.SNAPPY_COMPRESSION
        };
        Configuration conf = new Configuration();
        conf.set(
                RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL,
                new ArrayList<>(Arrays.asList(compressionTypes)));

        rocksDbBackend =
                rocksDbBackend.configure(conf, Thread.currentThread().getContextClassLoader());

        RocksDBResourceContainer resourceContainer =
                rocksDbBackend.createOptionsAndResourceContainer(TempDirUtils.newFile(tempFolder));
        ColumnFamilyOptions columnFamilyOptions = resourceContainer.getColumnOptions();
        assertThat(columnFamilyOptions.compressionPerLevel()).containsExactly(compressionTypes);

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
            config.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "s3://mydomain.com:8020/flink");
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
            config.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "s3://mydomain.com:8020/flink");
            FileSystem.initialize(config);

            testLocalDbPaths(dbStoragePath, folder);
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }

    private void testLocalDbPaths(String configuredPath, File expectedPath) throws Exception {
        final EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();
        rocksDbBackend.setDbStoragePath(configuredPath);

        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);

        try {
            File instanceBasePath = keyedBackend.getInstanceBasePath();
            assertThat(instanceBasePath.getAbsolutePath())
                    .startsWith(expectedPath.getAbsolutePath());

            //noinspection NullArgumentToVariableArgMethod
            rocksDbBackend.setDbStoragePaths(null);
            assertThat(rocksDbBackend.getDbStoragePaths()).isNull();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
            env.close();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testCleanRelocatedDbLogs() throws Exception {
        final File folder = TempDirUtils.newFolder(tempFolder);
        final File relocatedDBLogDir = TempDirUtils.newFolder(tempFolder, "db_logs");
        final File logFile = new File(relocatedDBLogDir, "taskManager.log");
        Files.createFile(logFile.toPath());
        System.setProperty("log.file", logFile.getAbsolutePath());

        Configuration conf = new Configuration();
        conf.set(RocksDBConfigurableOptions.LOG_LEVEL, InfoLogLevel.DEBUG_LEVEL);
        conf.set(RocksDBConfigurableOptions.LOG_FILE_NUM, 4);
        conf.set(RocksDBConfigurableOptions.LOG_MAX_FILE_SIZE, MemorySize.parse("1kb"));
        final EmbeddedRocksDBStateBackend rocksDbBackend =
                new EmbeddedRocksDBStateBackend().configure(conf, getClass().getClassLoader());
        final String dbStoragePath = new Path(folder.toURI().toString()).toString();
        rocksDbBackend.setDbStoragePath(dbStoragePath);

        final MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder));
        RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);

        File instanceBasePath = keyedBackend.getInstanceBasePath();
        File instanceRocksDBPath =
                RocksDBKeyedStateBackendBuilder.getInstanceRocksDBPath(instanceBasePath);

        // avoid tests without relocate.
        assumeThat(instanceRocksDBPath.getAbsolutePath().length())
                .isLessThanOrEqualTo(255 - "_LOG".length());

        java.nio.file.Path[] relocatedDbLogs;
        try {
            relocatedDbLogs = FileUtils.listDirectory(relocatedDBLogDir.toPath());
            while (relocatedDbLogs.length <= 2) {
                // If the default number of log files in rocksdb is not enough, add more logs.
                try (FlushOptions flushOptions = new FlushOptions()) {
                    keyedBackend.db.put(RandomUtils.nextBytes(32), RandomUtils.nextBytes(512));
                    keyedBackend.db.flush(flushOptions);
                }
                relocatedDbLogs = FileUtils.listDirectory(relocatedDBLogDir.toPath());
            }
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
            env.close();
        }

        relocatedDbLogs = FileUtils.listDirectory(relocatedDBLogDir.toPath());
        assertThat(relocatedDbLogs).hasSize(1);
        assertThat(relocatedDbLogs[0].toFile().getName()).isEqualTo("taskManager.log");
    }

    // ------------------------------------------------------------------------
    //  RocksDB local file automatic from temp directories
    // ------------------------------------------------------------------------

    /**
     * This tests whether the RocksDB backends uses the temp directories that are provided from the
     * {@link Environment} when no db storage path is set.
     */
    @Test
    void testUseTempDirectories() throws Exception {
        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        File dir1 = TempDirUtils.newFolder(tempFolder);

        assertThat(rocksDbBackend.getDbStoragePaths()).isNull();

        final MockEnvironment env = getMockEnvironment(dir1);
        JobID jobID = env.getJobID();
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
        TaskKvStateRegistry kvStateRegistry = env.getTaskKvStateRegistry();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        RocksDBKeyedStateBackend<Integer> keyedBackend =
                (RocksDBKeyedStateBackend<Integer>)
                        rocksDbBackend.createKeyedStateBackend(
                                new KeyedStateBackendParametersImpl<>(
                                        (Environment) env,
                                        jobID,
                                        "test_op",
                                        IntSerializer.INSTANCE,
                                        1,
                                        keyGroupRange,
                                        kvStateRegistry,
                                        TtlTimeProvider.DEFAULT,
                                        (MetricGroup) new UnregisteredMetricsGroup(),
                                        Collections.emptyList(),
                                        cancelStreamRegistry));

        try {
            File instanceBasePath = keyedBackend.getInstanceBasePath();
            assertThat(instanceBasePath.getAbsolutePath()).startsWith(dir1.getAbsolutePath());
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
            env.close();
        }
    }

    // ------------------------------------------------------------------------
    //  RocksDB local file directory initialization
    // ------------------------------------------------------------------------

    @Test
    void testFailWhenNoLocalStorageDir() throws Exception {
        final File targetDir = TempDirUtils.newFolder(tempFolder);
        assumeThat(targetDir.setWritable(false, false))
                .as("Cannot mark directory non-writable")
                .isTrue();

        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        try (MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder))) {
            rocksDbBackend.setDbStoragePath(targetDir.getAbsolutePath());

            JobID jobID = env.getJobID();
            KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
            TaskKvStateRegistry kvStateRegistry =
                    new KvStateRegistry().createTaskRegistry(env.getJobID(), new JobVertexID());
            CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
            assertThatThrownBy(
                            () ->
                                    rocksDbBackend.createKeyedStateBackend(
                                            new KeyedStateBackendParametersImpl<>(
                                                    (Environment) env,
                                                    jobID,
                                                    "foobar",
                                                    IntSerializer.INSTANCE,
                                                    1,
                                                    keyGroupRange,
                                                    kvStateRegistry,
                                                    TtlTimeProvider.DEFAULT,
                                                    (MetricGroup) new UnregisteredMetricsGroup(),
                                                    Collections.emptyList(),
                                                    cancelStreamRegistry)),
                            "We must see a failure because no storaged directory is feasible.")
                    .hasMessageContaining("No local storage directories available")
                    .hasMessageContaining(targetDir.getAbsolutePath());
        } finally {
            //noinspection ResultOfMethodCallIgnored
            targetDir.setWritable(true, false);
        }
    }

    @Test
    void testContinueOnSomeDbDirectoriesMissing() throws Exception {
        final File targetDir1 = TempDirUtils.newFolder(tempFolder);
        final File targetDir2 = TempDirUtils.newFolder(tempFolder);
        assumeThat(targetDir1.setWritable(false, false))
                .as("Cannot mark directory non-writable")
                .isTrue();

        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        try (MockEnvironment env = getMockEnvironment(TempDirUtils.newFolder(tempFolder))) {
            rocksDbBackend.setDbStoragePaths(
                    targetDir1.getAbsolutePath(), targetDir2.getAbsolutePath());

            assertThatCode(
                            () -> {
                                JobID jobID = env.getJobID();
                                KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
                                TaskKvStateRegistry kvStateRegistry =
                                        new KvStateRegistry()
                                                .createTaskRegistry(
                                                        env.getJobID(), new JobVertexID());
                                CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
                                CheckpointableKeyedStateBackend<Integer> keyedStateBackend =
                                        rocksDbBackend.createKeyedStateBackend(
                                                new KeyedStateBackendParametersImpl<>(
                                                        (Environment) env,
                                                        jobID,
                                                        "foobar",
                                                        IntSerializer.INSTANCE,
                                                        1,
                                                        keyGroupRange,
                                                        kvStateRegistry,
                                                        TtlTimeProvider.DEFAULT,
                                                        (MetricGroup)
                                                                new UnregisteredMetricsGroup(),
                                                        Collections.emptyList(),
                                                        cancelStreamRegistry));

                                IOUtils.closeQuietly(keyedStateBackend);
                                keyedStateBackend.dispose();
                            })
                    .as("Backend initialization failed even though some paths were available")
                    .doesNotThrowAnyException();
        } finally {
            //noinspection ResultOfMethodCallIgnored
            targetDir1.setWritable(true, false);
        }
    }

    // ------------------------------------------------------------------------
    //  RocksDB Options
    // ------------------------------------------------------------------------

    @Test
    void testPredefinedOptions() throws Exception {
        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        // verify that we would use PredefinedOptions.DEFAULT by default.
        assertThat(rocksDbBackend.getPredefinedOptions()).isEqualTo(PredefinedOptions.DEFAULT);

        // verify that user could configure predefined options via config.yaml
        Configuration configuration = new Configuration();
        configuration.set(
                RocksDBOptions.PREDEFINED_OPTIONS, PredefinedOptions.FLASH_SSD_OPTIMIZED.name());
        rocksDbBackend = new EmbeddedRocksDBStateBackend();
        rocksDbBackend = rocksDbBackend.configure(configuration, getClass().getClassLoader());
        assertThat(rocksDbBackend.getPredefinedOptions())
                .isEqualTo(PredefinedOptions.FLASH_SSD_OPTIMIZED);

        // verify that predefined options could be set programmatically and override pre-configured
        // one.
        rocksDbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
        assertThat(rocksDbBackend.getPredefinedOptions())
                .isEqualTo(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
    }

    @Test
    void testConfigurableOptionsFromConfig() throws Exception {
        Configuration configuration = new Configuration();

        // verify illegal configuration
        {
            verifyIllegalArgument(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, "-1");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_LEVEL, "DEBUG");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_DIR, "tmp/rocksdb-logs/");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_DIR, "");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_FILE_NUM, "0");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_FILE_NUM, "-1");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_MAX_FILE_SIZE, "-1KB");
            verifyIllegalArgument(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, "-1");
            verifyIllegalArgument(
                    RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE, "-1");

            verifyIllegalArgument(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE, "0KB");
            verifyIllegalArgument(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE, "1BB");
            verifyIllegalArgument(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, "-1KB");
            verifyIllegalArgument(RocksDBConfigurableOptions.BLOCK_SIZE, "0MB");
            verifyIllegalArgument(RocksDBConfigurableOptions.METADATA_BLOCK_SIZE, "0MB");
            verifyIllegalArgument(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, "0");

            verifyIllegalArgument(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, "1");

            verifyIllegalArgument(RocksDBConfigurableOptions.COMPACTION_STYLE, "LEV");
            verifyIllegalArgument(RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL, "SNAP");
            verifyIllegalArgument(RocksDBConfigurableOptions.USE_BLOOM_FILTER, "NO");
            verifyIllegalArgument(RocksDBConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE, "YES");
            verifyIllegalArgument(
                    RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD, "2");
            verifyIllegalArgument(
                    RocksDBConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME, "-1s");
            verifyIllegalArgument(
                    RocksDBConfigurableOptions.COMPACT_FILTER_QUERY_TIME_AFTER_NUM_ENTRIES, "1.1");
        }

        // verify legal configuration
        {
            configuration.setString(RocksDBConfigurableOptions.LOG_LEVEL.key(), "DEBUG_LEVEL");
            configuration.setString(RocksDBConfigurableOptions.LOG_DIR.key(), "/tmp/rocksdb-logs/");
            configuration.setString(RocksDBConfigurableOptions.LOG_FILE_NUM.key(), "10");
            configuration.setString(RocksDBConfigurableOptions.LOG_MAX_FILE_SIZE.key(), "2MB");
            configuration.setString(RocksDBConfigurableOptions.COMPACTION_STYLE.key(), "level");
            configuration.setString(
                    RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL.key(),
                    "no_compression;snappy_compression;lz4_compression");
            configuration.setString(
                    RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE.key(), "TRUE");
            configuration.setString(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE.key(), "8 mb");
            configuration.setString(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE.key(), "128MB");
            configuration.setString(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS.key(), "4");
            configuration.setString(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER.key(), "4");
            configuration.setString(
                    RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key(), "2");
            configuration.setString(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE.key(), "64 MB");
            configuration.setString(RocksDBConfigurableOptions.BLOCK_SIZE.key(), "4 kb");
            configuration.setString(RocksDBConfigurableOptions.METADATA_BLOCK_SIZE.key(), "8 kb");
            configuration.setString(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE.key(), "512 mb");
            configuration.setString(RocksDBConfigurableOptions.USE_BLOOM_FILTER.key(), "TRUE");
            configuration.setString(
                    RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD.key(), "0.5");
            configuration.setString(
                    RocksDBConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME.key(), "1h");

            try (RocksDBResourceContainer optionsContainer =
                    new RocksDBResourceContainer(
                            configuration, PredefinedOptions.DEFAULT, null, null, null, false)) {

                DBOptions dbOptions = optionsContainer.getDbOptions();
                assertThat(dbOptions.maxOpenFiles()).isEqualTo(-1);
                assertThat(dbOptions.infoLogLevel()).isEqualTo(InfoLogLevel.DEBUG_LEVEL);
                assertThat(dbOptions.dbLogDir()).isEqualTo("/tmp/rocksdb-logs/");
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
        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        // verify that user-defined options factory could be configured via config.yaml
        Configuration config = new Configuration();
        config.setString(RocksDBOptions.OPTIONS_FACTORY.key(), TestOptionsFactory.class.getName());
        config.setString(TestOptionsFactory.BACKGROUND_JOBS_OPTION.key(), "4");

        rocksDbBackend = rocksDbBackend.configure(config, getClass().getClassLoader());

        assertThat(rocksDbBackend.getRocksDBOptions()).isInstanceOf(TestOptionsFactory.class);

        try (RocksDBResourceContainer optionsContainer =
                rocksDbBackend.createOptionsAndResourceContainer(null)) {
            DBOptions dbOptions = optionsContainer.getDbOptions();
            assertThat(dbOptions.maxBackgroundJobs()).isEqualTo(4);
        }

        // verify that user-defined options factory could be set programmatically and override
        // pre-configured one.
        rocksDbBackend.setRocksDBOptions(
                new RocksDBOptionsFactory() {
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

        try (RocksDBResourceContainer optionsContainer =
                rocksDbBackend.createOptionsAndResourceContainer(null)) {
            ColumnFamilyOptions colCreated = optionsContainer.getColumnOptions();
            assertThat(colCreated.compactionStyle()).isEqualTo(CompactionStyle.FIFO);
        }
    }

    @Test
    void testPredefinedAndConfigurableOptions() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RocksDBConfigurableOptions.COMPACTION_STYLE, CompactionStyle.UNIVERSAL);
        try (final RocksDBResourceContainer optionsContainer =
                new RocksDBResourceContainer(
                        configuration,
                        PredefinedOptions.SPINNING_DISK_OPTIMIZED,
                        null,
                        null,
                        null,
                        false)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertThat(columnFamilyOptions).isNotNull();
            assertThat(columnFamilyOptions.compactionStyle()).isEqualTo(CompactionStyle.UNIVERSAL);
        }

        try (final RocksDBResourceContainer optionsContainer =
                new RocksDBResourceContainer(
                        new Configuration(),
                        PredefinedOptions.SPINNING_DISK_OPTIMIZED,
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
        final RocksDBOptionsFactory optionsFactory =
                new RocksDBOptionsFactory() {
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

        try (final RocksDBResourceContainer optionsContainer =
                new RocksDBResourceContainer(
                        PredefinedOptions.SPINNING_DISK_OPTIMIZED, optionsFactory)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertThat(columnFamilyOptions).isNotNull();
            assertThat(columnFamilyOptions.compactionStyle()).isEqualTo(CompactionStyle.UNIVERSAL);
        }
    }

    // ------------------------------------------------------------------------
    //  RocksDB Memory Control
    // ------------------------------------------------------------------------

    @Test
    void testDefaultMemoryControlParameters() {
        RocksDBMemoryConfiguration memSettings = new RocksDBMemoryConfiguration();
        assertThat(memSettings.isUsingManagedMemory()).isTrue();
        assertThat(memSettings.isUsingFixedMemoryPerSlot()).isFalse();
        assertThat(memSettings.getHighPriorityPoolRatio())
                .isEqualTo(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue());
        assertThat(memSettings.getWriteBufferRatio())
                .isEqualTo(RocksDBOptions.WRITE_BUFFER_RATIO.defaultValue());

        RocksDBMemoryConfiguration configured =
                RocksDBMemoryConfiguration.fromOtherAndConfiguration(
                        memSettings, new Configuration());
        assertThat(configured.isUsingManagedMemory()).isTrue();
        assertThat(configured.isUsingFixedMemoryPerSlot()).isFalse();
        assertThat(configured.getHighPriorityPoolRatio())
                .isEqualTo(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue());
        assertThat(configured.getWriteBufferRatio())
                .isEqualTo(RocksDBOptions.WRITE_BUFFER_RATIO.defaultValue());
    }

    @Test
    void testConfigureManagedMemory() {
        final Configuration config = new Configuration();
        config.set(RocksDBOptions.USE_MANAGED_MEMORY, true);

        final RocksDBMemoryConfiguration memSettings =
                RocksDBMemoryConfiguration.fromOtherAndConfiguration(
                        new RocksDBMemoryConfiguration(), config);

        assertThat(memSettings.isUsingManagedMemory()).isTrue();
    }

    @Test
    void testConfigureIllegalMemoryControlParameters() {
        RocksDBMemoryConfiguration memSettings = new RocksDBMemoryConfiguration();

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
    void testDefaultRestoreOverlapThreshold() {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        assertThat(rocksDBStateBackend.getOverlapFractionThreshold())
                .isEqualTo(
                        RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD
                                .defaultValue());
    }

    @Test
    void testConfigureRestoreOverlapThreshold() {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        Configuration configuration = new Configuration();
        configuration.set(RocksDBConfigurableOptions.RESTORE_OVERLAP_FRACTION_THRESHOLD, 0.3);
        rocksDBStateBackend =
                rocksDBStateBackend.configure(configuration, getClass().getClassLoader());
        assertThat(rocksDBStateBackend.getOverlapFractionThreshold()).isEqualTo(0.3);
    }

    @Test
    void testDefaultUseIngestDB() {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        assertThat(rocksDBStateBackend.getUseIngestDbRestoreMode())
                .isEqualTo(RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE.defaultValue());
    }

    @Test
    void testConfigureUseIngestDB() {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        Configuration configuration = new Configuration();
        configuration.set(RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE, true);
        rocksDBStateBackend =
                rocksDBStateBackend.configure(configuration, getClass().getClassLoader());
        assertThat(rocksDBStateBackend.getUseIngestDbRestoreMode()).isTrue();
    }

    @Test
    void testDefaultUseDeleteFilesInRange() {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        assertThat(rocksDBStateBackend.isRescalingUseDeleteFilesInRange())
                .isEqualTo(
                        RocksDBConfigurableOptions.USE_DELETE_FILES_IN_RANGE_DURING_RESCALING
                                .defaultValue());
    }

    @Test
    void testConfigureUseFilesInRange() {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        Configuration configuration = new Configuration();
        configuration.set(
                RocksDBConfigurableOptions.USE_DELETE_FILES_IN_RANGE_DURING_RESCALING,
                !RocksDBConfigurableOptions.USE_DELETE_FILES_IN_RANGE_DURING_RESCALING
                        .defaultValue());
        rocksDBStateBackend =
                rocksDBStateBackend.configure(configuration, getClass().getClassLoader());
        assertThat(rocksDBStateBackend.isRescalingUseDeleteFilesInRange())
                .isEqualTo(
                        !RocksDBConfigurableOptions.USE_DELETE_FILES_IN_RANGE_DURING_RESCALING
                                .defaultValue());
    }

    @Test
    void testDefaultIncrementalRestoreInstanceBufferSize() {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        assertThat(rocksDBStateBackend.getIncrementalRestoreAsyncCompactAfterRescale())
                .isEqualTo(
                        RocksDBConfigurableOptions.INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE
                                .defaultValue());
    }

    @Test
    void testConfigureIncrementalRestoreInstanceBufferSize() {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        Configuration configuration = new Configuration();
        boolean notDefault =
                !RocksDBConfigurableOptions.INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE
                        .defaultValue();
        configuration.set(
                RocksDBConfigurableOptions.INCREMENTAL_RESTORE_ASYNC_COMPACT_AFTER_RESCALE,
                notDefault);
        rocksDBStateBackend =
                rocksDBStateBackend.configure(configuration, getClass().getClassLoader());
        assertThat(rocksDBStateBackend.getIncrementalRestoreAsyncCompactAfterRescale())
                .isEqualTo(notDefault);
    }

    @Test
    void testConfigurePeriodicCompactionTime() throws Exception {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        Configuration configuration = new Configuration();
        configuration.setString(
                RocksDBConfigurableOptions.COMPACT_FILTER_PERIODIC_COMPACTION_TIME.key(), "1d");
        rocksDBStateBackend =
                rocksDBStateBackend.configure(configuration, getClass().getClassLoader());
        try (RocksDBResourceContainer resourceContainer =
                rocksDBStateBackend.createOptionsAndResourceContainer(null)) {
            assertThat(resourceContainer.getPeriodicCompactionTime()).isEqualTo(Duration.ofDays(1));
        }
    }

    @Test
    void testConfigureQueryTimeAfterNumEntries() throws Exception {
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        Configuration configuration = new Configuration();
        configuration.setString(
                RocksDBConfigurableOptions.COMPACT_FILTER_QUERY_TIME_AFTER_NUM_ENTRIES.key(),
                "100");
        rocksDBStateBackend =
                rocksDBStateBackend.configure(configuration, getClass().getClassLoader());
        try (RocksDBResourceContainer resourceContainer =
                rocksDBStateBackend.createOptionsAndResourceContainer(null)) {
            assertThat(resourceContainer.getQueryTimeAfterNumEntries()).isEqualTo(100L);
        }
    }

    private void verifySetParameter(Runnable setter) {
        assertThatThrownBy(setter::run).isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    static MockEnvironment getMockEnvironment(File tempDir) {
        return MockEnvironment.builder()
                .setUserCodeClassLoader(RocksDBStateBackendConfigTest.class.getClassLoader())
                .setTaskManagerRuntimeInfo(
                        new TestingTaskManagerRuntimeInfo(new Configuration(), tempDir))
                .build();
    }

    private void verifyIllegalArgument(ConfigOption<?> configOption, String configValue) {
        Configuration configuration = new Configuration();
        configuration.setString(configOption.key(), configValue);

        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend();
        assertThatThrownBy(() -> stateBackend.configure(configuration, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** An implementation of options factory for testing. */
    public static class TestOptionsFactory implements ConfigurableRocksDBOptionsFactory {
        public static final ConfigOption<Integer> BACKGROUND_JOBS_OPTION =
                ConfigOptions.key("my.custom.rocksdb.backgroundJobs").intType().defaultValue(2);

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
        public RocksDBOptionsFactory configure(ReadableConfig configuration) {
            this.backgroundJobs = configuration.get(BACKGROUND_JOBS_OPTION);
            return this;
        }
    }
}
