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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.testutils.junit.FailsInGHAContainerWithRootUser;
import org.apache.flink.util.FileUtils;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Timeout;
import org.junit.rules.TemporaryFolder;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.state.forst.ForStTestUtils.createKeyedStateBackend;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for configuring the ForSt State Backend. */
public class ForStStateBackendConfigTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    // ------------------------------------------------------------------------
    //  default values
    // ------------------------------------------------------------------------

    @Test
    public void testDefaultDbLogDir() throws Exception {
        final ForStStateBackend backend = new ForStStateBackend();
        final File logFile = File.createTempFile(getClass().getSimpleName() + "-", ".log");
        // set the environment variable 'log.file' with the Flink log file location
        System.setProperty("log.file", logFile.getPath());
        try (ForStResourceContainer container = backend.createOptionsAndResourceContainer(null)) {
            assertEquals(
                    ForStConfigurableOptions.LOG_LEVEL.defaultValue(),
                    container.getDbOptions().infoLogLevel());
            assertEquals(logFile.getParent(), container.getDbOptions().dbLogDir());
        } finally {
            logFile.delete();
        }

        StringBuilder longInstanceBasePath =
                new StringBuilder(tempFolder.newFolder().getAbsolutePath());
        while (longInstanceBasePath.length() < 255) {
            longInstanceBasePath.append("/append-for-long-path");
        }
        try (ForStResourceContainer container =
                backend.createOptionsAndResourceContainer(
                        new File(longInstanceBasePath.toString()))) {
            assertTrue(container.getDbOptions().dbLogDir().isEmpty());
        } finally {
            logFile.delete();
        }
    }

    // ------------------------------------------------------------------------
    //  ForSt local file directory
    // ------------------------------------------------------------------------

    /** This test checks the behavior for basic setting of local DB directories. */
    @Test
    public void testSetDbPath() throws Exception {
        final ForStStateBackend forStStateBackend = new ForStStateBackend();

        final String testDir1 = tempFolder.newFolder().getAbsolutePath();
        final String testDir2 = tempFolder.newFolder().getAbsolutePath();

        assertNull(forStStateBackend.getLocalDbStoragePaths());

        forStStateBackend.setLocalDbStoragePath(testDir1);
        assertArrayEquals(new String[] {testDir1}, forStStateBackend.getLocalDbStoragePaths());

        forStStateBackend.setLocalDbStoragePath(null);
        assertNull(forStStateBackend.getLocalDbStoragePaths());

        forStStateBackend.setLocalDbStoragePaths(testDir1, testDir2);
        assertArrayEquals(
                new String[] {testDir1, testDir2}, forStStateBackend.getLocalDbStoragePaths());

        final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());
        final ForStKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(forStStateBackend, env, IntSerializer.INSTANCE);

        try {
            File instanceBasePath = keyedBackend.getLocalBasePath();
            assertThat(
                    instanceBasePath.getAbsolutePath(),
                    anyOf(startsWith(testDir1), startsWith(testDir2)));

            //noinspection NullArgumentToVariableArgMethod
            forStStateBackend.setLocalDbStoragePaths(null);
            assertNull(forStStateBackend.getLocalDbStoragePaths());
        } finally {
            keyedBackend.dispose();
            keyedBackend.close();
            env.close();
        }
    }

    @Test
    public void testConfigureForStCompressionPerLevel() throws Exception {
        GlobalConfiguration.setStandardYaml(false);
        final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());
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
                forStStateBackend.createOptionsAndResourceContainer(tempFolder.newFile());
        ColumnFamilyOptions columnFamilyOptions = resourceContainer.getColumnOptions();
        assertArrayEquals(compressionTypes, columnFamilyOptions.compressionPerLevel().toArray());

        resourceContainer.close();
        env.close();
        GlobalConfiguration.setStandardYaml(true);
    }

    @Test
    public void testStoragePathWithFilePrefix() throws Exception {
        final File folder = tempFolder.newFolder();
        final String dbStoragePath = new Path(folder.toURI().toString()).toString();

        assertTrue(dbStoragePath.startsWith("file:"));

        testLocalDbPaths(dbStoragePath, folder);
    }

    @Test
    public void testWithDefaultFsSchemeNoStoragePath() throws Exception {
        try {
            // set the default file system scheme
            Configuration config = new Configuration();
            config.set(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "file:///mydomain.com:8020/flink");
            FileSystem.initialize(config);
            testLocalDbPaths(null, tempFolder.getRoot());
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }

    @Test
    public void testWithDefaultFsSchemeAbsoluteStoragePath() throws Exception {
        final File folder = tempFolder.newFolder();
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

        final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());
        ForStKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(forStBackend, env, IntSerializer.INSTANCE);

        try {
            File instanceBasePath = keyedBackend.getLocalBasePath();
            assertThat(
                    instanceBasePath.getAbsolutePath(), startsWith(expectedPath.getAbsolutePath()));

            //noinspection NullArgumentToVariableArgMethod
            forStBackend.setLocalDbStoragePaths(null);
            assertNull(forStBackend.getLocalDbStoragePaths());
        } finally {
            keyedBackend.dispose();
            keyedBackend.close();
            env.close();
        }
    }

    /** Validates that empty arguments for the local DB path are invalid. */
    @Test(expected = IllegalArgumentException.class)
    public void testSetEmptyPaths() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        forStStateBackend.setLocalDbStoragePaths();
    }

    /** Validates that schemes other than 'file:/' are not allowed. */
    @Test(expected = IllegalArgumentException.class)
    public void testNonFileSchemePath() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        forStStateBackend.setLocalDbStoragePath("hdfs:///some/path/to/perdition");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDbPathRelativePaths() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();
        forStStateBackend.setLocalDbStoragePath("relative/path");
    }

    @Test
    @Timeout(value = 60)
    public void testCleanRelocatedDbLogs() throws Exception {
        final File folder = tempFolder.newFolder();
        final File relocatedDBLogDir = tempFolder.newFolder("db_logs");
        final File logFile = new File(relocatedDBLogDir, "taskManager.log");
        Files.createFile(logFile.toPath());
        System.setProperty("log.file", logFile.getAbsolutePath());

        Configuration conf = new Configuration();
        conf.set(ForStConfigurableOptions.LOG_LEVEL, InfoLogLevel.DEBUG_LEVEL);
        conf.set(ForStConfigurableOptions.LOG_FILE_NUM, 4);
        conf.set(ForStConfigurableOptions.LOG_MAX_FILE_SIZE, MemorySize.parse("1kb"));
        final ForStStateBackend forStBackend =
                new ForStStateBackend().configure(conf, getClass().getClassLoader());
        final String dbStoragePath = new Path(folder.toURI().toString()).toString();
        forStBackend.setLocalDbStoragePath(dbStoragePath);

        final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());
        ForStKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(forStBackend, env, IntSerializer.INSTANCE);

        File localBasePath = keyedBackend.getLocalBasePath();
        File localForStPath = new File(localBasePath, "db");

        // avoid tests without relocate.
        Assume.assumeTrue(localForStPath.getAbsolutePath().length() <= 255 - "_LOG".length());

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
        assertEquals(1, relocatedDbLogs.length);
        assertEquals("taskManager.log", relocatedDbLogs[0].toFile().getName());
    }

    // ------------------------------------------------------------------------
    //  ForSt local file automatic from temp directories
    // ------------------------------------------------------------------------

    /**
     * This tests whether the ForSt backends uses the temp directories that are provided from the
     * {@link Environment} when no db storage path is set.
     */
    @Test
    public void testUseTempDirectories() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();

        File dir1 = tempFolder.newFolder();

        assertNull(forStStateBackend.getLocalDbStoragePaths());

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
            File instanceBasePath = keyedBackend.getLocalBasePath();
            assertThat(instanceBasePath.getAbsolutePath(), startsWith(dir1.getAbsolutePath()));
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
    @Category(FailsInGHAContainerWithRootUser.class)
    public void testFailWhenNoLocalStorageDir() throws Exception {
        final File targetDir = tempFolder.newFolder();
        Assume.assumeTrue(
                "Cannot mark directory non-writable", targetDir.setWritable(false, false));

        ForStStateBackend forStStateBackend = new ForStStateBackend();

        try (MockEnvironment env = getMockEnvironment(tempFolder.newFolder())) {
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
                assertTrue(e.getMessage().contains("No local storage directories available"));
                assertTrue(e.getMessage().contains(targetDir.getAbsolutePath()));
                hasFailure = true;
            }
            assertTrue(
                    "We must see a failure because no storaged directory is feasible.", hasFailure);
        } finally {
            //noinspection ResultOfMethodCallIgnored
            targetDir.setWritable(true, false);
        }
    }

    @Test
    public void testContinueOnSomeDbDirectoriesMissing() throws Exception {
        final File targetDir1 = tempFolder.newFolder();
        final File targetDir2 = tempFolder.newFolder();
        Assume.assumeTrue(
                "Cannot mark directory non-writable", targetDir1.setWritable(false, false));

        ForStStateBackend forStStateBackend = new ForStStateBackend();

        try (MockEnvironment env = getMockEnvironment(tempFolder.newFolder())) {
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
    public void testConfigurableOptionsFromConfig() throws Exception {
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

            try (ForStResourceContainer optionsContainer =
                    new ForStResourceContainer(configuration, null, null, null, null, false)) {

                DBOptions dbOptions = optionsContainer.getDbOptions();
                assertEquals(-1, dbOptions.maxOpenFiles());
                assertEquals(InfoLogLevel.DEBUG_LEVEL, dbOptions.infoLogLevel());
                assertEquals("/tmp/ForSt-logs/", dbOptions.dbLogDir());
                assertEquals(10, dbOptions.keepLogFileNum());
                assertEquals(2 * SizeUnit.MB, dbOptions.maxLogFileSize());

                ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();
                assertEquals(CompactionStyle.LEVEL, columnOptions.compactionStyle());
                assertTrue(columnOptions.levelCompactionDynamicLevelBytes());
                assertEquals(8 * SizeUnit.MB, columnOptions.targetFileSizeBase());
                assertEquals(128 * SizeUnit.MB, columnOptions.maxBytesForLevelBase());
                assertEquals(4, columnOptions.maxWriteBufferNumber());
                assertEquals(2, columnOptions.minWriteBufferNumberToMerge());
                assertEquals(64 * SizeUnit.MB, columnOptions.writeBufferSize());
                assertEquals(
                        Arrays.asList(
                                CompressionType.NO_COMPRESSION,
                                CompressionType.SNAPPY_COMPRESSION,
                                CompressionType.LZ4_COMPRESSION),
                        columnOptions.compressionPerLevel());

                BlockBasedTableConfig tableConfig =
                        (BlockBasedTableConfig) columnOptions.tableFormatConfig();
                assertEquals(4 * SizeUnit.KB, tableConfig.blockSize());
                assertEquals(8 * SizeUnit.KB, tableConfig.metadataBlockSize());
                assertEquals(512 * SizeUnit.MB, tableConfig.blockCacheSize());
                assertTrue(tableConfig.filterPolicy() instanceof BloomFilter);
            }
        }
    }

    @Test
    public void testOptionsFactory() throws Exception {
        ForStStateBackend forStStateBackend = new ForStStateBackend();

        // verify that user-defined options factory could be configured via config.yaml
        Configuration config = new Configuration();
        config.setString(ForStOptions.OPTIONS_FACTORY.key(), TestOptionsFactory.class.getName());
        config.setString(TestOptionsFactory.BACKGROUND_JOBS_OPTION.key(), "4");

        forStStateBackend = forStStateBackend.configure(config, getClass().getClassLoader());

        assertTrue(forStStateBackend.getForStOptions() instanceof TestOptionsFactory);

        try (ForStResourceContainer optionsContainer =
                forStStateBackend.createOptionsAndResourceContainer(null)) {
            DBOptions dbOptions = optionsContainer.getDbOptions();
            assertEquals(4, dbOptions.maxBackgroundJobs());
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
            assertEquals(CompactionStyle.FIFO, colCreated.compactionStyle());
        }
    }

    @Test
    public void testConfigurableOptions() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(ForStConfigurableOptions.COMPACTION_STYLE, CompactionStyle.UNIVERSAL);
        try (final ForStResourceContainer optionsContainer =
                new ForStResourceContainer(configuration, null, null, null, null, false)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertNotNull(columnFamilyOptions);
            assertEquals(CompactionStyle.UNIVERSAL, columnFamilyOptions.compactionStyle());
        }

        try (final ForStResourceContainer optionsContainer =
                new ForStResourceContainer(new Configuration(), null, null, null, null, false)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertNotNull(columnFamilyOptions);
            assertEquals(CompactionStyle.LEVEL, columnFamilyOptions.compactionStyle());
        }
    }

    @Test
    public void testPredefinedAndOptionsFactory() throws Exception {
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
            assertNotNull(columnFamilyOptions);
            assertEquals(CompactionStyle.UNIVERSAL, columnFamilyOptions.compactionStyle());
        }
    }

    // ------------------------------------------------------------------------
    //  Reconfiguration
    // ------------------------------------------------------------------------

    @Test
    public void testForStReconfigurationCopiesExistingValues() throws Exception {
        final ForStStateBackend original = new ForStStateBackend();
        final ForStOptionsFactory optionsFactory = new TestOptionsFactory();
        original.setForStOptions(optionsFactory);

        final String[] localDirs =
                new String[] {
                    tempFolder.newFolder().getAbsolutePath(),
                    tempFolder.newFolder().getAbsolutePath()
                };
        original.setLocalDbStoragePaths(localDirs);

        ForStStateBackend copy =
                original.configure(
                        new Configuration(), Thread.currentThread().getContextClassLoader());

        assertArrayEquals(original.getLocalDbStoragePaths(), copy.getLocalDbStoragePaths());
        assertEquals(original.getForStOptions(), copy.getForStOptions());
    }

    // ------------------------------------------------------------------------
    //  ForSt Memory Control
    // ------------------------------------------------------------------------

    @Test
    public void testDefaultMemoryControlParameters() {
        ForStMemoryConfiguration memSettings = new ForStMemoryConfiguration();
        assertTrue(memSettings.isUsingManagedMemory());
        assertFalse(memSettings.isUsingFixedMemoryPerSlot());
        assertEquals(
                ForStOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue(),
                memSettings.getHighPriorityPoolRatio(),
                0.0);
        assertEquals(
                ForStOptions.WRITE_BUFFER_RATIO.defaultValue(),
                memSettings.getWriteBufferRatio(),
                0.0);

        ForStMemoryConfiguration configured =
                ForStMemoryConfiguration.fromOtherAndConfiguration(
                        memSettings, new Configuration());
        assertTrue(configured.isUsingManagedMemory());
        assertFalse(configured.isUsingFixedMemoryPerSlot());
        assertEquals(
                ForStOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue(),
                configured.getHighPriorityPoolRatio(),
                0.0);
        assertEquals(
                ForStOptions.WRITE_BUFFER_RATIO.defaultValue(),
                configured.getWriteBufferRatio(),
                0.0);
    }

    @Test
    public void testConfigureManagedMemory() {
        final Configuration config = new Configuration();
        config.set(ForStOptions.USE_MANAGED_MEMORY, true);

        final ForStMemoryConfiguration memSettings =
                ForStMemoryConfiguration.fromOtherAndConfiguration(
                        new ForStMemoryConfiguration(), config);

        assertTrue(memSettings.isUsingManagedMemory());
    }

    @Test
    public void testConfigureIllegalMemoryControlParameters() {
        ForStMemoryConfiguration memSettings = new ForStMemoryConfiguration();

        verifySetParameter(() -> memSettings.setFixedMemoryPerSlot("-1B"));
        verifySetParameter(() -> memSettings.setHighPriorityPoolRatio(-0.1));
        verifySetParameter(() -> memSettings.setHighPriorityPoolRatio(1.1));
        verifySetParameter(() -> memSettings.setWriteBufferRatio(-0.1));
        verifySetParameter(() -> memSettings.setWriteBufferRatio(1.1));

        memSettings.setFixedMemoryPerSlot("128MB");
        memSettings.setWriteBufferRatio(0.6);
        memSettings.setHighPriorityPoolRatio(0.6);

        try {
            // sum of writeBufferRatio and highPriPoolRatio larger than 1.0
            memSettings.validate();
            fail("Expected an IllegalArgumentException.");
        } catch (IllegalArgumentException expected) {
            // expected exception
        }
    }

    @Test
    public void testRemoteDirectory() throws Exception {
        FileSystem.initialize(new Configuration(), null);
        Configuration configuration = new Configuration();
        configuration.set(ForStOptions.REMOTE_DIRECTORY, tempFolder.newFolder().toURI().toString());
        ForStStateBackend forStStateBackend =
                new ForStStateBackend().configure(configuration, null);
        ForStKeyedStateBackend<Integer> keyedBackend = null;
        try {
            keyedBackend =
                    createKeyedStateBackend(
                            forStStateBackend,
                            getMockEnvironment(tempFolder.newFolder()),
                            IntSerializer.INSTANCE);
            assertTrue(
                    keyedBackend
                            .getRemoteBasePath()
                            .toString()
                            .startsWith(configuration.get(ForStOptions.REMOTE_DIRECTORY)));
        } finally {
            if (keyedBackend != null) {
                keyedBackend.dispose();
                keyedBackend.close();
            }
        }
    }

    private void verifySetParameter(Runnable setter) {
        try {
            setter.run();
            fail("No expected IllegalArgumentException.");
        } catch (IllegalArgumentException expected) {
            // expected exception
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    static MockEnvironment getMockEnvironment(File tempDir) {
        return MockEnvironment.builder()
                .setUserCodeClassLoader(ForStStateBackendConfigTest.class.getClassLoader())
                .setTaskManagerRuntimeInfo(
                        new TestingTaskManagerRuntimeInfo(new Configuration(), tempDir))
                .build();
    }

    private void verifyIllegalArgument(ConfigOption<?> configOption, String configValue) {
        Configuration configuration = new Configuration();
        configuration.setString(configOption.key(), configValue);

        ForStStateBackend stateBackend = new ForStStateBackend();
        try {
            stateBackend.configure(configuration, null);
            fail("Not throwing expected IllegalArgumentException.");
        } catch (IllegalArgumentException e) {
            // ignored
        }
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
