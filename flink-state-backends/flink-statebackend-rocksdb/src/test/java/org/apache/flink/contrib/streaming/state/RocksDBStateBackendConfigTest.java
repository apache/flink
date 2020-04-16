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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.IOUtils;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for configuring the RocksDB State Backend.
 */
@SuppressWarnings("serial")
public class RocksDBStateBackendConfigTest {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	// ------------------------------------------------------------------------
	//  default values
	// ------------------------------------------------------------------------

	@Test
	public void testDefaultsInSync() throws Exception {
		final boolean defaultIncremental = CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

		RocksDBStateBackend backend = new RocksDBStateBackend(tempFolder.newFolder().toURI());
		assertEquals(defaultIncremental, backend.isIncrementalCheckpointsEnabled());
	}

	// ------------------------------------------------------------------------
	//  RocksDB local file directory
	// ------------------------------------------------------------------------

	/**
	 * This test checks the behavior for basic setting of local DB directories.
	 */
	@Test
	public void testSetDbPath() throws Exception {
		final RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(tempFolder.newFolder().toURI().toString());

		final String testDir1 = tempFolder.newFolder().getAbsolutePath();
		final String testDir2 = tempFolder.newFolder().getAbsolutePath();

		assertNull(rocksDbBackend.getDbStoragePaths());

		rocksDbBackend.setDbStoragePath(testDir1);
		assertArrayEquals(new String[] { testDir1 }, rocksDbBackend.getDbStoragePaths());

		rocksDbBackend.setDbStoragePath(null);
		assertNull(rocksDbBackend.getDbStoragePaths());

		rocksDbBackend.setDbStoragePaths(testDir1, testDir2);
		assertArrayEquals(new String[] { testDir1, testDir2 }, rocksDbBackend.getDbStoragePaths());

		final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());
		final RocksDBKeyedStateBackend<Integer> keyedBackend = createKeyedStateBackend(rocksDbBackend, env);

		try {
			File instanceBasePath = keyedBackend.getInstanceBasePath();
			assertThat(instanceBasePath.getAbsolutePath(), anyOf(startsWith(testDir1), startsWith(testDir2)));

			//noinspection NullArgumentToVariableArgMethod
			rocksDbBackend.setDbStoragePaths(null);
			assertNull(rocksDbBackend.getDbStoragePaths());
		}
		finally {
			IOUtils.closeQuietly(keyedBackend);
			keyedBackend.dispose();
			env.close();
		}
	}

	@Test
	public void testConfigureTimerService() throws Exception {

		final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());

		// Fix the option key string
		Assert.assertEquals("state.backend.rocksdb.timer-service.factory", RocksDBOptions.TIMER_SERVICE_FACTORY.key());

		// Fix the option value string and ensure all are covered
		Assert.assertEquals(2, RocksDBStateBackend.PriorityQueueStateType.values().length);
		Assert.assertEquals("ROCKSDB", RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString());
		Assert.assertEquals("HEAP", RocksDBStateBackend.PriorityQueueStateType.HEAP.toString());

		// Fix the default
		Assert.assertEquals(
			RocksDBStateBackend.PriorityQueueStateType.ROCKSDB,
			RocksDBOptions.TIMER_SERVICE_FACTORY.defaultValue());

		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(tempFolder.newFolder().toURI().toString());

		RocksDBKeyedStateBackend<Integer> keyedBackend = createKeyedStateBackend(rocksDbBackend, env);
		Assert.assertEquals(RocksDBPriorityQueueSetFactory.class, keyedBackend.getPriorityQueueFactory().getClass());
		keyedBackend.dispose();

		Configuration conf = new Configuration();
		conf.set(RocksDBOptions.TIMER_SERVICE_FACTORY, RocksDBStateBackend.PriorityQueueStateType.HEAP);

		rocksDbBackend = rocksDbBackend.configure(conf, Thread.currentThread().getContextClassLoader());
		keyedBackend = createKeyedStateBackend(rocksDbBackend, env);
		Assert.assertEquals(
			HeapPriorityQueueSetFactory.class,
			keyedBackend.getPriorityQueueFactory().getClass());
		keyedBackend.dispose();
		env.close();
	}

	/**
	 * Validates that user custom configuration from code should override the flink-conf.yaml.
	 */
	@Test
	public void testConfigureTimerServiceLoadingFromApplication() throws Exception {
		final MockEnvironment env = new MockEnvironmentBuilder().build();

		// priorityQueueStateType of the job backend
		final RocksDBStateBackend backend = new RocksDBStateBackend(tempFolder.newFolder().toURI().toString());
		backend.setPriorityQueueStateType(RocksDBStateBackend.PriorityQueueStateType.HEAP);

		// priorityQueueStateType in the cluster config
		final Configuration configFromConfFile = new Configuration();
		configFromConfFile.setString(
			RocksDBOptions.TIMER_SERVICE_FACTORY.key(),
			RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString());

		// configure final backend from job and cluster config
		final RocksDBStateBackend configuredRocksDBStateBackend = backend.configure(
			configFromConfFile,
			Thread.currentThread().getContextClassLoader());
		final RocksDBKeyedStateBackend<Integer> keyedBackend = createKeyedStateBackend(configuredRocksDBStateBackend, env);

		// priorityQueueStateType of the job backend should be preserved
		assertThat(keyedBackend.getPriorityQueueFactory(), instanceOf(HeapPriorityQueueSetFactory.class));

		keyedBackend.close();
		keyedBackend.dispose();
		env.close();
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
			config.setString(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "s3://mydomain.com:8020/flink");
			FileSystem.initialize(config);
			testLocalDbPaths(null, tempFolder.getRoot());
		}
		finally {
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
			config.setString(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "s3://mydomain.com:8020/flink");
			FileSystem.initialize(config);

			testLocalDbPaths(dbStoragePath, folder);
		}
		finally {
			FileSystem.initialize(new Configuration());
		}
	}

	private void testLocalDbPaths(String configuredPath, File expectedPath) throws Exception {
		final RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(tempFolder.newFolder().toURI().toString());
		rocksDbBackend.setDbStoragePath(configuredPath);

		final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());
		RocksDBKeyedStateBackend<Integer> keyedBackend = createKeyedStateBackend(rocksDbBackend, env);

		try {
			File instanceBasePath = keyedBackend.getInstanceBasePath();
			assertThat(instanceBasePath.getAbsolutePath(), startsWith(expectedPath.getAbsolutePath()));

			//noinspection NullArgumentToVariableArgMethod
			rocksDbBackend.setDbStoragePaths(null);
			assertNull(rocksDbBackend.getDbStoragePaths());
		} finally {
			IOUtils.closeQuietly(keyedBackend);
			keyedBackend.dispose();
			env.close();
		}
	}

	/**
	 * Validates that empty arguments for the local DB path are invalid.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testSetEmptyPaths() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);
		rocksDbBackend.setDbStoragePaths();
	}

	/**
	 * Validates that schemes other than 'file:/' are not allowed.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testNonFileSchemePath() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);
		rocksDbBackend.setDbStoragePath("hdfs:///some/path/to/perdition");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDbPathRelativePaths() throws Exception {
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(tempFolder.newFolder().toURI().toString());
		rocksDbBackend.setDbStoragePath("relative/path");
	}

	// ------------------------------------------------------------------------
	//  RocksDB local file automatic from temp directories
	// ------------------------------------------------------------------------

	/**
	 * This tests whether the RocksDB backends uses the temp directories that are provided
	 * from the {@link Environment} when no db storage path is set.
	 */
	@Test
	public void testUseTempDirectories() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		File dir1 = tempFolder.newFolder();
		File dir2 = tempFolder.newFolder();

		assertNull(rocksDbBackend.getDbStoragePaths());

		final MockEnvironment env = getMockEnvironment(dir1, dir2);
		RocksDBKeyedStateBackend<Integer> keyedBackend = (RocksDBKeyedStateBackend<Integer>) rocksDbBackend.
			createKeyedStateBackend(
				env,
				env.getJobID(),
				"test_op",
				IntSerializer.INSTANCE,
				1,
				new KeyGroupRange(0, 0),
				env.getTaskKvStateRegistry(),
				TtlTimeProvider.DEFAULT,
				new UnregisteredMetricsGroup(),
				Collections.emptyList(),
				new CloseableRegistry());

		try {
			File instanceBasePath = keyedBackend.getInstanceBasePath();
			assertThat(instanceBasePath.getAbsolutePath(), anyOf(startsWith(dir1.getAbsolutePath()), startsWith(dir2.getAbsolutePath())));
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
	public void testFailWhenNoLocalStorageDir() throws Exception {
		final File targetDir = tempFolder.newFolder();
		Assume.assumeTrue("Cannot mark directory non-writable", targetDir.setWritable(false, false));

		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		try (MockEnvironment env = getMockEnvironment(tempFolder.newFolder())) {
			rocksDbBackend.setDbStoragePath(targetDir.getAbsolutePath());

			boolean hasFailure = false;
			try {
				rocksDbBackend.createKeyedStateBackend(
					env,
					env.getJobID(),
					"foobar",
					IntSerializer.INSTANCE,
					1,
					new KeyGroupRange(0, 0),
					new KvStateRegistry().createTaskRegistry(env.getJobID(), new JobVertexID()),
					TtlTimeProvider.DEFAULT,
					new UnregisteredMetricsGroup(),
					Collections.emptyList(),
					new CloseableRegistry());
			} catch (Exception e) {
				assertTrue(e.getMessage().contains("No local storage directories available"));
				assertTrue(e.getMessage().contains(targetDir.getAbsolutePath()));
				hasFailure = true;
			}
			assertTrue("We must see a failure because no storaged directory is feasible.", hasFailure);
		} finally {
			//noinspection ResultOfMethodCallIgnored
			targetDir.setWritable(true, false);
		}
	}

	@Test
	public void testContinueOnSomeDbDirectoriesMissing() throws Exception {
		final File targetDir1 = tempFolder.newFolder();
		final File targetDir2 = tempFolder.newFolder();
		Assume.assumeTrue("Cannot mark directory non-writable", targetDir1.setWritable(false, false));

		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		try (MockEnvironment env = getMockEnvironment(tempFolder.newFolder())) {
			rocksDbBackend.setDbStoragePaths(targetDir1.getAbsolutePath(), targetDir2.getAbsolutePath());

			try {
				AbstractKeyedStateBackend<Integer> keyedStateBackend = rocksDbBackend.createKeyedStateBackend(
					env,
					env.getJobID(),
					"foobar",
					IntSerializer.INSTANCE,
					1,
					new KeyGroupRange(0, 0),
					new KvStateRegistry().createTaskRegistry(env.getJobID(), new JobVertexID()),
					TtlTimeProvider.DEFAULT,
					new UnregisteredMetricsGroup(),
					Collections.emptyList(),
					new CloseableRegistry());

				IOUtils.closeQuietly(keyedStateBackend);
				keyedStateBackend.dispose();
			}
			catch (Exception e) {
				e.printStackTrace();
				fail("Backend initialization failed even though some paths were available");
			}
		} finally {
			//noinspection ResultOfMethodCallIgnored
			targetDir1.setWritable(true, false);
		}
	}

	// ------------------------------------------------------------------------
	//  RocksDB Options
	// ------------------------------------------------------------------------

	@Test
	public void testPredefinedOptions() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		// verify that we would use PredefinedOptions.DEFAULT by default.
		assertEquals(PredefinedOptions.DEFAULT, rocksDbBackend.getPredefinedOptions());

		// verify that user could configure predefined options via flink-conf.yaml
		Configuration configuration = new Configuration();
		configuration.setString(RocksDBOptions.PREDEFINED_OPTIONS, PredefinedOptions.FLASH_SSD_OPTIMIZED.name());
		rocksDbBackend = new RocksDBStateBackend(checkpointPath);
		rocksDbBackend = rocksDbBackend.configure(configuration, getClass().getClassLoader());
		assertEquals(PredefinedOptions.FLASH_SSD_OPTIMIZED, rocksDbBackend.getPredefinedOptions());

		// verify that predefined options could be set programmatically and override pre-configured one.
		rocksDbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
		assertEquals(PredefinedOptions.SPINNING_DISK_OPTIMIZED, rocksDbBackend.getPredefinedOptions());
	}

	@Test
	public void testSetConfigurableOptions() throws Exception  {
		DefaultConfigurableOptionsFactory customizedOptions = new DefaultConfigurableOptionsFactory()
			.setMaxBackgroundThreads(4)
			.setMaxOpenFiles(-1)
			.setCompactionStyle(CompactionStyle.LEVEL)
			.setUseDynamicLevelSize(true)
			.setTargetFileSizeBase("4MB")
			.setMaxSizeLevelBase("128 mb")
			.setWriteBufferSize("128 MB")
			.setMaxWriteBufferNumber(4)
			.setMinWriteBufferNumberToMerge(3)
			.setBlockSize("64KB")
			.setBlockCacheSize("512mb");

		try (RocksDBResourceContainer optionsContainer =
				new RocksDBResourceContainer(PredefinedOptions.DEFAULT, customizedOptions)) {

			DBOptions dbOptions = optionsContainer.getDbOptions();
			assertEquals(-1, dbOptions.maxOpenFiles());

			ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();
			assertEquals(CompactionStyle.LEVEL, columnOptions.compactionStyle());
			assertTrue(columnOptions.levelCompactionDynamicLevelBytes());
			assertEquals(4 * SizeUnit.MB, columnOptions.targetFileSizeBase());
			assertEquals(128 * SizeUnit.MB, columnOptions.maxBytesForLevelBase());
			assertEquals(4, columnOptions.maxWriteBufferNumber());
			assertEquals(3, columnOptions.minWriteBufferNumberToMerge());

			BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) columnOptions.tableFormatConfig();
			assertEquals(64 * SizeUnit.KB, tableConfig.blockSize());
			assertEquals(512 * SizeUnit.MB, tableConfig.blockCacheSize());
		}
	}

	@Test
	public void testConfigurableOptionsFromConfig() throws Exception {
		Configuration configuration = new Configuration();
		DefaultConfigurableOptionsFactory defaultOptionsFactory = new DefaultConfigurableOptionsFactory();
		assertTrue(defaultOptionsFactory.configure(configuration).getConfiguredOptions().isEmpty());

		// verify illegal configuration
		{
			verifyIllegalArgument(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, "-1");
			verifyIllegalArgument(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, "-1");
			verifyIllegalArgument(RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE, "-1");

			verifyIllegalArgument(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE, "0KB");
			verifyIllegalArgument(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE, "1BB");
			verifyIllegalArgument(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, "-1KB");
			verifyIllegalArgument(RocksDBConfigurableOptions.BLOCK_SIZE, "0MB");
			verifyIllegalArgument(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, "0");

			verifyIllegalArgument(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, "1");

			verifyIllegalArgument(RocksDBConfigurableOptions.COMPACTION_STYLE, "LEV");
		}

		// verify legal configuration
		{
			configuration.setString(RocksDBConfigurableOptions.COMPACTION_STYLE.key(), "level");
			configuration.setString(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE.key(), "TRUE");
			configuration.setString(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE.key(), "8 mb");
			configuration.setString(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE.key(), "128MB");
			configuration.setString(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS.key(), "4");
			configuration.setString(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER.key(), "4");
			configuration.setString(RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key(), "2");
			configuration.setString(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE.key(), "64 MB");
			configuration.setString(RocksDBConfigurableOptions.BLOCK_SIZE.key(), "4 kb");
			configuration.setString(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE.key(), "512 mb");

			DefaultConfigurableOptionsFactory optionsFactory = new DefaultConfigurableOptionsFactory();
			optionsFactory.configure(configuration);

			try (RocksDBResourceContainer optionsContainer =
					new RocksDBResourceContainer(PredefinedOptions.DEFAULT, optionsFactory)) {

				DBOptions dbOptions = optionsContainer.getDbOptions();
				assertEquals(-1, dbOptions.maxOpenFiles());

				ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();
				assertEquals(CompactionStyle.LEVEL, columnOptions.compactionStyle());
				assertTrue(columnOptions.levelCompactionDynamicLevelBytes());
				assertEquals(8 * SizeUnit.MB, columnOptions.targetFileSizeBase());
				assertEquals(128 * SizeUnit.MB, columnOptions.maxBytesForLevelBase());
				assertEquals(4, columnOptions.maxWriteBufferNumber());
				assertEquals(2, columnOptions.minWriteBufferNumberToMerge());
				assertEquals(64 * SizeUnit.MB, columnOptions.writeBufferSize());

				BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) columnOptions.tableFormatConfig();
				assertEquals(4 * SizeUnit.KB, tableConfig.blockSize());
				assertEquals(512 * SizeUnit.MB, tableConfig.blockCacheSize());
			}
		}
	}

	@Test
	public void testOptionsFactory() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		// verify that user-defined options factory could be configured via flink-conf.yaml
		Configuration config = new Configuration();
		config.setString(RocksDBOptions.OPTIONS_FACTORY.key(), TestOptionsFactory.class.getName());
		config.setString(TestOptionsFactory.BACKGROUND_JOBS_OPTION.key(), "4");

		rocksDbBackend = rocksDbBackend.configure(config, getClass().getClassLoader());

		assertTrue(rocksDbBackend.getRocksDBOptions() instanceof TestOptionsFactory);
		assertTrue(rocksDbBackend.getOptions() instanceof TestOptionsFactory);

		try (RocksDBResourceContainer optionsContainer = rocksDbBackend.createOptionsAndResourceContainer()) {
			DBOptions dbOptions = optionsContainer.getDbOptions();
			assertEquals(4, dbOptions.maxBackgroundJobs());
		}

		// verify that user-defined options factory could be set programmatically and override pre-configured one.
		rocksDbBackend.setRocksDBOptions(new RocksDBOptionsFactory() {
			@Override
			public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				return currentOptions;
			}

			@Override
			public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				return currentOptions.setCompactionStyle(CompactionStyle.FIFO);
			}
		});

		try (RocksDBResourceContainer optionsContainer = rocksDbBackend.createOptionsAndResourceContainer()) {
			ColumnFamilyOptions colCreated = optionsContainer.getColumnOptions();
			assertEquals(CompactionStyle.FIFO, colCreated.compactionStyle());
		}
	}

	@Test
	public void testPredefinedAndOptionsFactory() throws Exception {
		final RocksDBOptionsFactory optionsFactory = new RocksDBOptionsFactory() {
			@Override
			public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				return currentOptions;
			}

			@Override
			public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				return currentOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
			}
		};

		try (final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer(
				PredefinedOptions.SPINNING_DISK_OPTIMIZED, optionsFactory)) {

			final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
			assertNotNull(columnFamilyOptions);
			assertEquals(CompactionStyle.UNIVERSAL, columnFamilyOptions.compactionStyle());
		}
	}

	@Test
	public void testPredefinedOptionsEnum() {
		ArrayList<AutoCloseable> handlesToClose = new ArrayList<>();
		for (PredefinedOptions o : PredefinedOptions.values()) {
			try (DBOptions opt = o.createDBOptions(handlesToClose)) {
				assertNotNull(opt);
			}
		}
		handlesToClose.forEach(IOUtils::closeQuietly);
		handlesToClose.clear();
	}

	// ------------------------------------------------------------------------
	//  Reconfiguration
	// ------------------------------------------------------------------------

	@Test
	public void testRocksDbReconfigurationCopiesExistingValues() throws Exception {
		final FsStateBackend checkpointBackend = new FsStateBackend(tempFolder.newFolder().toURI().toString());
		final boolean incremental = !CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

		final RocksDBStateBackend original = new RocksDBStateBackend(checkpointBackend, incremental);

		// these must not be the default options
		final PredefinedOptions predOptions = PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM;
		assertNotEquals(predOptions, original.getPredefinedOptions());
		original.setPredefinedOptions(predOptions);

		final RocksDBOptionsFactory optionsFactory = mock(RocksDBOptionsFactory.class);
		original.setRocksDBOptions(optionsFactory);

		final String[] localDirs = new String[] {
				tempFolder.newFolder().getAbsolutePath(), tempFolder.newFolder().getAbsolutePath() };
		original.setDbStoragePaths(localDirs);

		RocksDBStateBackend copy = original.configure(new Configuration(), Thread.currentThread().getContextClassLoader());

		assertEquals(original.isIncrementalCheckpointsEnabled(), copy.isIncrementalCheckpointsEnabled());
		assertArrayEquals(original.getDbStoragePaths(), copy.getDbStoragePaths());
		assertEquals(original.getOptions(), copy.getOptions());
		assertEquals(original.getPredefinedOptions(), copy.getPredefinedOptions());

		FsStateBackend copyCheckpointBackend = (FsStateBackend) copy.getCheckpointBackend();
		assertEquals(checkpointBackend.getCheckpointPath(), copyCheckpointBackend.getCheckpointPath());
		assertEquals(checkpointBackend.getSavepointPath(), copyCheckpointBackend.getSavepointPath());
	}

	// ------------------------------------------------------------------------
	//  RocksDB Memory Control
	// ------------------------------------------------------------------------

	@Test
	public void testDefaultMemoryControlParameters() {
		RocksDBMemoryConfiguration memSettings = new RocksDBMemoryConfiguration();
		assertTrue(memSettings.isUsingManagedMemory());
		assertFalse(memSettings.isUsingFixedMemoryPerSlot());
		assertEquals(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue(), memSettings.getHighPriorityPoolRatio(), 0.0);
		assertEquals(RocksDBOptions.WRITE_BUFFER_RATIO.defaultValue(), memSettings.getWriteBufferRatio(), 0.0);

		RocksDBMemoryConfiguration configured = RocksDBMemoryConfiguration.fromOtherAndConfiguration(memSettings, new Configuration());
		assertTrue(configured.isUsingManagedMemory());
		assertFalse(configured.isUsingFixedMemoryPerSlot());
		assertEquals(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue(), configured.getHighPriorityPoolRatio(), 0.0);
		assertEquals(RocksDBOptions.WRITE_BUFFER_RATIO.defaultValue(), configured.getWriteBufferRatio(), 0.0);
	}

	@Test
	public void testConfigureManagedMemory() {
		final Configuration config = new Configuration();
		config.setBoolean(RocksDBOptions.USE_MANAGED_MEMORY, true);

		final RocksDBMemoryConfiguration memSettings = RocksDBMemoryConfiguration.fromOtherAndConfiguration(
			new RocksDBMemoryConfiguration(), config);

		assertTrue(memSettings.isUsingManagedMemory());
	}

	@Test
	public void testConfigureIllegalMemoryControlParameters() {
		RocksDBMemoryConfiguration memSettings = new RocksDBMemoryConfiguration();

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

	private void verifySetParameter(Runnable setter) {
		try {
			setter.run();
			fail("No expected IllegalArgumentException.");
		} catch (IllegalArgumentException expected) {
			// expected exception
		}
	}

	// ------------------------------------------------------------------------
	//  Contained Non-partitioned State Backend
	// ------------------------------------------------------------------------

	@Test
	public void testCallsForwardedToNonPartitionedBackend() throws Exception {
		StateBackend storageBackend = new MemoryStateBackend();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(storageBackend);
		assertEquals(storageBackend, rocksDbBackend.getCheckpointBackend());
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	static RocksDBKeyedStateBackend<Integer> createKeyedStateBackend(
			RocksDBStateBackend rocksDbBackend, Environment env) throws Exception {

		return (RocksDBKeyedStateBackend<Integer>) rocksDbBackend.createKeyedStateBackend(
			env,
			env.getJobID(),
			"test_op",
			IntSerializer.INSTANCE,
			1,
			new KeyGroupRange(0, 0),
			env.getTaskKvStateRegistry(),
			TtlTimeProvider.DEFAULT,
			new UnregisteredMetricsGroup(),
			Collections.emptyList(),
			new CloseableRegistry());
	}

	static MockEnvironment getMockEnvironment(File... tempDirs) {
		final String[] tempDirStrings = new String[tempDirs.length];
		for (int i = 0; i < tempDirs.length; i++) {
			tempDirStrings[i] = tempDirs[i].getAbsolutePath();
		}

		IOManager ioMan = mock(IOManager.class);
		when(ioMan.getSpillingDirectories()).thenReturn(tempDirs);

		return MockEnvironment.builder()
			.setUserCodeClassLoader(RocksDBStateBackendConfigTest.class.getClassLoader())
			.setTaskManagerRuntimeInfo(new TestingTaskManagerRuntimeInfo(new Configuration(), tempDirStrings))
			.setIOManager(ioMan).build();
	}

	private void verifyIllegalArgument(
			ConfigOption<?> configOption,
			String configValue) {
		Configuration configuration = new Configuration();
		configuration.setString(configOption.key(), configValue);

		DefaultConfigurableOptionsFactory optionsFactory = new DefaultConfigurableOptionsFactory();
		try {
			optionsFactory.configure(configuration);
			fail("Not throwing expected IllegalArgumentException.");
		} catch (IllegalArgumentException e) {
			// ignored
		}
	}

	/**
	 * An implementation of options factory for testing.
	 */
	public static class TestOptionsFactory implements ConfigurableRocksDBOptionsFactory {
		public static final ConfigOption<Integer> BACKGROUND_JOBS_OPTION =
			ConfigOptions.key("my.custom.rocksdb.backgroundJobs")
				.intType()
				.defaultValue(2);

		private int backgroundJobs = BACKGROUND_JOBS_OPTION.defaultValue();

		@Override
		public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
			return currentOptions.setMaxBackgroundJobs(backgroundJobs);
		}

		@Override
		public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
			return currentOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
		}

		@Override
		public RocksDBOptionsFactory configure(ReadableConfig configuration) {
			this.backgroundJobs = configuration.get(BACKGROUND_JOBS_OPTION);
			return this;
		}
	}
}
