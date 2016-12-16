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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.OperatingSystem;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;

import java.io.File;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for configuring the RocksDB State Backend
 */
@SuppressWarnings("serial")
public class RocksDBStateBackendConfigTest {


	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void checkOperatingSystem() {
		Assume.assumeTrue("This test can't run successfully on Windows.", !OperatingSystem.isWindows());
	}

	// ------------------------------------------------------------------------
	//  RocksDB local file directory
	// ------------------------------------------------------------------------

	@Test
	public void testSetDbPath() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		File testDir1 = tempFolder.newFolder();
		File testDir2 = tempFolder.newFolder();

		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		assertNull(rocksDbBackend.getDbStoragePaths());

		rocksDbBackend.setDbStoragePath(testDir1.getAbsolutePath());
		assertArrayEquals(new String[] { testDir1.getAbsolutePath() }, rocksDbBackend.getDbStoragePaths());

		rocksDbBackend.setDbStoragePath(null);
		assertNull(rocksDbBackend.getDbStoragePaths());

		rocksDbBackend.setDbStoragePaths(testDir1.getAbsolutePath(), testDir2.getAbsolutePath());
		assertArrayEquals(new String[] { testDir1.getAbsolutePath(), testDir2.getAbsolutePath() }, rocksDbBackend.getDbStoragePaths());

		Environment env = getMockEnvironment();
		RocksDBKeyedStateBackend<Integer> keyedBackend = (RocksDBKeyedStateBackend<Integer>) rocksDbBackend.
				createKeyedStateBackend(
						env,
						env.getJobID(),
						"test_op",
						IntSerializer.INSTANCE,
						1,
						new KeyGroupRange(0, 0),
						env.getTaskKvStateRegistry());


		File instanceBasePath = keyedBackend.getInstanceBasePath();
		assertThat(instanceBasePath.getAbsolutePath(), anyOf(startsWith(testDir1.getAbsolutePath()), startsWith(testDir2.getAbsolutePath())));

		//noinspection NullArgumentToVariableArgMethod
		rocksDbBackend.setDbStoragePaths(null);
		assertNull(rocksDbBackend.getDbStoragePaths());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetNullPaths() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);
		rocksDbBackend.setDbStoragePaths();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNonFileSchemePath() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);
		rocksDbBackend.setDbStoragePath("hdfs:///some/path/to/perdition");
	}

	// ------------------------------------------------------------------------
	//  RocksDB local file automatic from temp directories
	// ------------------------------------------------------------------------

	/**
	 * This tests whether the RocksDB backends uses the temp directories that are provided
	 * from the {@link Environment} when no db storage path is set.
	 *
	 * @throws Exception
	 */
	@Test
	public void testUseTempDirectories() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		File dir1 = tempFolder.newFolder();
		File dir2 = tempFolder.newFolder();

		File[] tempDirs = new File[] { dir1, dir2 };

		assertNull(rocksDbBackend.getDbStoragePaths());

		Environment env = getMockEnvironment(tempDirs);
		RocksDBKeyedStateBackend<Integer> keyedBackend = (RocksDBKeyedStateBackend<Integer>) rocksDbBackend.
				createKeyedStateBackend(
						env,
						env.getJobID(),
						"test_op",
						IntSerializer.INSTANCE,
						1,
						new KeyGroupRange(0, 0),
						env.getTaskKvStateRegistry());


		File instanceBasePath = keyedBackend.getInstanceBasePath();
		assertThat(instanceBasePath.getAbsolutePath(), anyOf(startsWith(dir1.getAbsolutePath()), startsWith(dir2.getAbsolutePath())));
	}

	// ------------------------------------------------------------------------
	//  RocksDB local file directory initialization
	// ------------------------------------------------------------------------

	@Test
	public void testFailWhenNoLocalStorageDir() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);
		File targetDir = tempFolder.newFolder();

		try {
			if (!targetDir.setWritable(false, false)) {
				System.err.println("Cannot execute 'testFailWhenNoLocalStorageDir' because cannot mark directory non-writable");
				return;
			}

			rocksDbBackend.setDbStoragePath(targetDir.getAbsolutePath());

			boolean hasFailure = false;
			try {
				Environment env = getMockEnvironment();
				rocksDbBackend.createKeyedStateBackend(
						env,
						env.getJobID(),
						"foobar",
						IntSerializer.INSTANCE,
						1,
						new KeyGroupRange(0, 0),
						new KvStateRegistry().createTaskRegistry(env.getJobID(), new JobVertexID()));
			}
			catch (Exception e) {
				assertTrue(e.getMessage().contains("No local storage directories available"));
				assertTrue(e.getMessage().contains(targetDir.getAbsolutePath()));
				hasFailure = true;
			}
			assertTrue("We must see a failure because no storaged directory is feasible.", hasFailure);
		}
		finally {
			//noinspection ResultOfMethodCallIgnored
			targetDir.setWritable(true, false);
			FileUtils.deleteDirectory(targetDir);
		}
	}

	@Test
	public void testContinueOnSomeDbDirectoriesMissing() throws Exception {
		File targetDir1 = tempFolder.newFolder();
		File targetDir2 = tempFolder.newFolder();

		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		try {

			if (!targetDir1.setWritable(false, false)) {
				System.err.println("Cannot execute 'testContinueOnSomeDbDirectoriesMissing' because cannot mark directory non-writable");
				return;
			}

			rocksDbBackend.setDbStoragePaths(targetDir1.getAbsolutePath(), targetDir2.getAbsolutePath());

			try {
				Environment env = getMockEnvironment();
				rocksDbBackend.createKeyedStateBackend(
						env,
						env.getJobID(),
						"foobar",
						IntSerializer.INSTANCE,
						1,
						new KeyGroupRange(0, 0),
						new KvStateRegistry().createTaskRegistry(env.getJobID(), new JobVertexID()));
			}
			catch (Exception e) {
				e.printStackTrace();
				fail("Backend initialization failed even though some paths were available");
			}
		} finally {
			//noinspection ResultOfMethodCallIgnored
			targetDir1.setWritable(true, false);
			FileUtils.deleteDirectory(targetDir1);
			FileUtils.deleteDirectory(targetDir2);
		}
	}

	// ------------------------------------------------------------------------
	//  RocksDB Options
	// ------------------------------------------------------------------------

	@Test
	public void testPredefinedOptions() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		assertEquals(PredefinedOptions.DEFAULT, rocksDbBackend.getPredefinedOptions());

		rocksDbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
		assertEquals(PredefinedOptions.SPINNING_DISK_OPTIMIZED, rocksDbBackend.getPredefinedOptions());

		DBOptions opt1 = rocksDbBackend.getDbOptions();
		DBOptions opt2 = rocksDbBackend.getDbOptions();

		assertEquals(opt1, opt2);

		ColumnFamilyOptions columnOpt1 = rocksDbBackend.getColumnOptions();
		ColumnFamilyOptions columnOpt2 = rocksDbBackend.getColumnOptions();

		assertEquals(columnOpt1, columnOpt2);

		assertEquals(CompactionStyle.LEVEL, columnOpt1.compactionStyle());
	}

	@Test
	public void testOptionsFactory() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		rocksDbBackend.setOptions(new OptionsFactory() {
			@Override
			public DBOptions createDBOptions(DBOptions currentOptions) {
				return currentOptions;
			}

			@Override
			public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
				return currentOptions.setCompactionStyle(CompactionStyle.FIFO);
			}
		});

		assertNotNull(rocksDbBackend.getOptions());
		assertEquals(CompactionStyle.FIFO, rocksDbBackend.getColumnOptions().compactionStyle());
	}

	@Test
	public void testPredefinedAndOptionsFactory() throws Exception {
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

		assertEquals(PredefinedOptions.DEFAULT, rocksDbBackend.getPredefinedOptions());

		rocksDbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
		rocksDbBackend.setOptions(new OptionsFactory() {
			@Override
			public DBOptions createDBOptions(DBOptions currentOptions) {
				return currentOptions;
			}

			@Override
			public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
				return currentOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
			}
		});

		assertEquals(PredefinedOptions.SPINNING_DISK_OPTIMIZED, rocksDbBackend.getPredefinedOptions());
		assertNotNull(rocksDbBackend.getOptions());
		assertEquals(CompactionStyle.UNIVERSAL, rocksDbBackend.getColumnOptions().compactionStyle());
	}

	@Test
	public void testPredefinedOptionsEnum() {
		for (PredefinedOptions o : PredefinedOptions.values()) {
			DBOptions opt = o.createDBOptions();
			try {
				assertNotNull(opt);
			} finally {
				opt.dispose();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Contained Non-partitioned State Backend
	// ------------------------------------------------------------------------

	@Test
	public void testCallsForwardedToNonPartitionedBackend() throws Exception {
		AbstractStateBackend nonPartBackend = mock(AbstractStateBackend.class);
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(nonPartBackend);

		Environment env = getMockEnvironment();
		rocksDbBackend.createStreamFactory(env.getJobID(), "foobar");

		verify(nonPartBackend, times(1)).createStreamFactory(any(JobID.class), anyString());
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static Environment getMockEnvironment() {
		return getMockEnvironment(new File[] { new File(System.getProperty("java.io.tmpdir")) });
	}

	private static Environment getMockEnvironment(File[] tempDirs) {
		final String[] tempDirStrings = new String[tempDirs.length];
		for (int i = 0; i < tempDirs.length; i++) {
			tempDirStrings[i] = tempDirs[i].getAbsolutePath();
		}

		IOManager ioMan = mock(IOManager.class);
		when(ioMan.getSpillingDirectories()).thenReturn(tempDirs);

		Environment env = mock(Environment.class);
		when(env.getJobID()).thenReturn(new JobID());
		when(env.getUserClassLoader()).thenReturn(RocksDBStateBackendConfigTest.class.getClassLoader());
		when(env.getIOManager()).thenReturn(ioMan);
		when(env.getTaskKvStateRegistry()).thenReturn(new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()));

		TaskInfo taskInfo = mock(TaskInfo.class);
		when(env.getTaskInfo()).thenReturn(taskInfo);
		when(taskInfo.getIndexOfThisSubtask()).thenReturn(0);

		TaskManagerRuntimeInfo tmInfo = new TaskManagerRuntimeInfo("localhost", new Configuration(), tempDirStrings);
		when(env.getTaskManagerInfo()).thenReturn(tmInfo);

		return env;
	}
}
