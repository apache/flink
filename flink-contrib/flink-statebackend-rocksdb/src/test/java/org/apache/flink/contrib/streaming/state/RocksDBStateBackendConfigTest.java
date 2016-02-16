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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.AbstractStateBackend;

import org.junit.Test;

import org.rocksdb.CompactionStyle;
import org.rocksdb.Options;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for configuring the RocksDB State Backend 
 */
@SuppressWarnings("serial")
public class RocksDBStateBackendConfigTest {
	
	private static final String TEMP_URI = new File(System.getProperty("java.io.tmpdir")).toURI().toString();

	// ------------------------------------------------------------------------
	//  RocksDB local file directory
	// ------------------------------------------------------------------------
	
	@Test
	public void testSetDbPath() throws Exception {
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);
		
		assertNull(rocksDbBackend.getDbStoragePaths());
		
		rocksDbBackend.setDbStoragePath("/abc/def");
		assertArrayEquals(new String[] { "/abc/def" }, rocksDbBackend.getDbStoragePaths());

		rocksDbBackend.setDbStoragePath(null);
		assertNull(rocksDbBackend.getDbStoragePaths());

		rocksDbBackend.setDbStoragePaths("/abc/def", "/uvw/xyz");
		assertArrayEquals(new String[] { "/abc/def", "/uvw/xyz" }, rocksDbBackend.getDbStoragePaths());

		//noinspection NullArgumentToVariableArgMethod
		rocksDbBackend.setDbStoragePaths(null);
		assertNull(rocksDbBackend.getDbStoragePaths());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetNullPaths() throws Exception {
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);
		rocksDbBackend.setDbStoragePaths();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNonFileSchemePath() throws Exception {
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);
		rocksDbBackend.setDbStoragePath("hdfs:///some/path/to/perdition");
	}

	// ------------------------------------------------------------------------
	//  RocksDB local file automatic from temp directories
	// ------------------------------------------------------------------------
	
	@Test
	public void testUseTempDirectories() throws Exception {
		File dir1 = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
		File dir2 = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

		File[] tempDirs = new File[] { dir1, dir2 };
		
		try {
			assertTrue(dir1.mkdirs());
			assertTrue(dir2.mkdirs());

			RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);
			assertNull(rocksDbBackend.getDbStoragePaths());
			
			rocksDbBackend.initializeForJob(getMockEnvironment(tempDirs), "foobar", IntSerializer.INSTANCE);
			assertArrayEquals(tempDirs, rocksDbBackend.getStoragePaths());
		}
		finally {
			FileUtils.deleteDirectory(dir1);
			FileUtils.deleteDirectory(dir2);
		}
	}
	
	// ------------------------------------------------------------------------
	//  RocksDB local file directory initialization
	// ------------------------------------------------------------------------

	@Test
	public void testFailWhenNoLocalStorageDir() throws Exception {
		File targetDir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
		try {
			assertTrue(targetDir.mkdirs());
			
			if (!targetDir.setWritable(false, false)) {
				System.err.println("Cannot execute 'testFailWhenNoLocalStorageDir' because cannot mark directory non-writable");
				return;
			}
			
			RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);
			rocksDbBackend.setDbStoragePath(targetDir.getAbsolutePath());
			
			try {
				rocksDbBackend.initializeForJob(getMockEnvironment(), "foobar", IntSerializer.INSTANCE);
			}
			catch (Exception e) {
				assertTrue(e.getMessage().contains("No local storage directories available"));
				assertTrue(e.getMessage().contains(targetDir.getAbsolutePath()));
			}
		}
		finally {
			//noinspection ResultOfMethodCallIgnored
			targetDir.setWritable(true, false);
			FileUtils.deleteDirectory(targetDir);
		}
	}

	@Test
	public void testContinueOnSomeDbDirectoriesMissing() throws Exception {
		File targetDir1 = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
		File targetDir2 = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
		
		try {
			assertTrue(targetDir1.mkdirs());
			assertTrue(targetDir2.mkdirs());
	
			if (!targetDir1.setWritable(false, false)) {
				System.err.println("Cannot execute 'testContinueOnSomeDbDirectoriesMissing' because cannot mark directory non-writable");
				return;
			}
	
			RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);
			rocksDbBackend.setDbStoragePaths(targetDir1.getAbsolutePath(), targetDir2.getAbsolutePath());
	
			try {
				rocksDbBackend.initializeForJob(getMockEnvironment(), "foobar", IntSerializer.INSTANCE);
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
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);
		
		assertEquals(PredefinedOptions.DEFAULT, rocksDbBackend.getPredefinedOptions());
		
		rocksDbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
		assertEquals(PredefinedOptions.SPINNING_DISK_OPTIMIZED, rocksDbBackend.getPredefinedOptions());
		
		Options opt1 = rocksDbBackend.getRocksDBOptions();
		Options opt2 = rocksDbBackend.getRocksDBOptions();
		
		assertEquals(opt1, opt2);
		
		assertEquals(CompactionStyle.LEVEL, opt1.compactionStyle());
	}

	@Test
	public void testOptionsFactory() throws Exception {
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);
		
		rocksDbBackend.setOptions(new OptionsFactory() {
			@Override
			public Options createOptions(Options currentOptions) {
				return currentOptions.setCompactionStyle(CompactionStyle.FIFO);
			}
		});
		
		assertNotNull(rocksDbBackend.getOptions());
		assertEquals(CompactionStyle.FIFO, rocksDbBackend.getRocksDBOptions().compactionStyle());
	}

	@Test
	public void testPredefinedAndOptionsFactory() throws Exception {
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI);

		assertEquals(PredefinedOptions.DEFAULT, rocksDbBackend.getPredefinedOptions());

		rocksDbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
		rocksDbBackend.setOptions(new OptionsFactory() {
			@Override
			public Options createOptions(Options currentOptions) {
				return currentOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
			}
		});
		
		assertEquals(PredefinedOptions.SPINNING_DISK_OPTIMIZED, rocksDbBackend.getPredefinedOptions());
		assertNotNull(rocksDbBackend.getOptions());
		assertEquals(CompactionStyle.UNIVERSAL, rocksDbBackend.getRocksDBOptions().compactionStyle());
	}

	@Test
	public void testPredefinedOptionsEnum() {
		for (PredefinedOptions o : PredefinedOptions.values()) {
			Options opt = o.createOptions();
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
		RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(TEMP_URI, nonPartBackend);

		rocksDbBackend.initializeForJob(getMockEnvironment(), "foo", IntSerializer.INSTANCE);
		verify(nonPartBackend, times(1)).initializeForJob(any(Environment.class), anyString(), any(TypeSerializer.class));

		rocksDbBackend.disposeAllStateForCurrentJob();
		verify(nonPartBackend, times(1)).disposeAllStateForCurrentJob();
		
		rocksDbBackend.close();
		verify(nonPartBackend, times(1)).close();
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static Environment getMockEnvironment() {
		return getMockEnvironment(new File[] { new File(System.getProperty("java.io.tmpdir")) });
	}
	
	private static Environment getMockEnvironment(File[] tempDirs) {
		IOManager ioMan = mock(IOManager.class);
		when(ioMan.getSpillingDirectories()).thenReturn(tempDirs);
		
		Environment env = mock(Environment.class);
		when(env.getJobID()).thenReturn(new JobID());
		when(env.getUserClassLoader()).thenReturn(RocksDBStateBackendConfigTest.class.getClassLoader());
		when(env.getIOManager()).thenReturn(ioMan);
		return env;
	}
}
