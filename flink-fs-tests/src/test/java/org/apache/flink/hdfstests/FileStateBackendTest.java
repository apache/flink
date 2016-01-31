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

package org.apache.flink.hdfstests;

import org.apache.commons.io.FileUtils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.filesystem.FileStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileStateBackendTest {
	
	private static File TEMP_DIR;
	
	private static String HDFS_ROOT_URI;
	
	private static MiniDFSCluster HDFS_CLUSTER;
	
	private static FileSystem FS;
	
	// ------------------------------------------------------------------------
	//  startup / shutdown
	// ------------------------------------------------------------------------
	
	@BeforeClass
	public static void createHDFS() {
		try {
			TEMP_DIR = new File(ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH, UUID.randomUUID().toString());

			Configuration hdConf = new Configuration();
			hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEMP_DIR.getAbsolutePath());
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
			HDFS_CLUSTER = builder.build();

			HDFS_ROOT_URI = "hdfs://" + HDFS_CLUSTER.getURI().getHost() + ":"
					+ HDFS_CLUSTER.getNameNodePort() + "/";
			
			FS = FileSystem.get(new URI(HDFS_ROOT_URI));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Could not create HDFS mini cluster " + e.getMessage());
		}
	}

	@AfterClass
	public static void destroyHDFS() {
		try {
			HDFS_CLUSTER.shutdown();
			FileUtils.deleteDirectory(TEMP_DIR);
		}
		catch (Exception ignored) {}
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------
	
	@Test
	public void testSetupAndSerialization() {
		try {
			URI baseUri = new URI(HDFS_ROOT_URI + UUID.randomUUID().toString());
			
			FsStateBackend originalBackend = new FsStateBackend(baseUri);

			assertFalse(originalBackend.isInitialized());
			assertEquals(baseUri, originalBackend.getBasePath().toUri());
			assertNull(originalBackend.getCheckpointDirectory());

			// serialize / copy the backend
			FsStateBackend backend = CommonTestUtils.createCopySerializable(originalBackend);
			assertFalse(backend.isInitialized());
			assertEquals(baseUri, backend.getBasePath().toUri());
			assertNull(backend.getCheckpointDirectory());

			// no file operations should be possible right now
			try {
				backend.checkpointStateSerializable("exception train rolling in", 2L, System.currentTimeMillis());
				fail("should fail with an exception");
			} catch (IllegalStateException e) {
				// supreme!
			}

			backend.initializeForJob(new DummyEnvironment("test", 1, 0));
			assertNotNull(backend.getCheckpointDirectory());

			Path checkpointDir = backend.getCheckpointDirectory();
			assertTrue(FS.exists(checkpointDir));
			assertTrue(isDirectoryEmpty(checkpointDir));

			backend.disposeAllStateForCurrentJob();
			assertNull(backend.getCheckpointDirectory());

			assertTrue(isDirectoryEmpty(baseUri));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSerializableState() {
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(
				new FsStateBackend(randomHdfsFileUri(), 40));
			backend.initializeForJob(new DummyEnvironment("test", 1, 0));

			Path checkpointDir = backend.getCheckpointDirectory();

			String state1 = "dummy state";
			String state2 = "row row row your boat";
			Integer state3 = 42;

			StateHandle<String> handle1 = backend.checkpointStateSerializable(state1, 439568923746L, System.currentTimeMillis());
			StateHandle<String> handle2 = backend.checkpointStateSerializable(state2, 439568923746L, System.currentTimeMillis());
			StateHandle<Integer> handle3 = backend.checkpointStateSerializable(state3, 439568923746L, System.currentTimeMillis());

			assertEquals(state1, handle1.getState(getClass().getClassLoader()));
			handle1.discardState();

			assertEquals(state2, handle2.getState(getClass().getClassLoader()));
			handle2.discardState();

			assertEquals(state3, handle3.getState(getClass().getClassLoader()));
			handle3.discardState();

			assertTrue(isDirectoryEmpty(checkpointDir));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStateOutputStream() {
		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(
				new FsStateBackend(randomHdfsFileUri(), 15));
			backend.initializeForJob(new DummyEnvironment("test", 1, 0));

			Path checkpointDir = backend.getCheckpointDirectory();

			byte[] state1 = new byte[1274673];
			byte[] state2 = new byte[1];
			byte[] state3 = new byte[0];
			byte[] state4 = new byte[177];

			Random rnd = new Random();
			rnd.nextBytes(state1);
			rnd.nextBytes(state2);
			rnd.nextBytes(state3);
			rnd.nextBytes(state4);

			long checkpointId = 97231523452L;

			FsStateBackend.FsCheckpointStateOutputStream stream1 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			FsStateBackend.FsCheckpointStateOutputStream stream2 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			FsStateBackend.FsCheckpointStateOutputStream stream3 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			stream1.write(state1);
			stream2.write(state2);
			stream3.write(state3);

			FileStreamStateHandle handle1 = (FileStreamStateHandle) stream1.closeAndGetHandle();
			ByteStreamStateHandle handle2 = (ByteStreamStateHandle) stream2.closeAndGetHandle();
			ByteStreamStateHandle handle3 = (ByteStreamStateHandle) stream3.closeAndGetHandle();

			// use with try-with-resources
			StreamStateHandle handle4;
			try (StateBackend.CheckpointStateOutputStream stream4 =
						 backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis())) {
				stream4.write(state4);
				handle4 = stream4.closeAndGetHandle();
			}

			// close before accessing handle
			StateBackend.CheckpointStateOutputStream stream5 =
					backend.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			stream5.write(state4);
			stream5.close();
			try {
				stream5.closeAndGetHandle();
				fail();
			} catch (IOException e) {
				// uh-huh
			}

			validateBytesInStream(handle1.getState(getClass().getClassLoader()), state1);
			handle1.discardState();
			assertFalse(isDirectoryEmpty(checkpointDir));
			ensureFileDeleted(handle1.getFilePath());

			validateBytesInStream(handle2.getState(getClass().getClassLoader()), state2);
			handle2.discardState();

			validateBytesInStream(handle3.getState(getClass().getClassLoader()), state3);
			handle3.discardState();

			validateBytesInStream(handle4.getState(getClass().getClassLoader()), state4);
			handle4.discardState();
			assertTrue(isDirectoryEmpty(checkpointDir));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static void ensureFileDeleted(Path path) {
		try {
			assertFalse(FS.exists(path));
		}
		catch (IOException ignored) {}
	}

	private static boolean isDirectoryEmpty(URI directory) {
		return isDirectoryEmpty(new Path(directory));
	}
	
	private static boolean isDirectoryEmpty(Path directory) {
		try {
			FileStatus[] nested = FS.listStatus(directory);
			return  nested == null || nested.length == 0;
		}
		catch (IOException e) {
			return true;
		}
	}

	private static URI randomHdfsFileUri() {
		String uriString = HDFS_ROOT_URI + UUID.randomUUID().toString();
		try {
			return new URI(uriString);
		}
		catch (URISyntaxException e) {
			throw new RuntimeException("Invalid test directory URI: " + uriString, e);
		}
	}

	private static void validateBytesInStream(InputStream is, byte[] data) throws IOException {
		byte[] holder = new byte[data.length];
		
		int pos = 0;
		int read;
		while (pos < holder.length && (read = is.read(holder, pos, holder.length - pos)) != -1) {
			pos += read;
		}
			
		assertEquals("not enough data", holder.length, pos); 
		assertEquals("too much data", -1, is.read());
		assertArrayEquals("wrong data", data, holder);
	}
}
