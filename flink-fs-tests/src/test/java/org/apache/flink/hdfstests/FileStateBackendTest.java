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
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileStateBackendTest extends StateBackendTestBase<FsStateBackend> {

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

	@Override
	protected FsStateBackend getStateBackend() throws Exception {
		URI stateBaseURI = new URI(HDFS_ROOT_URI + UUID.randomUUID().toString());
		return new FsStateBackend(stateBaseURI);

	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	// disable these because the verification does not work for this state backend
	@Override
	@Test
	public void testValueStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testListStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testReducingStateRestoreWithWrongSerializers() {}

	@Test
	public void testStateOutputStream() {
		URI basePath = randomHdfsFileUri();

		try {
			FsStateBackend backend = CommonTestUtils.createCopySerializable(new FsStateBackend(basePath, 15));
			JobID jobId = new JobID();


			CheckpointStreamFactory streamFactory = backend.createStreamFactory(jobId, "test_op");

			// we know how FsCheckpointStreamFactory is implemented so we know where it
			// will store checkpoints
			Path checkpointPath = new Path(new Path(basePath), jobId.toString());

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

			CheckpointStreamFactory.CheckpointStateOutputStream stream1 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			CheckpointStreamFactory.CheckpointStateOutputStream stream2 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			CheckpointStreamFactory.CheckpointStateOutputStream stream3 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());

			stream1.write(state1);
			stream2.write(state2);
			stream3.write(state3);

			FileStateHandle handle1 = (FileStateHandle) stream1.closeAndGetHandle();
			ByteStreamStateHandle handle2 = (ByteStreamStateHandle) stream2.closeAndGetHandle();
			ByteStreamStateHandle handle3 = (ByteStreamStateHandle) stream3.closeAndGetHandle();

			// use with try-with-resources
			FileStateHandle handle4;
			try (CheckpointStreamFactory.CheckpointStateOutputStream stream4 =
						 streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis())) {
				stream4.write(state4);
				handle4 = (FileStateHandle) stream4.closeAndGetHandle();
			}

			// close before accessing handle
			CheckpointStreamFactory.CheckpointStateOutputStream stream5 =
					streamFactory.createCheckpointStateOutputStream(checkpointId, System.currentTimeMillis());
			stream5.write(state4);
			stream5.close();
			try {
				stream5.closeAndGetHandle();
				fail();
			} catch (IOException e) {
				// uh-huh
			}

			validateBytesInStream(handle1.openInputStream(), state1);
			handle1.discardState();
			assertFalse(isDirectoryEmpty(checkpointPath));
			ensureFileDeleted(handle1.getFilePath());

			validateBytesInStream(handle2.openInputStream(), state2);
			handle2.discardState();

			// stream 3 has zero bytes, so it should not return anything
			assertNull(handle3);

			validateBytesInStream(handle4.openInputStream(), state4);
			handle4.discardState();
			assertTrue(isDirectoryEmpty(checkpointPath));
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

		try {
			byte[] holder = new byte[data.length];

			int pos = 0;
			int read;
			while (pos < holder.length && (read = is.read(holder, pos, holder.length - pos)) != -1) {
				pos += read;
			}

			assertEquals("not enough data", holder.length, pos);
			assertEquals("too much data", -1, is.read());
			assertArrayEquals("wrong data", data, holder);
		} finally {
			is.close();
		}
	}
}
