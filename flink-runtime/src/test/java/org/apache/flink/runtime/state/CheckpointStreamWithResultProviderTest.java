/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.MethodForwardingTestUtil;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class CheckpointStreamWithResultProviderTest extends TestLogger {

	private static TemporaryFolder temporaryFolder;

	@BeforeClass
	public static void beforeClass() throws IOException {
		temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();
	}

	@AfterClass
	public static void afterClass() {
		temporaryFolder.delete();
	}

	@Test
	public void testFactory() throws Exception {

		CheckpointStreamFactory primaryFactory = createCheckpointStreamFactory();
		try (
			CheckpointStreamWithResultProvider primaryOnly =
				CheckpointStreamWithResultProvider.createSimpleStream(
					CheckpointedStateScope.EXCLUSIVE,
					primaryFactory)) {

			Assert.assertTrue(primaryOnly instanceof CheckpointStreamWithResultProvider.PrimaryStreamOnly);
		}

		LocalRecoveryDirectoryProvider directoryProvider = createLocalRecoveryDirectoryProvider();
		try (
			CheckpointStreamWithResultProvider primaryAndSecondary =
				CheckpointStreamWithResultProvider.createDuplicatingStream(
					42L,
					CheckpointedStateScope.EXCLUSIVE,
					primaryFactory,
					directoryProvider)) {

			Assert.assertTrue(primaryAndSecondary instanceof CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream);
		}
	}

	@Test
	public void testCloseAndFinalizeCheckpointStreamResultPrimaryOnly() throws Exception {
		CheckpointStreamFactory primaryFactory = createCheckpointStreamFactory();

		CheckpointStreamWithResultProvider resultProvider =
			CheckpointStreamWithResultProvider.createSimpleStream(CheckpointedStateScope.EXCLUSIVE, primaryFactory);

		SnapshotResult<StreamStateHandle> result = writeCheckpointTestData(resultProvider);

		Assert.assertNotNull(result.getJobManagerOwnedSnapshot());
		Assert.assertNull(result.getTaskLocalSnapshot());

		try (FSDataInputStream inputStream = result.getJobManagerOwnedSnapshot().openInputStream()) {
			Assert.assertEquals(0x42, inputStream.read());
			Assert.assertEquals(-1, inputStream.read());
		}
	}

	@Test
	public void testCloseAndFinalizeCheckpointStreamResultPrimaryAndSecondary() throws Exception {
		CheckpointStreamFactory primaryFactory = createCheckpointStreamFactory();
		LocalRecoveryDirectoryProvider directoryProvider = createLocalRecoveryDirectoryProvider();

		CheckpointStreamWithResultProvider resultProvider =
			CheckpointStreamWithResultProvider.createDuplicatingStream(
				42L,
				CheckpointedStateScope.EXCLUSIVE,
				primaryFactory,
				directoryProvider);

		SnapshotResult<StreamStateHandle> result = writeCheckpointTestData(resultProvider);

		Assert.assertNotNull(result.getJobManagerOwnedSnapshot());
		Assert.assertNotNull(result.getTaskLocalSnapshot());

		try (FSDataInputStream inputStream = result.getJobManagerOwnedSnapshot().openInputStream()) {
			Assert.assertEquals(0x42, inputStream.read());
			Assert.assertEquals(-1, inputStream.read());
		}

		try (FSDataInputStream inputStream = result.getTaskLocalSnapshot().openInputStream()) {
			Assert.assertEquals(0x42, inputStream.read());
			Assert.assertEquals(-1, inputStream.read());
		}
	}

	@Test
	public void testCompletedAndCloseStateHandling() throws Exception {
		CheckpointStreamFactory primaryFactory = createCheckpointStreamFactory();

		testCloseBeforeComplete(new CheckpointStreamWithResultProvider.PrimaryStreamOnly(
			primaryFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)));
		testCompleteBeforeClose(new CheckpointStreamWithResultProvider.PrimaryStreamOnly(
			primaryFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)));

		testCloseBeforeComplete(new CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream(
				primaryFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE),
				primaryFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)));
		testCompleteBeforeClose(new CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream(
				primaryFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE),
				primaryFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)));
	}

	@Test
	public void testCloseMethodForwarding() throws Exception {
		CheckpointStreamFactory streamFactory = createCheckpointStreamFactory();

		MethodForwardingTestUtil.testMethodForwarding(
			Closeable.class,
			CheckpointStreamWithResultProvider.PrimaryStreamOnly::new,
			() -> {
				try {
					return streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});

		MethodForwardingTestUtil.testMethodForwarding(
			Closeable.class,
			CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream::new,
			() -> {
				try {
					return new DuplicatingCheckpointOutputStream(
						streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE),
						streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
	}

	private SnapshotResult<StreamStateHandle> writeCheckpointTestData(
		CheckpointStreamWithResultProvider resultProvider) throws IOException {

		CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
			resultProvider.getCheckpointOutputStream();
		checkpointOutputStream.write(0x42);
		return resultProvider.closeAndFinalizeCheckpointStreamResult();
	}

	private CheckpointStreamFactory createCheckpointStreamFactory() {
		return new MemCheckpointStreamFactory(16 * 1024);
	}

	/**
	 * Test that an exception is thrown if the stream was already closed before and we ask for a result later.
	 */
	private void testCloseBeforeComplete(CheckpointStreamWithResultProvider resultProvider) throws IOException {
		resultProvider.getCheckpointOutputStream().write(0x42);
		resultProvider.close();
		try {
			resultProvider.closeAndFinalizeCheckpointStreamResult();
			Assert.fail();
		} catch (IOException ignore) {
		}
	}

	private void testCompleteBeforeClose(CheckpointStreamWithResultProvider resultProvider) throws IOException {
		resultProvider.getCheckpointOutputStream().write(0x42);
		Assert.assertNotNull(resultProvider.closeAndFinalizeCheckpointStreamResult());
		resultProvider.close();
	}

	private LocalRecoveryDirectoryProvider createLocalRecoveryDirectoryProvider() throws IOException {
		File localStateDir = temporaryFolder.newFolder();
		JobID jobID = new JobID();
		JobVertexID jobVertexID = new JobVertexID();
		int subtaskIdx = 0;
		return new LocalRecoveryDirectoryProviderImpl(localStateDir, jobID, jobVertexID, subtaskIdx);
	}
}
