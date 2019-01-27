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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.partition.FixedLengthBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

/**
 * Test the reading and writing of the files produced by external result partition.
 */
@RunWith(Parameterized.class)
public class ExternalResultPartitionReadWriteTest {

	/*************************** Global configurations ***********************/

	private static final String USER = new String(ExternalResultPartitionReadWriteTest.class.getName());

	private static final String APP_ID = "application_15910870958_123456";

	private static final JobID JOB_ID = new JobID();

	private static final ResultPartitionID partitionID = new ResultPartitionID();

	public static final int PAGE_SIZE = 4096;

	public static final int NUM_PAGES = 1024 * 1024 / PAGE_SIZE;

	public static final int MEMORY_SIZE = PAGE_SIZE * NUM_PAGES;

	public static final int NUM_PARTITIONS = 4;

	/*********************** Internal variables for cases *********************/

	/** Total directories on both SATA and SSD disks for external shuffle */
	private String outputLocalDir;

	private final AbstractInvokable parentTask = spy(new DummyInvokable());

	private IOManager ioManager;

	private MemoryManager memoryManager;

	private TypeSerializer<Integer> serializer;

	/** Parameterized variable of whether enable async merging or not. */
	private final boolean enableAsyncMerging;

	/** Parameterized variable of whether merge to one file or not. */
	private final boolean mergeToOneFile;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{false, false}, {false, true}, {true, false}, {true, true}
		});
	}

	public ExternalResultPartitionReadWriteTest(boolean enableAsyncMerging, boolean mergeToOneFile) {
		this.enableAsyncMerging = enableAsyncMerging;
		this.mergeToOneFile = mergeToOneFile;
	}

	@Before
	public void before() {
		outputLocalDir = EnvironmentInformation.getTemporaryFileDirectory() + "/" + UUID.randomUUID().toString() + "/";

		this.ioManager = new IOManagerAsync();
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, 4096, MemoryType.HEAP, true);
		this.serializer = IntSerializer.INSTANCE;
	}

	@After
	public void after() {
		this.ioManager.shutdown();
		if (!this.ioManager.isProperlyShutDown()) {
			fail("I/O Manager was not properly shut down.");
		}

		if (this.memoryManager != null) {
			this.memoryManager.releaseAll(parentTask);
			Assert.assertTrue("Memory leak: not all segments have been returned to the memory manager.",
				this.memoryManager.verifyEmpty());

			this.memoryManager.shutdown();
			this.memoryManager = null;
		}

		cleanExternalShuffleDirs();
	}

	@Test
	public void testLocalSpillMultipleFileNoMerge() throws Exception {
		runTest(PersistentFileType.MERGED_PARTITION_FILE, false, false);
	}

	@Test
	public void testLocalSpillMultipleFileNoMergeWithSkewData() throws Exception {
		runTest(PersistentFileType.MERGED_PARTITION_FILE, false, true);
	}

	@Test
	public void testLocalSpillMultipleFileWithMerge() throws Exception {
		runTest(PersistentFileType.MERGED_PARTITION_FILE, true, false);
	}

	@Test
	public void testLocalSpillMultipleFileWithMergeWithSkewData() throws Exception {
		runTest(PersistentFileType.MERGED_PARTITION_FILE, true, true);
	}

	@Test
	public void testLocalSpillSingleFileEachPartition() throws Exception {
		runTest(PersistentFileType.HASH_PARTITION_FILE, false, false);
	}

	@Test
	public void testLocalSpillSingleFileEachPartitionWithSkewData() throws Exception {
		runTest(PersistentFileType.HASH_PARTITION_FILE, false, true);
	}

	private void cleanExternalShuffleDirs() {
		try {
			if (outputLocalDir != null) {
				FileSystem.getLocalFileSystem().delete(new Path(outputLocalDir), true);
			}
		} catch (Throwable e) {
			// do nothing
		}
	}

	private void runTest(PersistentFileType externalFileType,
							boolean withMerge,
							boolean dataSkew) throws Exception {

		// 1. prepare internal variables according to specific case configuration
		final int segmentSize = memoryManager.getPageSize();
		final int nrSegments = (externalFileType == PersistentFileType.MERGED_PARTITION_FILE && !withMerge) ? 300 : 1600;

		// Each record consumes 8 bytes, 4 for length and 4 for the record itself
		int numRecordsEachPartition = segmentSize * nrSegments / (4 + 4);

		// 2. Compute the configuration
		Configuration taskManagerConfig = prepareTestConfiguration(externalFileType);

		// 2. write external shuffle files to disk
		writeRecordsToFile(taskManagerConfig, numRecordsEachPartition, withMerge, dataSkew);

		// 3. read external shuffle files and test data's validity
		assertFileContentConsistent(externalFileType, numRecordsEachPartition, withMerge, dataSkew);
	}

	/**
	 * Prepare configurations for ExternalResultPartition to test different write mode.
	 */
	private Configuration prepareTestConfiguration(PersistentFileType externalFileType) {
		Configuration taskManagerConfig = new Configuration();

		taskManagerConfig.setBoolean(TaskManagerOptions.TASK_MANAGER_OUTPUT_ENABLE_ASYNC_MERGE, enableAsyncMerging);
		taskManagerConfig.setBoolean(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_TO_ONE_FILE, mergeToOneFile);
		if (PersistentFileType.HASH_PARTITION_FILE == externalFileType) {
			taskManagerConfig.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB, 1);
			taskManagerConfig.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS, 16);
			taskManagerConfig.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR, 16);
		} else if (PersistentFileType.MERGED_PARTITION_FILE == externalFileType) {
			taskManagerConfig.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB, 1);
			taskManagerConfig.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS, 2);
			taskManagerConfig.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR, 2);
		} else {
			throw new IllegalArgumentException("Invalid configuration for ExternalFileType");
		}

		// add dirs
		taskManagerConfig.setString(TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS,
			outputLocalDir + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(USER, APP_ID) + "/");

		return taskManagerConfig;
	}

	private void writeRecordsToFile(Configuration configuration, int numRecordsEachPartition, boolean withMerge, boolean dataSkew) throws IOException, InterruptedException {
		// choose a directory to write external shuffle, we can choose directory based on disk type
		ExternalResultPartition<Integer> resultPartition = new ExternalResultPartition<>(
			configuration,
			"taskName",
			JOB_ID,
			partitionID,
			ResultPartitionType.BLOCKING,
			NUM_PARTITIONS,
			NUM_PARTITIONS,
			memoryManager,
			ioManager);

		resultPartition.setTypeSerializer(serializer);
		resultPartition.setParentTask(parentTask);

		if (dataSkew) {
			// In data skew mode, we make data of the first subpartition locate at the head of this partition
			// and make data of the last subpartition empty.
			int skewSubpartition = 0;
			int emptySubpartition = 1;
			for (int i = skewSubpartition; i < NUM_PARTITIONS * numRecordsEachPartition; i += NUM_PARTITIONS) {
				resultPartition.emitRecord(i, new int[]{skewSubpartition}, false, false);
			}
			for (int i = 0; i < NUM_PARTITIONS * numRecordsEachPartition; i++) {
				int writePartition = i % NUM_PARTITIONS;
				if (writePartition != skewSubpartition && writePartition != (NUM_PARTITIONS - 1)) {
					resultPartition.emitRecord(i, new int[]{writePartition}, false, false);
				}
			}
		} else {
			for (int i = 0; i < NUM_PARTITIONS * numRecordsEachPartition; i++) {
				int writePartition = i % NUM_PARTITIONS;
				resultPartition.emitRecord(i, new int[]{writePartition}, false, false);
			}
		}

		resultPartition.finish();

		assertTrue("The written bytes should be large than 0.", resultPartition.getNumBytesOut().getCount() > 0);
		assertTrue("The number of written buffers should be large than 0.", resultPartition.getNumBuffersOut().getCount() > 0);

		resultPartition.release();
	}

	private void assertFileContentConsistent(PersistentFileType fileType, int numRecordsEachPartition, boolean withMerge, boolean dataSkew) throws Exception {
		ExternalBlockResultPartitionMeta meta = new ExternalBlockResultPartitionMeta(
			partitionID,
			FileSystem.getLocalFileSystem(),
			new ExternalBlockResultPartitionManagerTest.MockResultPartitionFileInfo(
				outputLocalDir,
				ExternalBlockShuffleUtils.generatePartitionRootPath(
					outputLocalDir + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(USER, APP_ID),
					partitionID.getProducerId().toString(),
					partitionID.getPartitionId().toString()),
				3600,
				3600));

		FixedLengthBufferPool bufferPool = new FixedLengthBufferPool(10, PAGE_SIZE, MemoryType.OFF_HEAP);
		ExecutorService producerThreadPool = Executors.newFixedThreadPool(
			NUM_PARTITIONS, new DispatcherThreadFactory(new ThreadGroup("Disk IO Thread"), "IO thread for Disk"));
		ExecutorService consumerThreadPool = Executors.newFixedThreadPool(
			NUM_PARTITIONS, new DispatcherThreadFactory(new ThreadGroup("Netty IO Thread"), "IO thread for Network"));

		final List<TestConsumerCallback.VerifyAscendingOnFixedStepCallback> resultValidators = Lists.newArrayList();
		final List<Future<Boolean>> consumerResults = Lists.newArrayList();

		for (int i = 0; i < NUM_PARTITIONS; i++) {
			TestConsumerCallback.VerifyAscendingOnFixedStepCallback resultValidator =
				new TestConsumerCallback.VerifyAscendingOnFixedStepCallback(i, NUM_PARTITIONS, numRecordsEachPartition);
			resultValidators.add(resultValidator);

			TestSubpartitionConsumer consumer = new TestSubpartitionConsumer(false, resultValidator);

			ExternalBlockSubpartitionView subpartitionView = new ExternalBlockSubpartitionView(
				meta,
				i,
				producerThreadPool,
				partitionID,
				bufferPool,
				0,
				consumer);

			consumer.setSubpartitionView(subpartitionView);

			subpartitionView.notifyCreditAdded(Integer.MAX_VALUE);
			consumerResults.add(consumerThreadPool.submit(consumer));
		}

		// Wait for the results
		for (Future<Boolean> res : consumerResults) {
			try {
				res.get(100, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				throw new TimeoutException("There has been a timeout in the test. This " +
					"indicates that there is a bug/deadlock in the tested subpartition " +
					"view.");
			}
		}

		for (int i = 0; i < resultValidators.size(); i++) {
			if (dataSkew && i == (NUM_PARTITIONS - 1)) {
				// In data skew mode, we make the last subpartition empty
				continue;
			}
			Assert.assertEquals(numRecordsEachPartition, resultValidators.get(i).getActualRecordCount());
		}

		// Check meta and file type is as expected.
		Assert.assertTrue(meta.hasInitialized());
		Assert.assertEquals(meta.getExternalBlockFileType(), fileType);

		consumerThreadPool.shutdown();
		consumerThreadPool.awaitTermination(10, TimeUnit.SECONDS);
		List<Runnable> leftProducers = producerThreadPool.shutdownNow();
		producerThreadPool.awaitTermination(10, TimeUnit.SECONDS);
		Assert.assertTrue("All the producers should finish while " + leftProducers.size() + " producer(s) is(are) left", leftProducers.isEmpty());
	}
}
