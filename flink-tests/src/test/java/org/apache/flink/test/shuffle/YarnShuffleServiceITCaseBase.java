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

package org.apache.flink.test.shuffle;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;
import org.apache.flink.runtime.io.network.partition.external.PersistentFileType;
import org.apache.flink.runtime.io.network.partition.external.YarnLocalResultPartitionResolver;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.test.util.MockYarnShuffleService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Base class for Yarn Shuffle Service IT cases.
 */
public class YarnShuffleServiceITCaseBase extends TestLogger {

	protected static final Logger LOG = LoggerFactory.getLogger(YarnShuffleServiceITCaseBase.class);

	protected static final String USER = "blinkUser";

	protected static final String APP_ID = "appid_845a3c45-e68a-4120-b61a-4617b96f4381";

	protected static final String NUM_RECORDS_KEY = "num_records";

	protected static final String RECORD_LENGTH_KEY = "record_length";

	protected static final int NUM_THREAD = 3;

	protected static final int NUM_PRODUCERS = 3;

	protected static final int NUM_CONSUMERS = 3;

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	protected static MockYarnShuffleService mockYarnShuffleService = null;

	protected static Stack<File> filesOrDirsToClear = new Stack<>();

	@BeforeClass
	public static void beforeClass() throws Exception {
		// 1. Create a mock container-executor script used for file clearing.
		String yarnHomeEnvVar = System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
		File hadoopBin = new File(yarnHomeEnvVar, "bin");
		File fakeContainerExecutor = new File(hadoopBin, "container-executor");

		if (!fakeContainerExecutor.exists()) {
			List<File> dirsToCreate = new ArrayList<>();

			File currentFile = hadoopBin;
			while (currentFile != null && !currentFile.exists()) {
				dirsToCreate.add(currentFile);
				currentFile = currentFile.getParentFile();
			}

			for (int i = dirsToCreate.size() - 1; i >= 0; --i) {
				boolean success = dirsToCreate.get(i).mkdir();
				if (!success) {
					throw new IOException("Failed to create the mock container-executor since the dir " + dirsToCreate + " fails to create.");
				}

				dirsToCreate.get(i).deleteOnExit();
				filesOrDirsToClear.push(dirsToCreate.get(i));
			}

			boolean success = fakeContainerExecutor.createNewFile();
			if (!success) {
				throw new IOException("Failed to create the mock container-executor file");
			}
			filesOrDirsToClear.push(fakeContainerExecutor);
		}

		// 2. Start the shuffle service.
		Throwable failCause = null;

		// Retry to avoid shuffle port conflicts.
		for (int i = 0; i < 10; ++i) {
			try {
				int port = findAvailableServerPort();
				mockYarnShuffleService = new MockYarnShuffleService(
					TEMP_FOLDER.getRoot().getAbsolutePath(),
					USER,
					APP_ID,
					port,
					NUM_THREAD);

				mockYarnShuffleService.start();
				break;
			} catch (Exception e) {
				failCause = e;

				if (mockYarnShuffleService != null) {
					mockYarnShuffleService.stop();
					mockYarnShuffleService = null;
				}
			}
		}

		assertNotNull("Fail to start yarn shuffle service, and the last retry exception " +
			"is " + ExceptionUtils.stringifyException(failCause), mockYarnShuffleService);
	}

	@After
	public void tearDown() {
		while (!filesOrDirsToClear.empty()) {
			filesOrDirsToClear.pop().delete();
		}
	}

	@AfterClass
	public static void afterClass() {
		// 1. Create all the files or dirs.
		while (!filesOrDirsToClear.empty()) {
			filesOrDirsToClear.pop().delete();
		}

		// 2. Shutdown the shuffle service.
		if (mockYarnShuffleService != null) {
			mockYarnShuffleService.stop();
		}
	}

	/**
	 * A wrapper contains general configurations for external shuffle service.
	 */
	protected static class YarnShuffleServiceTestConfiguration {

		/** File type to use. */
		public final PersistentFileType fileType;

		/** The length of maximum concurrent requests. */
		public final int maxConcurrentRequests;

		/** Whether enable async merging. */
		public final boolean enableAsyncMerging;

		/** Whether merge to one file. */
		public final boolean mergeToOneFile;

		/** Whether compress external shuffle data. */
		public final boolean useCompression;

		/** The length of compression / decompression buffer. */
		public final int maxCompressionBufferSize;

		public YarnShuffleServiceTestConfiguration(
			PersistentFileType fileType,
			int maxConcurrentRequests,
			boolean enableAsyncMerging,
			boolean mergeToOneFile,
			boolean useCompression,
			int maxCompressionBufferSize) {

			this.fileType = fileType;
			this.maxConcurrentRequests = maxConcurrentRequests;
			this.enableAsyncMerging = enableAsyncMerging;
			this.mergeToOneFile = mergeToOneFile;
			this.useCompression = useCompression;
			this.maxCompressionBufferSize = maxCompressionBufferSize;
		}

		@Override
		public String toString() {
			return "fileType: " + fileType + ", maxConcurrentRequests: " + maxConcurrentRequests +
				", enableAsyncMerging: " + enableAsyncMerging + ", mergeToOneFile: " + mergeToOneFile +
				", useCompression: " + useCompression + ", maxCompressionBufferSize: " + maxCompressionBufferSize;
		}
	}

	protected Configuration prepareConfiguration(YarnShuffleServiceTestConfiguration shuffleConfig) {
		Configuration configuration = new Configuration();

		configuration.setBoolean(TaskManagerOptions.TASK_MANAGER_OUTPUT_ENABLE_ASYNC_MERGE, shuffleConfig.enableAsyncMerging);
		configuration.setBoolean(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_TO_ONE_FILE, shuffleConfig.mergeToOneFile);

		if (shuffleConfig.useCompression) {
			configuration.setBoolean(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_ENABLE_COMPRESSION, true);
			configuration.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_COMPRESSION_BUFFER_SIZE, shuffleConfig.maxCompressionBufferSize);
		} else {
			configuration.setBoolean(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_ENABLE_COMPRESSION, false);
		}

		if (PersistentFileType.HASH_PARTITION_FILE == shuffleConfig.fileType) {
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB, 1);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS, 16);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR, 16);
		} else if (PersistentFileType.MERGED_PARTITION_FILE == shuffleConfig.fileType) {
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB, 1);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS, 2);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR, 2);
		} else {
			throw new IllegalArgumentException("Invalid configuration for ExternalFileType");
		}

		configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, 1L << 20);
		configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 1L << 20);
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_EXTERNAL_BLOCKING_CHANNEL, 4);

		configuration.setString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE, BlockingShuffleType.YARN.toString());
		configuration.setInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY.key(), mockYarnShuffleService.getPort());

		configuration.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_MAX_CONCURRENT_REQUESTS, shuffleConfig.maxConcurrentRequests);

		// Add local shuffle dirs
		configuration.setString(TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS,
			TEMP_FOLDER.getRoot().getAbsolutePath() + "/" + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(USER, APP_ID));

		// Use random port to avoid port conflict
		configuration.setInteger(RestOptions.PORT, 0);

		return configuration;
	}

	protected void executeShuffleTest(JobGraph jobGraph, Configuration configuration) throws Exception {
		int numTaskManager = 0;
		for (JobVertex jobVertex : jobGraph.getVertices()) {
			numTaskManager += jobVertex.getParallelism();
		}

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(numTaskManager)
			.setNumSlotsPerTaskManager(1)
			.build();

		try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);
			CompletableFuture<JobSubmissionResult> submissionFuture = miniClusterClient.submitJob(jobGraph);

			// wait for the submission to succeed
			JobSubmissionResult jobSubmissionResult = submissionFuture.get();
			CompletableFuture<JobResult> resultFuture = miniClusterClient.requestJobResult(jobSubmissionResult.getJobID());

			JobResult jobResult = resultFuture.get();
			assertThat(jobResult.getSerializedThrowable().toString(), jobResult.getSerializedThrowable().isPresent(), is(false));

			// Try best to release the direct memory by GC the DirectByteBuffer objects.
			System.gc();
		}
	}

	protected JobGraph createJobGraph(Class recordClass, Class producerClass, Class consumerClass) throws IOException {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setExecutionMode(ExecutionMode.BATCH_FORCED);
		env.getConfig().registerKryoType(recordClass);

		JobGraph jobGraph = new JobGraph("Yarn Shuffle Service Test");

		jobGraph.setExecutionConfig(env.getConfig());
		jobGraph.setAllowQueuedScheduling(true);

		JobVertex producer = new JobVertex("Yarn Shuffle Service Test Producer");
		jobGraph.addVertex(producer);
		producer.setSlotSharingGroup(new SlotSharingGroup());
		producer.setInvokableClass(producerClass);
		producer.setParallelism(NUM_PRODUCERS);

		JobVertex consumer = new JobVertex("Yarn Shuffle Service Test Consumer");
		jobGraph.addVertex(consumer);
		consumer.setSlotSharingGroup(new SlotSharingGroup());
		consumer.setInvokableClass(consumerClass);
		consumer.setParallelism(NUM_CONSUMERS);

		consumer.connectNewDataSetAsInput(producer, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		return jobGraph;
	}

	private static int findAvailableServerPort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	/**
	 * Data producer for IT case.
	 */
	public static class TestProducer extends AbstractInvokable {
		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public TestProducer(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			ResultPartitionWriter rpw = getEnvironment().getWriter(0);
			rpw.setParentTask(this);
			rpw.setTypeSerializer(TypeExtractor.createTypeInfo(TestRecord.class).createSerializer(getEnvironment().getExecutionConfig()));
			RecordWriter<TestRecord> writer = new RecordWriter<>(rpw);

			int numRecords = getEnvironment().getJobConfiguration().getInteger(NUM_RECORDS_KEY, -1);
			assertTrue("Number of records not set", numRecords >= 0);

			int recordLength = getEnvironment().getJobConfiguration().getInteger(RECORD_LENGTH_KEY, -1);
			assertTrue("Record length not set", recordLength > 0);

			try {
				for (int i = 0; i < numRecords; ++i) {
					TestRecord record = new TestRecord(i, recordLength);
					if (i % 2 == 0) {
						writer.broadcastEmit(new TestRecord(i, recordLength));
					} else {
						for (int j = 0; j < rpw.getNumberOfSubpartitions(); ++j) {
							writer.emit(record);
						}
					}
				}
			} finally {
				writer.flushAll();
			}
		}
	}

	/**
	 * Data consumer for IT case.
	 */
	public static class TestConsumer extends AbstractInvokable {
		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public TestConsumer(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			int numRecords = getEnvironment().getJobConfiguration().getInteger(NUM_RECORDS_KEY, -1);
			assertTrue("Number of records not set", numRecords >= 0);

			int recordLength = getEnvironment().getJobConfiguration().getInteger(RECORD_LENGTH_KEY, -1);
			assertTrue("Record length not set", recordLength > 0);

			Map<TestRecord, Integer> recordsCount = new HashMap<>();

			MutableRecordReader<DeserializationDelegate<TestRecord>> reader = new MutableRecordReader<>(
				getEnvironment().getInputGate(0),
				getEnvironment().getTaskManagerInfo().getTmpDirectories(),
				getEnvironment().getTaskManagerInfo().getConfiguration());

			try {
				DeserializationDelegate<TestRecord> deserializationDelegate = new NonReusingDeserializationDelegate<>(
					TypeExtractor.createTypeInfo(TestRecord.class).createSerializer(getEnvironment().getExecutionConfig()));

				while (reader.next(deserializationDelegate)) {
					TestRecord record = deserializationDelegate.getInstance();
					recordsCount.compute(record, (key, oldCount) -> oldCount == null ? 1 : oldCount + 1);
				}

				for (int i = 0; i < numRecords; ++i) {
					TestRecord record = new TestRecord(i, recordLength);
					int count = recordsCount.getOrDefault(record, 0);
					assertEquals(NUM_PRODUCERS, count);
				}
			} finally {
				reader.clearBuffers();
			}
		}
	}

	/**
	 * The records sent by the producers.
	 */
	public static class TestRecord {
		private int index;
		private int recordLength;
		private byte[] buf;

		public TestRecord() {
			// For deserialization.
		}

		public TestRecord(int index, int recordLength) {
			this.index = index;
			this.recordLength = recordLength;

			this.buf = new byte[recordLength];

			// Random with the same seed will always generate the same random number sequence.
			Random random = new Random(index);
			for (int i = 0; i < recordLength; ++i) {
				buf[i] = (byte) (random.nextInt() % 128);
			}
		}

		public int getIndex() {
			return index;
		}

		public byte[] getByte() {
			return buf;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TestRecord that = (TestRecord) o;
			return index == that.index &&
				Arrays.equals(buf, that.buf);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(index);
			result = 31 * result + Arrays.hashCode(buf);
			return result;
		}

		@Override
		public String toString() {
			StringBuilder ret = new StringBuilder();

			ret.append(index).append(": ");
			for (int i = 0; i < recordLength; ++i) {
				ret.append(buf[i]).append(" ");
			}
			return ret.toString();
		}
	}
}
