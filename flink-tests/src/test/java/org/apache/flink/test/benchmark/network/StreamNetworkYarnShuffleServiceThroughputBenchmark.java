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

package org.apache.flink.test.benchmark.network;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.external.YarnLocalResultPartitionResolver;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.test.util.MockYarnShuffleService;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Network broadcast throughput benchmarks.
 */
public class StreamNetworkYarnShuffleServiceThroughputBenchmark<T extends IOReadableWritable> extends StreamNetworkThroughputBenchmark<T> {

	private final Stack<File> filesOrDirsToClear = new Stack<>();
	private MockYarnShuffleService yarnShuffleService;
	protected final String spillRootPath = new File(StreamNetworkYarnShuffleServiceThroughputBenchmark.class.getResource("/").getPath()).getParentFile().getPath() + "/test-spill-folder/";
	protected final String user = "test-user";
	protected final String appId = "test-appId";
	protected final int shufflePort = 23857;
	private final BlockingQueue<CheckedThread> finishedWriters = new LinkedBlockingQueue<>();

	public void setUp(
		int recordWriters,
		int recordReceivers,
		int channels,
		int flushTimeout,
		long numRecordToSend,
		int receiverBufferPoolSize,
		T[] recordsSet,
		T value,
		TypeSerializer<T> serializer,
		Configuration configuration) throws Exception {
		startYarnShuffleservice();
		super.setUp(
			recordWriters,
			recordReceivers,
			channels,
			flushTimeout,
			numRecordToSend,
			false,
			false,
			-1,
			receiverBufferPoolSize,
			recordsSet,
			value,
			serializer,
			configuration);
	}

	@Override
	protected void createWriterThread() {
		writerThreads = new ShuffleRecordWriterThread[numRecordWriters];
		for (int writer = 0; writer < numRecordWriters; writer++) {
			writerThreads[writer] = new ShuffleRecordWriterThread<>(broadcastMode, recordsSet, finishedWriters);
			writerThreads[writer].start();
		}
	}

	@Override
	protected void createReceiverThread() throws IOException {
		receiverThreads = new ShuffleReceiverThread[numRecordReceivers];
		for (int receiver = 0; receiver < numRecordReceivers; ++receiver) {
			receiverThreads[receiver] = new ShuffleReceiverThread<>(value);
			receiverThreads[receiver].start();
		}
	}

	@Override
	protected void createRcordWriter() throws Exception {
		for (int writer = 0; writer < numRecordWriters; writer++) {
			ResultPartition<T> resultPartition = environment.createExternalResultPartition(writer, serializer);
			StreamRecordWriter<T> recordWriter = environment.createRecordWriter(resultPartition, flushTimeout);
			writerThreads[writer].setupWriter(recordWriter, resultPartition);
		}
	}

	@Override
	protected void createRcordReceiver() throws Exception {
		for (int receiver = 0; receiver < numRecordReceivers; ++receiver) {
			SingleInputGate[] inputGates = environment.createInputGate(
				shufflePort, ResultPartitionType.BLOCKING, channelRanges[receiver][0], channelRanges[receiver][1]);
			receiverThreads[receiver].setupReader(inputGates);
		}
	}

	private void startYarnShuffleservice() throws Exception {
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

		yarnShuffleService = new MockYarnShuffleService(
			spillRootPath,
			user,
			appId,
			shufflePort,
			10);

		yarnShuffleService.start();
	}

	public void tearDown() throws Exception {
		super.tearDown();

		// delete all the files or dirs.
		while (!filesOrDirsToClear.empty()) {
			filesOrDirsToClear.pop().delete();
		}

		yarnShuffleService.stop();
	}

	@Override
	public void executeBenchmark(long timeout) throws Exception {
		prepareSpillFolder();

		for (RecordWriterThread writerThread : writerThreads) {
			writerThread.setRecordsToSend(numRecordsPerWriter);
		}

		// wait writer
		for (int i = 0; i < writerThreads.length; ++i) {
			finishedWriters.take();
		}

		for (int i = 0; i < numRecordReceivers; ++i) {
			receiverFutures[i] = receiverThreads[i].setExpectedRecord(numRecordToReceive[i]);
		}

		// wait receiver
		for (int i = 0; i < numRecordReceivers; ++i) {
			receiverFutures[i].get(timeout, TimeUnit.MILLISECONDS);
		}

		createRcordWriter();
		createRcordReceiver();

		File spillFolder = new File(spillRootPath);
		deleteFile(spillFolder);
		environment.memoryManager.releaseAll(environment.parentTask);
	}

	private String prepareSpillFolder() {
		String spillPath = spillRootPath + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(user, appId);
		File spillFolder = new File(spillPath);
		spillFolder.mkdirs();
		return spillPath;
	}

	private boolean deleteFile(File dirFile) throws IOException {
		if (!dirFile.exists()) {
			return false;
		}

		if (dirFile.isFile()) {
			return dirFile.delete();
		} else {
			File[] files = dirFile.listFiles();
			if (files != null) {
				for (File file : files) {
					deleteFile(file);
				}
			} else {
				throw new IOException("Delete spilled files failed.");
			}
		}

		return dirFile.delete();
	}
}
