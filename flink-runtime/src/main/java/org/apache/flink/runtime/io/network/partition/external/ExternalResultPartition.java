/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.SerializerManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.external.writer.PartitionHashFileWriter;
import org.apache.flink.runtime.io.network.partition.external.writer.PartitionMergeFileWriter;
import org.apache.flink.runtime.io.network.partition.external.writer.PersistentFileWriter;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ExternalResultPartition is used when shuffling data through external shuffle service,
 * e.g. yarn shuffle service.
 */
public class ExternalResultPartition<T> extends ResultPartition<T> {

	private static final Logger LOG = LoggerFactory.getLogger(ExternalResultPartition.class);

	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final String partitionRootPath;
	private final int hashMaxSubpartitions;
	private final int mergeFactor;
	private final boolean enableAsyncMerging;
	private final boolean mergeToOneFile;
	private final double shuffleMemory;
	private final int numPages;
	private final SerializerManager<SerializationDelegate<T>> serializerManager;

	/** TTL for consumed partitions, in milliseconds. */
	private final long consumedPartitionTTL;

	/** TTL for partial consumed partitions, in milliseconds. */
	private final long partialConsumedPartitionTTL;

	/** TTL for unconsumed partitions, in milliseconds. */
	private final long unconsumedPartitionTTL;

	/** TTL for unfinished partitions, in milliseconds. */
	private final long unfinishedPartitionTTL;

	private PersistentFileWriter<T> fileWriter;

	private volatile boolean initialized;

	public ExternalResultPartition(
		Configuration taskManagerConfiguration,
		String owningTaskName,
		JobID jobId,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		int numberOfSubpartitions,
		int numTargetKeyGroups,
		MemoryManager memoryManager,
		IOManager ioManager) {

		super(owningTaskName, jobId, partitionId, partitionType, numberOfSubpartitions, numTargetKeyGroups);

		checkNotNull(taskManagerConfiguration);

		this.memoryManager = checkNotNull(memoryManager);
		this.ioManager = checkNotNull(ioManager);

		this.partitionRootPath = ExternalBlockShuffleUtils.generatePartitionRootPath(
			getSpillRootPath(taskManagerConfiguration, jobId.toString(), partitionId.toString()),
			partitionId.getProducerId().toString(), partitionId.getPartitionId().toString());
		this.hashMaxSubpartitions = taskManagerConfiguration.getInteger(
			TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS);
		this.mergeFactor = taskManagerConfiguration.getInteger(
			TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR);
		this.enableAsyncMerging = taskManagerConfiguration.getBoolean(
			TaskManagerOptions.TASK_MANAGER_OUTPUT_ENABLE_ASYNC_MERGE);
		this.mergeToOneFile = taskManagerConfiguration.getBoolean(
			TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_TO_ONE_FILE);
		this.shuffleMemory = taskManagerConfiguration.getInteger(
			TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB);
		this.numPages = (int) (shuffleMemory * 1024 * 1024 / memoryManager.getPageSize());

		checkArgument(hashMaxSubpartitions > 0,
			"The max allowed number of subpartitions should be larger than 0, but actually is: " + hashMaxSubpartitions);
		checkArgument(mergeFactor > 0,
			"The merge factor should be larger than 0, but actually is: " + mergeFactor);
		checkArgument(shuffleMemory > 0,
			"The shuffle memory should be larger than 0, but actually is: " + shuffleMemory);
		checkArgument(numPages > 0,
			"The number of pages should be larger than 0, but actually is: " + numPages);

		this.serializerManager = new SerializerManager<SerializationDelegate<T>>(
			ResultPartitionType.BLOCKING, taskManagerConfiguration);

		this.consumedPartitionTTL = taskManagerConfiguration.getInteger(
			TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_CONSUMED_PARTITION_TTL_IN_SECONDS) * 1000;
		this.partialConsumedPartitionTTL = taskManagerConfiguration.getInteger(
			TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_PARTIAL_CONSUMED_PARTITION_TTL_IN_SECONDS) * 1000;
		this.unconsumedPartitionTTL = taskManagerConfiguration.getInteger(
			TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_UNCONSUMED_PARTITION_TTL_IN_SECONDS) * 1000;
		this.unfinishedPartitionTTL = taskManagerConfiguration.getInteger(
			TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_UNFINISHED_PARTITION_TTL_IN_SECONDS) * 1000;
	}

	private void initialize() {
		checkNotNull(typeSerializer);
		checkNotNull(parentTask);

		try {
			Path tmpPartitionRootPath = new Path(partitionRootPath);
			FileSystem fs = FileSystem.getLocalFileSystem();
			if (fs.exists(tmpPartitionRootPath)) {
				// if partition root directory exists, we will delete the job root directory
				fs.delete(tmpPartitionRootPath, true);
			}
			int maxRetryCnt = 100;
			do {
				try {
					fs.mkdirs(tmpPartitionRootPath);
				} catch (IOException e) {
					if (maxRetryCnt-- > 0) {
						LOG.error("Fail to create partition root path: " + partitionRootPath
							+ ", left retry times: " + maxRetryCnt);
					} else {
						LOG.error("Reach retry limit, fail to create partition root path: " + partitionRootPath);
						throw e;
					}
				}
			} while (!fs.exists(tmpPartitionRootPath));

			writeConfigFile(fs);

			List<MemorySegment> memory = memoryManager.allocatePages(parentTask, numPages);

			// If the memory amount is less that the number of subpartitions, it should enter partition merge process.
			if (numberOfSubpartitions <= hashMaxSubpartitions && numberOfSubpartitions <= memory.size()
				&& !serializerManager.useCompression()) {
				fileWriter = new PartitionHashFileWriter<T>(
					numberOfSubpartitions,
					partitionRootPath,
					memoryManager,
					memory,
					ioManager,
					typeSerializer,
					numBytesOut,
					numBuffersOut);
			} else {
				fileWriter = new PartitionMergeFileWriter<T>(
					numberOfSubpartitions,
					partitionRootPath,
					mergeFactor,
					enableAsyncMerging,
					mergeToOneFile,
					memoryManager,
					memory,
					ioManager,
					typeSerializer,
					serializerManager,
					parentTask,
					numBytesOut,
					numBuffersOut);
			}

			initialized = true;

			LOG.info(toString() + " initialized successfully.");

		} catch (Throwable t) {
			deletePartitionDirOnFailure();
			throw new RuntimeException(t);
		}
	}

	@VisibleForTesting
	void writeConfigFile(FileSystem fileSystem) throws IOException {
		// Write the TTL configuration
		String configPath = ExternalBlockShuffleUtils.generateConfigPath(partitionRootPath);
		try (FSDataOutputStream configOut = fileSystem.create(new Path(configPath), FileSystem.WriteMode.OVERWRITE)) {
			DataOutputView configView = new DataOutputViewStreamWrapper(configOut);

			configView.writeLong(consumedPartitionTTL);
			configView.writeLong(partialConsumedPartitionTTL);
			configView.writeLong(unconsumedPartitionTTL);
			configView.writeLong(unfinishedPartitionTTL);
		} catch (IOException e) {
			LOG.error("Write the config file " + configPath + " fail.", e);
			throw e;
		}
	}

	@Override
	public void emitRecord(
			T record,
			int[] targetChannels,
			boolean isBroadcast,
			boolean flushAlways) throws IOException, InterruptedException {
		if (!initialized) {
			initialize();
		}

		try {
			checkInProduceState();
			fileWriter.add(record, targetChannels);
		} catch (Throwable e) {
			deletePartitionDirOnFailure();
			throw e;
		}
	}

	@Override
	public void emitRecord(
			T record,
			int targetChannel,
			boolean isBroadcast,
			boolean flushAlways) throws IOException, InterruptedException {
		if (!initialized) {
			initialize();
		}

		try {
			checkInProduceState();
			fileWriter.add(record, targetChannel);
		} catch (Throwable e) {
			deletePartitionDirOnFailure();
			throw e;
		}
	}

	@Override
	public void broadcastEvent(AbstractEvent event, boolean flushAlways) throws IOException {
		throw new UnsupportedOperationException("Event is not supported in external result partition.");
	}

	@Override
	public void clearBuffers() {
		// No operations.
	}

	@Override
	public void flushAll() {
		// No operations.
	}

	@Override
	public void flush(int subpartitionIndex) {
		// No operations.
	}

	@Override
	protected void releaseInternal() {
		try {
			if (fileWriter != null) {
				fileWriter.clear();
				fileWriter = null;
			}
		} catch (IOException e) {
			LOG.error("Fail to clear external shuffler", e);
		}
	}

	@Override
	public void finish() throws IOException {
		try {
			if (!initialized) {
				initialize();
				LOG.warn("The result partition {} has no data before finish.", partitionId);
			}
			if (isReleased.get()) {
				LOG.warn("The result partition {} has been released already before finish.", partitionId);
				deletePartitionDirOnFailure();
				return;
			}

			checkInProduceState();

			if (!initialized) {
				initialize();
			}

			FileSystem fs = FileSystem.get(new Path(partitionRootPath).toUri());

			fileWriter.finish();

			// write index files.
			List<List<PartitionIndex>> indicesList = fileWriter.generatePartitionIndices();
			for (int i = 0; i < indicesList.size(); ++i) {
				String indexPath = ExternalBlockShuffleUtils.generateIndexPath(partitionRootPath, i);

				try (FSDataOutputStream indexOut = fs.create(new Path(indexPath), FileSystem.WriteMode.OVERWRITE)) {
					DataOutputView indexView = new DataOutputViewStreamWrapper(indexOut);
					ExternalBlockShuffleUtils.serializeIndices(indicesList.get(i), indexView);
				}
			}

			// write finish files
			String finishedPath = ExternalBlockShuffleUtils.generateFinishedPath(partitionRootPath);
			try (FSDataOutputStream finishedOut = fs.create(new Path(finishedPath), FileSystem.WriteMode.OVERWRITE)) {
				DataOutputView finishedView = new DataOutputViewStreamWrapper(finishedOut);

				finishedView.writeInt(ExternalBlockResultPartitionMeta.SUPPORTED_PROTOCOL_VERSION);

				String externalFileType = fileWriter.getExternalFileType().name();
				finishedView.writeInt(externalFileType.length());
				finishedView.write(externalFileType.getBytes());
				finishedView.writeInt(indicesList.size());
				finishedView.writeInt(numberOfSubpartitions);
			}

		} catch (Throwable e) {
			deletePartitionDirOnFailure();
			ExceptionUtils.rethrow(e);
		} finally {
			releaseInternal();
		}

		isFinished = true;
	}

	private void deletePartitionDirOnFailure() {
		// currently we only support local file
		FileSystem fileSystem = FileSystem.getLocalFileSystem();
		boolean deleteSuccess = false;
		try {
			deleteSuccess = fileSystem.delete(new Path(partitionRootPath), true);
		} catch (Throwable e) {
			LOG.error("Exception occurred on deletePartitionDirOnFailure.", e);
		}
		if (!deleteSuccess) {
			LOG.error("Failed to delete dirty data, directory path " + partitionRootPath);
		}
	}

	private String getSpillRootPath(
		Configuration configuration, String jobIdStr, String partitionIdStr) {

		String localShuffleDirs = configuration.getString(
			TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS);

		if (localShuffleDirs.isEmpty()) {
			throw new IllegalStateException("The root dir for external result partition is not properly set. " +
				"Please check " + ExternalBlockShuffleServiceOptions.LOCAL_DIRS + " in hadoop configuration.");
		}

		String[] dirs = localShuffleDirs.split(",");
		Arrays.sort(dirs);

		int hashCode = ExternalBlockShuffleUtils.hashPartitionToDisk(jobIdStr, partitionIdStr);
		return dirs[hashCode % dirs.length];
	}

	@VisibleForTesting
	String getPartitionRootPath() {
		return partitionRootPath;
	}

	@Override
	public String toString() {
		return 	"External Result Partition: {" +
				"partitionId = " + partitionId +
				", fileWriter = " + fileWriter.getClass().getName() +
				", rootPath = " + partitionRootPath +
				", numberOfSubpartitions = " + numberOfSubpartitions +
				", hashMaxSubpartitions = " + hashMaxSubpartitions +
				", mergeFactor = " + mergeFactor +
				", shuffleMemory = " + shuffleMemory +
				", numPages = " + numPages +
				", enableAsyncMerging = " + enableAsyncMerging +
				", mergeToOneFile = " + mergeToOneFile +
				", consumedPartitionTTL" + consumedPartitionTTL +
				", partialConsumedPartitionTTL" + partialConsumedPartitionTTL +
				", unconsumedPartitionTTL" + unconsumedPartitionTTL +
				", unfinishedPartitionTTL" + unfinishedPartitionTTL +
			"}";
	}
}
