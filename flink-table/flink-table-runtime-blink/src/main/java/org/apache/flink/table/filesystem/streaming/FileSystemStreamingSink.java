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

package org.apache.flink.table.filesystem.streaming;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.filesystem.FileSystemFactory;
import org.apache.flink.table.filesystem.FileSystemOutputFormat;
import org.apache.flink.table.filesystem.OutputFormatFactory;
import org.apache.flink.table.filesystem.PartitionComputer;
import org.apache.flink.table.filesystem.PartitionTempFileManager;
import org.apache.flink.table.filesystem.PartitionWriter;
import org.apache.flink.table.filesystem.PartitionWriterFactory;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;
import org.apache.flink.table.filesystem.streaming.policy.PartitionCommitPolicy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.filesystem.PartitionTempFileManager.cleanTaskTempFiles;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * File system sink for streaming jobs.
 */
public class FileSystemStreamingSink<T> extends RichSinkFunction<T>
		implements CheckpointedFunction, CheckpointListener {

	private static final long FIRST_CHECKPOINT_ID = 1L;
	private static final ListStateDescriptor<Long> CP_ID_STATE_DESC =
			new ListStateDescriptor<>("checkpoint-id", LongSerializer.INSTANCE);

	private final FileSystemFactory fsFactory;
	private final TableMetaStoreFactory msFactory;
	private final Path tmpPath;
	private final Path locationPath;
	private final String[] partitionColumns;
	private final LinkedHashMap<String, String> staticPartitions;
	private final PartitionComputer<T> computer;
	private final OutputFormatFactory<T> formatFactory;
	private final Map<String, String> properties;

	private transient PartitionWriter<T> writer;
	private transient Configuration parameters;
	private transient int taskId;
	private transient ListState<Long> cpIdState;
	private transient PartitionWriterFactory<T> partitionWriterFactory;
	private transient FileSystemStreamCommitter committer;

	private transient boolean hasPartCommitter;
	private transient TaskPartitionManager partManager;
	private transient GlobalPartitionCommitter partitionCommitter;

	private FileSystemStreamingSink(
			FileSystemFactory fsFactory,
			TableMetaStoreFactory msFactory,
			Path tmpPath,
			Path locationPath,
			String[] partitionColumns,
			LinkedHashMap<String, String> staticPartitions,
			OutputFormatFactory<T> formatFactory,
			PartitionComputer<T> computer,
			Map<String, String> properties) {
		this.fsFactory = fsFactory;
		this.msFactory = msFactory;
		this.tmpPath = tmpPath;
		this.locationPath = locationPath;
		this.partitionColumns = partitionColumns;
		this.staticPartitions = staticPartitions;
		this.formatFactory = formatFactory;
		this.computer = computer;
		this.properties = properties;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.parameters = parameters;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.taskId = getRuntimeContext().getIndexOfThisSubtask();

		List<PartitionCommitPolicy> commitChain = PartitionCommitPolicy.createCommitChain(properties);
		this.hasPartCommitter = commitChain.size() > 0;
		this.committer = new FileSystemStreamCommitter(
				fsFactory,
				msFactory,
				commitChain,
				tmpPath,
				locationPath,
				partitionColumns.length);

		if (hasPartCommitter) {
			this.partitionCommitter = new GlobalPartitionCommitter(
					(StreamingRuntimeContext) getRuntimeContext(), committer);
			this.partManager = new TaskPartitionManager(
					context, getRuntimeContext(), properties);
		}

		this.cpIdState = context.getOperatorStateStore().getUnionListState(CP_ID_STATE_DESC);
		this.partitionWriterFactory = PartitionWriterFactory.get(
				partitionColumns.length - staticPartitions.size() > 0,
				false,
				staticPartitions);
		createPartitionWriter(context.isRestored() ?
				cpIdState.get().iterator().next() + 1 : FIRST_CHECKPOINT_ID);
	}

	private void closePartitionWriter() throws Exception {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}

	private void createPartitionWriter(long checkpointId) throws Exception {
		PartitionTempFileManager fileManager = new PartitionTempFileManager(
				fsFactory, tmpPath, taskId, checkpointId);
		writer = partitionWriterFactory.create(
				new PartitionWriter.Context<>(parameters, formatFactory),
				fileManager,
				computer);
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		String partition = this.writer.write(value);

		if (this.hasPartCommitter) {
			this.partManager.invokeForPartition(partition, context.currentWatermark());
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		long cpId = context.getCheckpointId();
		this.cpIdState.clear();
		this.cpIdState.add(cpId);

		if (this.hasPartCommitter) {
			this.partManager.snapshotState(cpId);
		}

		closePartitionWriter();
		createPartitionWriter(cpId + 1);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		this.committer.commitTaskUpToCheckpoint(checkpointId, taskId);
		if (hasPartCommitter) {
			this.partitionCommitter.commit(
					checkpointId,
					this.partManager.triggeredPartitions(checkpointId));
		}
	}

	@Override
	public void close() throws Exception {
		closePartitionWriter();
		cleanTaskTempFiles(fsFactory.create(tmpPath.toUri()), tmpPath, taskId);
	}

	/**
	 * Builder to build {@link FileSystemOutputFormat}.
	 */
	public static class Builder<T> {

		private String[] partitionColumns;
		private OutputFormatFactory<T> formatFactory;
		private TableMetaStoreFactory metaStoreFactory;
		private Path tmpPath;
		private Path locationPath;

		private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();
		private FileSystemFactory fileSystemFactory = FileSystem::get;

		private PartitionComputer<T> computer;

		private Map<String, String> properties;

		public Builder<T> setPartitionColumns(String[] partitionColumns) {
			this.partitionColumns = partitionColumns;
			return this;
		}

		public Builder<T> setStaticPartitions(LinkedHashMap<String, String> staticPartitions) {
			this.staticPartitions = staticPartitions;
			return this;
		}

		public Builder<T> setFormatFactory(OutputFormatFactory<T> formatFactory) {
			this.formatFactory = formatFactory;
			return this;
		}

		public Builder<T> setFileSystemFactory(FileSystemFactory fileSystemFactory) {
			this.fileSystemFactory = fileSystemFactory;
			return this;
		}

		public Builder<T> setMetaStoreFactory(TableMetaStoreFactory metaStoreFactory) {
			this.metaStoreFactory = metaStoreFactory;
			return this;
		}

		public Builder<T> setTempPath(Path tmpPath) {
			this.tmpPath = tmpPath;
			return this;
		}

		public Builder<T> setLocationPath(Path locationPath) {
			this.locationPath = locationPath;
			return this;
		}

		public Builder<T> setPartitionComputer(PartitionComputer<T> computer) {
			this.computer = computer;
			return this;
		}

		public Builder<T> setProperties(Map<String, String> properties) {
			this.properties = properties;
			return this;
		}

		public FileSystemStreamingSink<T> build() {
			checkNotNull(partitionColumns, "partitionColumns should not be null");
			checkNotNull(formatFactory, "formatFactory should not be null");
			checkNotNull(metaStoreFactory, "metaStoreFactory should not be null");
			checkNotNull(tmpPath, "tmpPath should not be null");
			checkNotNull(computer, "partitionComputer should not be null");

			return new FileSystemStreamingSink<>(
					fileSystemFactory,
					metaStoreFactory,
					tmpPath,
					locationPath,
					partitionColumns,
					staticPartitions,
					formatFactory,
					computer,
					properties);
		}
	}
}
