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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.filesystem.FileSystemOutputFormat;
import org.apache.flink.table.filesystem.PartitionComputer;
import org.apache.flink.table.filesystem.PartitionPathUtils;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;
import org.apache.flink.table.filesystem.streaming.policy.PartitionCommitPolicy;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * File system sink for streaming jobs.
 */
public class FileSystemStreamingSink<T> extends StreamingFileSink<T>
		implements CheckpointedFunction, CheckpointListener {

	private static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

	private final TableMetaStoreFactory msFactory;
	private final PartitionComputer<T> computer;
	private final Map<String, String> properties;

	private transient boolean hasPartCommitter;
	private transient TaskPartitionManager partManager;
	private transient GlobalPartitionCommitter partitionCommitter;

	private FileSystemStreamingSink(
			RowFormatBuilder<T, ?, ?> bucketsBuilder,
			TableMetaStoreFactory msFactory,
			PartitionComputer<T> computer,
			Map<String, String> properties) {
		super(bucketsBuilder, DEFAULT_BUCKET_CHECK_INTERVAL);
		this.msFactory = msFactory;
		this.computer = computer;
		this.properties = properties;
	}

	private FileSystemStreamingSink(
			BulkFormatBuilder<T, ?, ?> bucketsBuilder,
			TableMetaStoreFactory msFactory,
			PartitionComputer<T> computer,
			Map<String, String> properties) {
		super(bucketsBuilder, DEFAULT_BUCKET_CHECK_INTERVAL);
		this.msFactory = msFactory;
		this.computer = computer;
		this.properties = properties;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		super.initializeState(context);

		List<PartitionCommitPolicy> commitChain = PartitionCommitPolicy.createCommitChain(properties);
		this.hasPartCommitter = commitChain.size() > 0;
		if (this.hasPartCommitter) {
			this.partitionCommitter = new GlobalPartitionCommitter(
					(StreamingRuntimeContext) getRuntimeContext(), msFactory, commitChain);
			this.partManager = new TaskPartitionManager(
					context, getRuntimeContext(), properties);
		}
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		super.invoke(value, context);
		String partition = PartitionPathUtils.generatePartitionPath(
				this.computer.generatePartValues(value));

		if (this.hasPartCommitter) {
			this.partManager.invokeForPartition(partition, context.currentWatermark());
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		super.snapshotState(context);
		if (this.hasPartCommitter) {
			this.partManager.snapshotState(context.getCheckpointId());
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		if (hasPartCommitter) {
			this.partitionCommitter.commit(
					checkpointId,
					this.partManager.triggeredPartitions(checkpointId));
		}
	}

	/**
	 * Builder to build {@link FileSystemOutputFormat}.
	 */
	public static class Builder<T> {

		private Path basePath;
		private String[] partitionColumns;
		private BulkWriter.Factory<T> bulkFormatWriterFactory;
		private Encoder<T> rowFormatEncoder;
		private TableMetaStoreFactory metaStoreFactory;

		private PartitionComputer<T> computer;

		private Map<String, String> properties;

		public Builder<T> setPartitionColumns(String[] partitionColumns) {
			this.partitionColumns = partitionColumns;
			return this;
		}

		public Builder<T> enableBulkFormat(BulkWriter.Factory<T> bulkFormatWriterFactory) {
			this.bulkFormatWriterFactory = bulkFormatWriterFactory;
			return this;
		}

		public Builder<T> enableRowFormat(Encoder<T> rowFormatEncoder) {
			this.rowFormatEncoder = rowFormatEncoder;
			return this;
		}

		public Builder<T> setMetaStoreFactory(TableMetaStoreFactory metaStoreFactory) {
			this.metaStoreFactory = metaStoreFactory;
			return this;
		}

		public Builder<T> setBasePath(Path basePath) {
			this.basePath = basePath;
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
			checkNotNull(metaStoreFactory, "metaStoreFactory should not be null");
			checkNotNull(basePath, "tmpPath should not be null");
			checkNotNull(computer, "partitionComputer should not be null");

			TableBucketAssigner<T> assigner = new TableBucketAssigner<>(computer);
			TableRollingPolicy<T> rollingPolicy = new TableRollingPolicy<>(
					Long.parseLong(properties.getOrDefault(
							FileSystemValidator.CONNECTOR_SINK_ROLLING_POLICY_FILE_SIZE, "0")),
					Long.parseLong(properties.getOrDefault(
							FileSystemValidator.CONNECTOR_SINK_ROLLING_POLICY_TIME_INTERVAL, "0"))
			);

			if (this.rowFormatEncoder != null && this.bulkFormatWriterFactory != null) {
				throw new UnsupportedOperationException("Can not set both bulkFormat and rowFormat.");
			} else if (this.rowFormatEncoder != null) {
				return new FileSystemStreamingSink<>(
						StreamingFileSink.forRowFormat(basePath, rowFormatEncoder)
								.withBucketAssigner(assigner)
								.withRollingPolicy(rollingPolicy),
						metaStoreFactory,
						computer,
						properties);
			} else if (this.bulkFormatWriterFactory != null) {
				return new FileSystemStreamingSink<>(
						StreamingFileSink.forBulkFormat(basePath, bulkFormatWriterFactory)
								.withBucketAssigner(assigner)
								.withRollingPolicy(rollingPolicy),
						metaStoreFactory,
						computer,
						properties);
			} else {
				throw new UnsupportedOperationException(
						"Should set a rowFormatEncoder or bulkFormatWriterFactory.");
			}
		}
	}
}
