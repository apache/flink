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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.filesystem.EmptyMetaStoreFactory;
import org.apache.flink.table.filesystem.FileSystemFactory;
import org.apache.flink.table.filesystem.MetastoreCommitPolicy;
import org.apache.flink.table.filesystem.PartitionCommitPolicy;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME;
import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionSpecFromPath;
import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/**
 * Committer operator for partitions. This is the single (non-parallel) task.
 * It collects all the partition information sent from upstream, and triggers the partition
 * submission decision when it judges to collect the partitions from all tasks of a checkpoint.
 *
 * <p>NOTE: It processes records after the checkpoint completes successfully.
 * Receive records from upstream {@link CheckpointListener#notifyCheckpointComplete}.
 *
 * <p>Processing steps:
 * 1.Partitions are sent from upstream. Add partition to trigger.
 * 2.{@link TaskTracker} say it have already received partition data from all tasks in a checkpoint.
 * 3.Extracting committable partitions from {@link PartitionCommitTrigger}.
 * 4.Using {@link PartitionCommitPolicy} chain to commit partitions.
 */
public class PartitionCommitter extends AbstractStreamOperator<Void>
		implements OneInputStreamOperator<PartitionCommitInfo, Void> {

	private static final long serialVersionUID = 1L;

	private final Configuration conf;

	private final Path locationPath;

	private final ObjectIdentifier tableIdentifier;

	private final List<String> partitionKeys;

	private final TableMetaStoreFactory metaStoreFactory;

	private final FileSystemFactory fsFactory;

	private transient PartitionCommitTrigger trigger;

	private transient TaskTracker taskTracker;

	private transient long currentWatermark;

	private transient List<PartitionCommitPolicy> policies;

	public PartitionCommitter(
			Path locationPath,
			ObjectIdentifier tableIdentifier,
			List<String> partitionKeys,
			TableMetaStoreFactory metaStoreFactory,
			FileSystemFactory fsFactory,
			Configuration conf) {
		this.locationPath = locationPath;
		this.tableIdentifier = tableIdentifier;
		this.partitionKeys = partitionKeys;
		this.metaStoreFactory = metaStoreFactory;
		this.fsFactory = fsFactory;
		this.conf = conf;
		PartitionCommitPolicy.validatePolicyChain(
				metaStoreFactory instanceof EmptyMetaStoreFactory,
				conf.get(SINK_PARTITION_COMMIT_POLICY_KIND));
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		this.currentWatermark = Long.MIN_VALUE;
		this.trigger = PartitionCommitTrigger.create(
				context.isRestored(),
				context.getOperatorStateStore(),
				conf,
				getUserCodeClassloader(),
				partitionKeys,
				getProcessingTimeService());
		this.policies = PartitionCommitPolicy.createPolicyChain(
				getUserCodeClassloader(),
				conf.get(SINK_PARTITION_COMMIT_POLICY_KIND),
				conf.get(SINK_PARTITION_COMMIT_POLICY_CLASS),
				conf.get(SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME),
				() -> {
					try {
						return fsFactory.create(locationPath.toUri());
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
	}

	@Override
	public void processElement(StreamRecord<PartitionCommitInfo> element) throws Exception {
		PartitionCommitInfo message = element.getValue();
		for (String partition : message.getPartitions()) {
			trigger.addPartition(partition);
		}

		if (taskTracker == null) {
			taskTracker = new TaskTracker(message.getNumberOfTasks());
		}
		boolean needCommit = taskTracker.add(message.getCheckpointId(), message.getTaskId());
		if (needCommit) {
			commitPartitions(message.getCheckpointId());
		}
	}

	private void commitPartitions(long checkpointId) throws Exception {
		List<String> partitions = checkpointId == Long.MAX_VALUE ?
				trigger.endInput() :
				trigger.committablePartitions(checkpointId);
		if (partitions.isEmpty()) {
			return;
		}

		try (TableMetaStoreFactory.TableMetaStore metaStore = metaStoreFactory.createTableMetaStore()) {
			for (String partition : partitions) {
				LinkedHashMap<String, String> partSpec = extractPartitionSpecFromPath(new Path(partition));
				LOG.info("Partition {} of table {} is ready to be committed", partSpec, tableIdentifier);
				Path path = new Path(locationPath, generatePartitionPath(partSpec));
				PartitionCommitPolicy.Context context = new CommitPolicyContextImpl(
						new ArrayList<>(partSpec.values()), path);
				for (PartitionCommitPolicy policy : policies) {
					if (policy instanceof MetastoreCommitPolicy) {
						((MetastoreCommitPolicy) policy).setMetastore(metaStore);
					}
					policy.commit(context);
				}
			}
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		this.currentWatermark = mark.getTimestamp();
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		trigger.snapshotState(context.getCheckpointId(), currentWatermark);
	}

	private class CommitPolicyContextImpl implements PartitionCommitPolicy.Context {

		private final List<String> partitionValues;
		private final Path partitionPath;

		private CommitPolicyContextImpl(List<String> partitionValues, Path partitionPath) {
			this.partitionValues = partitionValues;
			this.partitionPath = partitionPath;
		}

		@Override
		public String catalogName() {
			return tableIdentifier.getCatalogName();
		}

		@Override
		public String databaseName() {
			return tableIdentifier.getDatabaseName();
		}

		@Override
		public String tableName() {
			return tableIdentifier.getObjectName();
		}

		@Override
		public List<String> partitionKeys() {
			return partitionKeys;
		}

		@Override
		public List<String> partitionValues() {
			return partitionValues;
		}

		@Override
		public Path partitionPath() {
			return partitionPath;
		}
	}
}
