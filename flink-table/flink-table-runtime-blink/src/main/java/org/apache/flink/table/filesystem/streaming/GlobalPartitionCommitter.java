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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;
import org.apache.flink.table.filesystem.streaming.policy.PartitionCommitPolicy;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.table.filesystem.PartitionPathUtils.extractPartitionSpecFromPath;
import static org.apache.flink.table.filesystem.PartitionPathUtils.generatePartitionPath;

/**
 * Partition committer to commit partition to success file or metastore.
 */
public class GlobalPartitionCommitter {

	private final GlobalAggregateManager aggregateManager;
	private final GlobalCommitFunction commitFunction;
	private final int taskId;

	public GlobalPartitionCommitter(
			StreamingRuntimeContext context,
			TableMetaStoreFactory metaStoreFactory,
			List<PartitionCommitPolicy> policies) {
		this.aggregateManager = context.getGlobalAggregateManager();
		this.taskId = context.getIndexOfThisSubtask();
		this.commitFunction = new GlobalCommitFunction(
				context.getNumberOfParallelSubtasks(),
				metaStoreFactory,
				policies);
	}

	public void commit(
			long checkpointId, Collection<String> pendingParts) throws Exception {
		aggregateManager.updateGlobalAggregate(
				"commit",
				new CommitAggregateValue(
						checkpointId,
						taskId,
						pendingParts),
				commitFunction);
	}

	private static class GlobalCommitFunction implements
			AggregateFunction<CommitAggregateValue, TreeMap<Long, CpAccumulator>, Boolean> {

		private final int numberOfTasks;
		private final TableMetaStoreFactory metaStoreFactory;
		private final List<PartitionCommitPolicy> policies;

		GlobalCommitFunction(
				int numberOfTasks,
				TableMetaStoreFactory metaStoreFactory,
				List<PartitionCommitPolicy> policies) {
			this.numberOfTasks = numberOfTasks;
			this.metaStoreFactory = metaStoreFactory;
			this.policies = policies;
		}

		@Override
		public TreeMap<Long, CpAccumulator> createAccumulator() {
			return new TreeMap<>();
		}

		@Override
		public TreeMap<Long, CpAccumulator> add(CommitAggregateValue value, TreeMap<Long, CpAccumulator> accumulator) {
			accumulator.compute(value.checkpointId, (cpId, cpAcc) -> {
				cpAcc = cpAcc == null ? new CpAccumulator() : cpAcc;
				cpAcc.add(value);
				return cpAcc;
			});
			return accumulator;
		}

		@Override
		public Boolean getResult(TreeMap<Long, CpAccumulator> accumulator) {
			Long commitCpId = null;
			for (Map.Entry<Long, CpAccumulator> entry : accumulator.descendingMap().entrySet()) {
				if (entry.getValue().taskIds.size() == numberOfTasks) {
					commitCpId = entry.getKey();
					try {
						commitJobPartitions(entry.getValue().pendingParts);
					} catch (Exception e) {
						throw new TableException("Commit failed.", e);
					}
					break;
				}
			}
			if (commitCpId != null) {
				accumulator.headMap(commitCpId, true).clear();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public TreeMap<Long, CpAccumulator> merge(
				TreeMap<Long, CpAccumulator> accumulator, TreeMap<Long, CpAccumulator> b) {
			b.forEach((cpId, acc) -> accumulator.compute(cpId, (key, preAcc) -> {
				preAcc = preAcc == null ? new CpAccumulator() : preAcc;
				preAcc.merge(acc);
				return preAcc;
			}));
			return accumulator;
		}

		/**
		 * Commit give pending partitions to meta store.
		 */
		void commitJobPartitions(Set<String> pendingPartitions) throws Exception {
			try (TableMetaStoreFactory.TableMetaStore metaStore = metaStoreFactory.createTableMetaStore()) {
				FileSystem fs = metaStore.getLocationPath().getFileSystem();
				for (String partition : pendingPartitions) {
					LinkedHashMap<String, String> partSpec = extractPartitionSpecFromPath(new Path(partition));
					Path path = new Path(metaStore.getLocationPath(), generatePartitionPath(partSpec));
					for (PartitionCommitPolicy policy : policies) {
						policy.commit(partSpec, path, fs, metaStore);
					}
				}
			}
		}
	}

	private static class CpAccumulator implements Serializable {

		private Set<Integer> taskIds = new HashSet<>();
		private Set<String> pendingParts = new HashSet<>();

		public void add(CommitAggregateValue value) {
			taskIds.add(value.taskId);
			this.pendingParts.addAll(value.pendingParts);
		}

		public void merge(CpAccumulator acc) {
			taskIds.addAll(acc.taskIds);
			this.pendingParts.addAll(acc.pendingParts);
		}
	}

	private static class CommitAggregateValue implements Serializable {

		private long checkpointId;
		private int taskId;
		private Set<String> pendingParts;

		public CommitAggregateValue() {}

		CommitAggregateValue(long checkpointId, int taskId, Collection<String> pendingParts) {
			this.checkpointId = checkpointId;
			this.taskId = taskId;
			this.pendingParts = new HashSet<>(pendingParts);
		}
	}
}
