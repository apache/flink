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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.FileSystemFactory;
import org.apache.flink.table.filesystem.PartitionLoader;
import org.apache.flink.table.filesystem.PartitionTempFileManager;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;
import org.apache.flink.table.filesystem.streaming.policy.PartitionCommitPolicy;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.filesystem.PartitionPathUtils.extractPartitionSpecFromPath;
import static org.apache.flink.table.filesystem.PartitionPathUtils.generatePartitionPath;
import static org.apache.flink.table.filesystem.PartitionPathUtils.searchPartSpecAndPaths;
import static org.apache.flink.table.filesystem.PartitionTempFileManager.deleteCheckpoint;
import static org.apache.flink.table.filesystem.PartitionTempFileManager.getTaskTempDir;
import static org.apache.flink.table.filesystem.PartitionTempFileManager.headCheckpoints;

/**
 * Streaming File system file committer implementation. It move all files to output path from temporary path.
 *
 * <p>In a checkpoint:
 *  1.Every task will create a {@link PartitionTempFileManager} to initialization, it generate path
 *  for task writing. And clean the temporary path of task.
 *  2.Every task will invoke {@link #commitTaskUpToCheckpoint} to move files to location path.
 *  3.After task commit finish, invoke {@link #commitJobPartitions(Set)} to add partitions to meta store,
 *  meta store related operations should be finish in a single point, meta store can not be touched by tasks.
 *
 * <p>Data consistency:
 * 1.For task failure: will launch a new task and create a {@link PartitionTempFileManager},
 *   this will clean previous temporary files (This simple design can make it easy to delete the
 *   invalid temporary directory of the task, but it also causes that our directory does not
 *   support the same task to start multiple backups to run).
 * 2.For notify checkpoint complete commit failure when append: This can lead to inconsistent data.
 *   But, considering that the commit action is a quick execution, and only moves files and updates
 *   metadata, it will be faster, so the probability of inconsistency is relatively small.
 *
 * <p>See:
 * {@link PartitionTempFileManager}.
 * {@link PartitionLoader}.
 */
class FileSystemStreamCommitter implements Serializable {

	private static final long serialVersionUID = 1L;

	private final FileSystemFactory factory;
	private final TableMetaStoreFactory metaStoreFactory;
	private final List<PartitionCommitPolicy> policies;
	private final Path tmpPath;
	private final Path locationPath;
	private final int partitionColumnSize;

	FileSystemStreamCommitter(
			FileSystemFactory factory,
			TableMetaStoreFactory metaStoreFactory,
			List<PartitionCommitPolicy> policies,
			Path tmpPath,
			Path locationPath,
			int partitionColumnSize) {
		this.factory = factory;
		this.metaStoreFactory = metaStoreFactory;
		this.policies = policies;
		this.tmpPath = tmpPath;
		this.locationPath = locationPath;
		this.partitionColumnSize = partitionColumnSize;
	}

	/**
	 * For committing job's output after one checkpoint finish for streaming job.
	 * Move all files to final output paths.
	 *
	 * <p>NOTE: According to checkpoint notify mechanism of Flink, checkpoint may fail and be
	 * abandoned, so this method should commit all checkpoint ids that less than current
	 * checkpoint id (Includes failure checkpoints).
	 */
	void commitTaskUpToCheckpoint(long toCpId, int taskId) throws Exception {
		FileSystem fs = factory.create(tmpPath.toUri());

		PartitionLoader pathLoader = new PartitionLoader(false, fs);
		for (long cp : headCheckpoints(fs, tmpPath, toCpId)) {
			try {
				Path taskTmpDir = getTaskTempDir(tmpPath, cp, taskId);
				if (partitionColumnSize > 0) {
					for (Tuple2<LinkedHashMap<String, String>, Path> entry :
							searchPartSpecAndPaths(fs, taskTmpDir, partitionColumnSize)) {
						pathLoader.loadPath(
								Collections.singletonList(entry.f1),
								new Path(locationPath, generatePartitionPath(entry.f0)));
					}
				} else {
					pathLoader.loadPath(Collections.singletonList(taskTmpDir), locationPath);
				}
			} finally {
				deleteCheckpoint(fs, tmpPath, cp);
			}
		}
	}

	/**
	 * Commit give pending partitions to meta store.
	 */
	void commitJobPartitions(Set<String> pendingPartitions) throws Exception {
		FileSystem fs = factory.create(tmpPath.toUri());
		try (TableMetaStoreFactory.TableMetaStore metaStore = metaStoreFactory.createTableMetaStore()) {
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
