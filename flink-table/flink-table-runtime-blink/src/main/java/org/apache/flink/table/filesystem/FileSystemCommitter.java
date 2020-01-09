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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.TableMetaStoreFactory.TableMetaStore;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.filesystem.PartitionPathUtils.generatePartitionPath;
import static org.apache.flink.table.filesystem.PartitionTempFileManager.collectPartSpecToPaths;
import static org.apache.flink.table.filesystem.PartitionTempFileManager.listTaskTemporaryPaths;

/**
 * File system file committer implementation. It move all files to output path from temporary path.
 *
 * <p>For a batch job:
 *  1.Every task will create a {@link PartitionTempFileManager} to initialization, it generate path
 *  for task writing. And clean the temporary path of task.
 *  2.After writing done, need invoke {@link #commitJob}, will move the temporary files to real
 *  output path.
 *
 * <p>Data consistency:
 * 1.For task failure: will launch a new task and create a {@link PartitionTempFileManager},
 *   this will clean previous temporary files (This simple design can make it easy to delete the
 *   invalid temporary directory of the task, but it also causes that our directory does not
 *   support the same task to start multiple backups to run).
 * 2.For job master commit failure when overwrite: this may result in unfinished intermediate
 *   results, but if we try to run job again, the final result must be correct (because the
 *   intermediate result will be overwritten).
 * 3.For job master commit failure when append: This can lead to inconsistent data. But,
 *   considering that the commit action is a single point of execution, and only moves files and
 *   updates metadata, it will be faster, so the probability of inconsistency is relatively small.
 *
 * <p>See:
 * {@link PartitionTempFileManager}.
 * {@link PartitionLoader}.
 */
@Internal
class FileSystemCommitter implements Serializable {

	private static final long serialVersionUID = 1L;
	static final long CHECKPOINT_ID = 0;

	private final FileSystemFactory factory;
	private final TableMetaStoreFactory metaStoreFactory;
	private final boolean overwrite;
	private final Path tmpPath;
	private final int partitionColumnSize;

	FileSystemCommitter(
			FileSystemFactory factory,
			TableMetaStoreFactory metaStoreFactory,
			boolean overwrite,
			Path tmpPath,
			int partitionColumnSize) {
		this.factory = factory;
		this.metaStoreFactory = metaStoreFactory;
		this.overwrite = overwrite;
		this.tmpPath = tmpPath;
		this.partitionColumnSize = partitionColumnSize;
	}

	/**
	 * For committing job's output after successful batch job completion. Move all files
	 * to final output paths.
	 */
	void commitJob() throws Exception {
		FileSystem fs = factory.create(tmpPath.toUri());
		try (TableMetaStore metaStore = metaStoreFactory.createTableMetaStore()) {
			PartitionLoader loader = new PartitionLoader(overwrite, fs);
			List<Path> taskPaths = listTaskTemporaryPaths(fs, tmpPath, CHECKPOINT_ID);
			if (partitionColumnSize > 0) {
				for (Map.Entry<LinkedHashMap<String, String>, List<Path>> entry :
						collectPartSpecToPaths(fs, taskPaths, partitionColumnSize).entrySet()) {
					Optional<Path> pathFromMeta = metaStore.getPartition(entry.getKey());
					Path path = pathFromMeta.orElseGet(() ->
							new Path(metaStore.getLocationPath(), generatePartitionPath(entry.getKey())));
					loader.loadPath(entry.getValue(), path);
					if (!pathFromMeta.isPresent()) {
						metaStore.createPartition(entry.getKey(), path);
					}
				}
			} else {
				Path tableLocation = metaStore.getLocationPath();
				loader.loadPath(taskPaths, tableLocation);
			}
		} finally {
			fs.delete(tmpPath, true);
		}
	}
}
