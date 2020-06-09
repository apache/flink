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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.TableMetaStoreFactory.TableMetaStore;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;
import static org.apache.flink.table.utils.PartitionPathUtils.listStatusWithoutHidden;

/**
 * Loader to temporary files to final output path and meta store. According to overwrite,
 * the loader will delete the previous data.
 *
 * <p>This provide two interface to load:
 * 1.{@link #loadPartition}: load temporary partitioned files, if it is new partition,
 * will create partition to meta store.
 * 2.{@link #loadNonPartition}: just rename all files to final output path.
 *
 * <p>TODO: src and dest may be on different FS.
 */
@Internal
public class PartitionLoader implements Closeable {

	private final boolean overwrite;
	private final FileSystem fs;
	private final TableMetaStore metaStore;

	public PartitionLoader(
			boolean overwrite,
			FileSystem fs,
			TableMetaStoreFactory factory) throws Exception {
		this.overwrite = overwrite;
		this.fs = fs;
		this.metaStore = factory.createTableMetaStore();
	}

	/**
	 * Load a single partition.
	 */
	public void loadPartition(
			LinkedHashMap<String, String> partSpec, List<Path> srcDirs) throws Exception {
		Optional<Path> pathFromMeta = metaStore.getPartition(partSpec);
		Path path = pathFromMeta.orElseGet(() -> new Path(
				metaStore.getLocationPath(), generatePartitionPath(partSpec)));

		overwriteAndRenameFiles(srcDirs, path);
		metaStore.createOrAlterPartition(partSpec, path);
	}

	/**
	 * Load a non-partition files to output path.
	 */
	public void loadNonPartition(List<Path> srcDirs) throws Exception {
		Path tableLocation = metaStore.getLocationPath();
		overwriteAndRenameFiles(srcDirs, tableLocation);
	}

	private void overwriteAndRenameFiles(List<Path> srcDirs, Path destDir) throws Exception {
		boolean dirSuccessExist = fs.exists(destDir) || fs.mkdirs(destDir);
		Preconditions.checkState(dirSuccessExist, "Failed to create dest path " + destDir);
		overwrite(destDir);
		renameFiles(srcDirs, destDir);
	}

	private void overwrite(Path destDir) throws Exception {
		if (overwrite) {
			// delete existing files for overwrite
			FileStatus[] existingFiles = listStatusWithoutHidden(fs, destDir);
			if (existingFiles != null) {
				for (FileStatus existingFile : existingFiles) {
					// TODO: We need move to trash when auto-purge is false.
					fs.delete(existingFile.getPath(), true);
				}
			}
		}
	}

	/**
	 * Moves files from srcDir to destDir.
	 */
	private void renameFiles(List<Path> srcDirs, Path destDir) throws Exception {
		for (Path srcDir : srcDirs) {
			if (!srcDir.equals(destDir)) {
				FileStatus[] srcFiles = listStatusWithoutHidden(fs, srcDir);
				if (srcFiles != null) {
					for (FileStatus srcFile : srcFiles) {
						Path srcPath = srcFile.getPath();
						Path destPath = new Path(destDir, srcPath.getName());
						fs.rename(srcPath, destPath);
					}
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		metaStore.close();
	}
}
