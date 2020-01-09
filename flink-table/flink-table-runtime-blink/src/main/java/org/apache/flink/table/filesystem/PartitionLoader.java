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
import org.apache.flink.util.Preconditions;

import java.util.List;

import static org.apache.flink.table.filesystem.PartitionPathUtils.listStatusWithoutHidden;

/**
 * Loader to temporary files to final output path. According to overwrite,
 * the loader will delete the previous data.
 *
 * <p>TODO: src and dest may be on different FS.
 */
@Internal
public class PartitionLoader {

	private final boolean overwrite;
	private final FileSystem fs;

	public PartitionLoader(boolean overwrite, FileSystem fs) {
		this.overwrite = overwrite;
		this.fs = fs;
	}

	public void loadPath(List<Path> srcDirs, Path destDir) throws Exception {
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
	 * Moves files from srcDir to destDir. Delete files in destDir first when overwrite.
	 */
	private void renameFiles(List<Path> srcDirs, Path destDir) throws Exception {
		for (Path srcDir : srcDirs) {
			if (!srcDir.equals(destDir)) {
				FileStatus[] srcFiles = listStatusWithoutHidden(fs, srcDir);
				if (srcFiles != null) {
					for (FileStatus srcFile : srcFiles) {
						Path srcPath = srcFile.getPath();
						Path destPath = new Path(destDir, srcPath.getName());
						int count = 1;
						while (!fs.rename(srcPath, destPath)) {
							String name = srcPath.getName() + "_copy_" + count;
							destPath = new Path(destDir, name);
							count++;
						}
					}
				}
			}
		}
	}
}
