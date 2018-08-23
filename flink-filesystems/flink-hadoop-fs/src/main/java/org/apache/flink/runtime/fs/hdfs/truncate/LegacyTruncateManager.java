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

package org.apache.flink.runtime.fs.hdfs.truncate;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Concrete implementation of the {@link TruncateManager} which emulate truncate logic
 * for Hadoop version lower Hadoop 2.7.
 */
public class LegacyTruncateManager implements TruncateManager {

	public static final String DEFAULT_VALID_SUFFIX = ".valid-length";

	public static final String ORIGINAL_COPY_SUFFIX = ".original";

	private FileSystem hadoopFs;

	public LegacyTruncateManager(FileSystem hadoopFs) {
		this.hadoopFs = hadoopFs;
	}

	private Path generateValidFilePath(Path file) {
		return file.suffix(DEFAULT_VALID_SUFFIX);
	}

	private Path generateOriginalCopyFilePath(Path file) {
		return file.suffix(ORIGINAL_COPY_SUFFIX);
	}

	@Override
	public void truncate(Path filePath, long length) throws IOException {
		if (hadoopFs.exists(filePath)) {
			FSDataInputStream partFile = hadoopFs.open(filePath);
			Path validInProgressFilePath = generateValidFilePath(filePath);
			FSDataOutputStream validInProgressFile = hadoopFs.create(validInProgressFilePath, true);
			try {
				org.apache.hadoop.io.IOUtils.copyBytes(partFile, validInProgressFile, length, true);
				Path originalCopyFilePath = generateOriginalCopyFilePath(filePath);
				hadoopFs.rename(filePath, originalCopyFilePath);
				try {
					hadoopFs.rename(validInProgressFilePath, filePath);
				} catch (IOException e) {
					hadoopFs.rename(originalCopyFilePath, filePath);
					throw e;
				}
				hadoopFs.delete(originalCopyFilePath, false);
			} catch (IOException e) {
				if (hadoopFs.exists(validInProgressFilePath)) {
					hadoopFs.delete(validInProgressFilePath, false);
				}
				throw e;
			}
		}
	}
}
