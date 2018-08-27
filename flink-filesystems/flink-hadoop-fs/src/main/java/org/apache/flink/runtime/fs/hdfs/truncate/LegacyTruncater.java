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
 * Concrete implementation of the {@link Truncater} which emulate truncate logic
 * for Hadoop version lower Hadoop 2.7.
 * Truncate logic:
 * 		1. Create a new file with '.truncated' extension in the same folder and
 * 		write the content of the file with the required length.
 * 		2. Remove the original file
 * 		3. Rename truncated file using the name of the original one.
 * In case of failure :
 * 	On the first step of invocation of ‘truncate’ method check if the original file exist:
 * 		a. If original file exists - start the process from the beginning (point 1).
 * 		b. If original file not exists but exists the file with '*.truncated' extension.
 * 	The absence of original file tells us about that truncated file was written fully and source crushed on the
 * stage of renaming truncated file. We can use it as a resultant file and finish the truncation process.
 */
public class LegacyTruncater implements Truncater {

	private static final String TRUNCATED_FILE_SUFFIX = ".truncated";

	private final FileSystem fs;

	public LegacyTruncater(FileSystem fs) {
		this.fs = fs;
	}

	protected static Path truncatedFile(Path file) {
		return file.suffix(TRUNCATED_FILE_SUFFIX);
	}

	protected void copyFileContent(Path src, Path dest, long length) throws IOException {
		FSDataInputStream srcFile = fs.open(src);
		FSDataOutputStream destFile = fs.create(dest, true);
		try {
			org.apache.hadoop.io.IOUtils.copyBytes(srcFile, destFile, length, true);
		} catch (IOException e) {
			if (fs.exists(dest)) {
				fs.delete(dest, false);
			}
			throw e;
		}
	}

	protected void touch(Path filePath) throws IOException {
		fs.delete(filePath, false);
		fs.create(filePath).close();
	}

	@Override
	public void truncate(Path filePath, long length) throws IOException {
		Path truncatedFilePath = truncatedFile(filePath);

		if (fs.exists(filePath)) {
			long fileSize = fs.getFileStatus(filePath).getLen();

			// Check the size and if the file should be truncated
			// to the zero size just recreate an empty file.
			if (length == 0L) {
				touch(filePath);

				// Ignore the case of the required size is equal actual file size
				// Apply truncation logic only for cases when the size is differ
				// Good question: should we handle the case when file size is smaller
				// than required truncation length ?
			} else if (length < fileSize) {
				copyFileContent(filePath, truncatedFilePath, length);
				fs.delete(filePath, false);
				fs.rename(truncatedFilePath, filePath);
			}
			// Recovery point for the case when the fail happen after removing of the original file.
		} else if (fs.exists(truncatedFilePath)) {
			fs.rename(truncatedFilePath, filePath);
		} else {
			throw new IOException("File cannot be truncated. Original file '" + filePath +
				"' and truncated copy '" + truncatedFilePath + "' are not exist.");
		}
	}
}
