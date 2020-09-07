/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

/**
 * This mapper copies files from an existing savepoint into a new directory.
 */
@Internal
public final class FileCopyMapFunction implements MapFunction<String, String> {

	private static final long serialVersionUID = 1L;
	private static final int BUFFER_SIZE = 4 * 1024;

	// the destination path to copy file
	private final String path;

	public FileCopyMapFunction(String path) {
		this.path = Preconditions.checkNotNull(path, "The destination path cannot be null");
	}

	private void copy(Path sourcePath, Path destPath) throws Exception {
		FSDataInputStream is = null;
		FSDataOutputStream os = null;
		try {
			os = destPath.getFileSystem().create(destPath, FileSystem.WriteMode.NO_OVERWRITE);
			is = sourcePath.getFileSystem().open(sourcePath);
			byte[] buffer = new byte[BUFFER_SIZE];
			int length;
			while ((length = is.read(buffer)) > 0) {
				os.write(buffer, 0, length);
			}
		} finally {
			is.close();
			os.close();
		}
	}

	@Override
	public String map(String sourceFile) throws Exception {
		// Create the destination parent directory before copy. It is not a problem if it exists already.
		Path destParent = new Path(path);
		destParent.getFileSystem().mkdirs(destParent);

		// Now copy file
		Path sourcePath = new Path(sourceFile);
		Path destPath = new Path(path, sourcePath.getName());
		copy(sourcePath, destPath);
		return sourceFile;
	}
}
