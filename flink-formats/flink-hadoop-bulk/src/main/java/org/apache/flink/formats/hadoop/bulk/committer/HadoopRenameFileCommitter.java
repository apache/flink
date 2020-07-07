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

package org.apache.flink.formats.hadoop.bulk.committer;

import org.apache.flink.formats.hadoop.bulk.HadoopFileCommitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The Hadoop file committer that directly rename the in-progress file
 * to the target file. For FileSystem like S3, renaming may lead to
 * additional copies.
 */
public class HadoopRenameFileCommitter implements HadoopFileCommitter {

	private final Configuration configuration;

	private final Path targetFilePath;

	private final Path tempFilePath;

	public HadoopRenameFileCommitter(Configuration configuration, Path targetFilePath) throws IOException {
		this.configuration = configuration;
		this.targetFilePath = targetFilePath;
		this.tempFilePath = generateTempFilePath();
	}

	public HadoopRenameFileCommitter(
		Configuration configuration,
		Path targetFilePath,
		Path inProgressPath) throws IOException {

		this.configuration = configuration;
		this.targetFilePath = targetFilePath;
		this.tempFilePath = inProgressPath;
	}

	@Override
	public Path getTargetFilePath() {
		return targetFilePath;
	}

	@Override
	public Path getTempFilePath() {
		return tempFilePath;
	}

	@Override
	public void preCommit() {
		// Do nothing.
	}

	@Override
	public void commit() throws IOException {
		rename(true);
	}

	@Override
	public void commitAfterRecovery() throws IOException {
		rename(false);
	}

	private void rename(boolean assertFileExists) throws IOException {
		FileSystem fileSystem = FileSystem.get(targetFilePath.toUri(), configuration);

		if (!fileSystem.exists(tempFilePath)) {
			if (assertFileExists) {
				throw new IOException(String.format("In progress file(%s) not exists.", tempFilePath));
			} else {
				// By pass the re-commit if source file not exists.
				// TODO: in the future we may also need to check if the target file exists.
				return;
			}
		}

		try {
			// If file exists, it will be overwritten.
			fileSystem.rename(tempFilePath, targetFilePath);
		} catch (IOException e) {
			throw new IOException(
				String.format("Could not commit file from %s to %s", tempFilePath, targetFilePath),
				e);
		}
	}

	private Path generateTempFilePath() throws IOException {
		checkArgument(targetFilePath.isAbsolute(), "Target file must be absolute");

		FileSystem fileSystem = FileSystem.get(targetFilePath.toUri(), configuration);

		Path parent = targetFilePath.getParent();
		String name = targetFilePath.getName();

		while (true) {
			Path candidate = new Path(parent, "." + name + ".inprogress." + UUID.randomUUID().toString());
			if (!fileSystem.exists(candidate)) {
				return candidate;
			}
		}
	}
}
