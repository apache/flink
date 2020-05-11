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

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The Hadoop file committer that directly rename the in-progress file
 * to the target file. For FileSystem like S3, renaming may lead to
 * additional copies.
 */
public class HadoopRenameFileCommitter implements HadoopFileCommitter {

	private final Configuration configuration;

	private final Path targetFilePath;

	private final Path inProgressFilePath;

	public HadoopRenameFileCommitter(Configuration configuration, Path targetFilePath) {
		this.configuration = configuration;
		this.targetFilePath = targetFilePath;
		this.inProgressFilePath = generateInProgressFilePath();
	}

	@Override
	public Path getTargetFilePath() {
		return targetFilePath;
	}

	@Override
	public Path getInProgressFilePath() {
		return inProgressFilePath;
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

		if (!fileSystem.exists(inProgressFilePath)) {
			if (assertFileExists) {
				throw new IOException(String.format("In progress file(%s) not exists.", inProgressFilePath));
			} else {

				// By pass the re-commit if source file not exists.
				// TODO: in the future we may also need to check if the target file exists.
				return;
			}
		}

		try {
			// If file exists, it will be overwritten.
			fileSystem.rename(inProgressFilePath, targetFilePath);
		} catch (IOException e) {
			throw new IOException(
				String.format("Could not commit file from %s to %s", inProgressFilePath, targetFilePath),
				e);
		}
	}

	private Path generateInProgressFilePath() {
		checkArgument(targetFilePath.isAbsolute(), "Target file must be absolute");

		Path parent = targetFilePath.getParent();
		String name = targetFilePath.getName();

		return new Path(parent, "." + name + ".inprogress");
	}
}
