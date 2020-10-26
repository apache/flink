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

package org.apache.flink.state.api.output;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;
import org.apache.flink.util.LambdaUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An output format to serialize {@link CheckpointMetadata} metadata to distributed storage.
 *
 * <p>This format may only be executed with parallelism 1.
 */
@Internal
public class SavepointOutputFormat extends RichOutputFormat<CheckpointMetadata> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(SavepointOutputFormat.class);

	private final Path savepointPath;

	private transient CheckpointStorageLocation targetLocation;

	public SavepointOutputFormat(Path savepointPath) {
		this.savepointPath = savepointPath;
	}

	@Override
	public void configure(Configuration parameters) {}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		Preconditions.checkState(numTasks == 1, "SavepointOutputFormat should only be executed with parallelism 1");

		targetLocation = createSavepointLocation(savepointPath);
	}

	@Override
	public void writeRecord(CheckpointMetadata metadata) throws IOException {
		String path = LambdaUtil.withContextClassLoader(getRuntimeContext().getUserCodeClassLoader(), () -> {
				try (CheckpointMetadataOutputStream out = targetLocation.createMetadataOutputStream()) {
					Checkpoints.storeCheckpointMetadata(metadata, out);
					CompletedCheckpointStorageLocation finalizedLocation = out.closeAndFinalizeCheckpoint();
					return finalizedLocation.getExternalPointer();
				}
		});

		LOG.info("Savepoint written to " + path);
	}

	@Override
	public void close() {}

	private static CheckpointStorageLocation createSavepointLocation(Path location) throws IOException {
		final CheckpointStorageLocationReference reference = AbstractFsCheckpointStorageAccess.encodePathAsReference(location);
		return new FsCheckpointStorageLocation(
			location.getFileSystem(),
			location,
			location,
			location,
			reference,
			(int) CheckpointingOptions.FS_SMALL_FILE_THRESHOLD.defaultValue().getBytes(),
			CheckpointingOptions.FS_WRITE_BUFFER_SIZE.defaultValue());
	}
}

