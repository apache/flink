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

package org.apache.flink.connector.file.sink.committer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committer implementation for {@link FileSink}.
 */
public class FileCommitter implements Committer<FileSinkCommittable> {
	private static final Logger LOG = LoggerFactory.getLogger(FileCommitter.class);

	private final BucketWriter<?, ?> bucketWriter;

	public FileCommitter(BucketWriter<?, ?> bucketWriter) {
		this.bucketWriter = checkNotNull(bucketWriter);
	}

	@Override
	public List<FileSinkCommittable> commit(List<FileSinkCommittable> committables)  {
		List<FileSinkCommittable> needRetry = new ArrayList<>();
		for (FileSinkCommittable committable : committables) {
			if (committable.hasPendingFile()) {
				// We should always use commitAfterRecovery which contains additional checks.
				try {
					bucketWriter.recoverPendingFile(committable.getPendingFile()).commitAfterRecovery();
				} catch (IOException e) {
					LOG.error("Failed to commit {}", committable.getPendingFile());
					needRetry.add(committable);
				}
			}

			if (committable.hasInProgressFileToCleanup()) {
				try {
					bucketWriter.cleanupInProgressFileRecoverable(committable.getInProgressFileToCleanup());
				} catch (IOException e) {
					LOG.error("Failed to cleanup {}", committable.getInProgressFileToCleanup());
					needRetry.add(committable);
				}
			}
		}

		return needRetry;
	}

	@Override
	public void close() throws Exception {
		// Do nothing.
	}
}
