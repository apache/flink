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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committer implementation for {@link FileSink}.
 *
 * <p>This committer is responsible for taking staged part-files, i.e. part-files in "pending"
 * state, created by the {@link org.apache.flink.connector.file.sink.writer.FileWriter FileWriter}
 * and commit them, or put them in "finished" state and ready to be consumed by downstream
 * applications or systems.
 */
@Internal
public class FileCommitter implements Committer<FileSinkCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(FileCommitter.class);

    private final BucketWriter<?, ?> bucketWriter;

    public FileCommitter(BucketWriter<?, ?> bucketWriter) {
        this.bucketWriter = checkNotNull(bucketWriter);
    }

    @Override
    public void commit(Collection<CommitRequest<FileSinkCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<FileSinkCommittable> request : requests) {
            FileSinkCommittable committable = request.getCommittable();
            if (committable.hasPendingFile()) {
                // We should always use commitAfterRecovery which contains additional checks.
                bucketWriter.recoverPendingFile(committable.getPendingFile()).commitAfterRecovery();
            }

            if (committable.hasInProgressFileToCleanup()) {
                bucketWriter.cleanupInProgressFileRecoverable(
                        committable.getInProgressFileToCleanup());
            }

            if (committable.hasCompactedFileToCleanup()) {
                Path committedFileToCleanup = committable.getCompactedFileToCleanup();
                try {
                    committedFileToCleanup.getFileSystem().delete(committedFileToCleanup, false);
                } catch (Exception e) {
                    // Try best to cleanup compacting files, skip if failed.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Failed to cleanup a compacted file, the file will be remained and should not be visible: {}",
                                committedFileToCleanup,
                                e);
                    }
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        // Do nothing.
    }
}
