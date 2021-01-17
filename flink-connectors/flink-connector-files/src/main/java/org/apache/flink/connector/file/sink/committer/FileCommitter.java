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
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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

    private final BucketWriter<?, ?> bucketWriter;

    public FileCommitter(BucketWriter<?, ?> bucketWriter) {
        this.bucketWriter = checkNotNull(bucketWriter);
    }

    @Override
    public List<FileSinkCommittable> commit(List<FileSinkCommittable> committables)
            throws IOException {
        for (FileSinkCommittable committable : committables) {
            if (committable.hasPendingFile()) {
                // We should always use commitAfterRecovery which contains additional checks.
                bucketWriter.recoverPendingFile(committable.getPendingFile()).commitAfterRecovery();
            }

            if (committable.hasInProgressFileToCleanup()) {
                bucketWriter.cleanupInProgressFileRecoverable(
                        committable.getInProgressFileToCleanup());
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // Do nothing.
    }
}
