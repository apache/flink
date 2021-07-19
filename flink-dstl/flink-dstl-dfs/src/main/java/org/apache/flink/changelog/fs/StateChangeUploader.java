/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.changelog.SequenceNumber;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.BASE_PATH;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.COMPRESSION_ENABLED;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.IN_FLIGHT_DATA_LIMIT;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.NUM_UPLOAD_THREADS;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PERSIST_DELAY;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PERSIST_SIZE_THRESHOLD;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.UPLOAD_BUFFER_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;

// todo: consider using CheckpointStreamFactory / CheckpointStorageWorkerView
//     Considerations:
//     0. need for checkpointId in the current API to resolve the location
//       option a: pass checkpointId (race condition?)
//       option b: pass location (race condition?)
//       option c: add FsCheckpointStorageAccess.createSharedStateStream
//     1. different settings for materialized/changelog (e.g. timeouts)
//     2. re-use closeAndGetHandle
//     3. re-use in-memory handles (.metadata)
//     4. handle in-memory handles duplication

/**
 * The purpose of this interface is to abstract the different implementations of uploading state
 * changelog parts. It has a single {@link #upload} method with a single {@link UploadTask} argument
 * which is meant to initiate such an upload.
 */
interface StateChangeUploader extends AutoCloseable {

    void upload(UploadTask uploadTask) throws IOException;

    default void upload(Collection<UploadTask> tasks) throws IOException {
        for (UploadTask task : tasks) {
            upload(task);
        }
    }

    static StateChangeUploader fromConfig(ReadableConfig config) throws IOException {
        Path basePath = new Path(config.get(BASE_PATH));
        long bytes = config.get(UPLOAD_BUFFER_SIZE).getBytes();
        checkArgument(bytes <= Integer.MAX_VALUE);
        int bufferSize = (int) bytes;
        StateChangeFsUploader store =
                new StateChangeFsUploader(
                        basePath,
                        basePath.getFileSystem(),
                        config.get(COMPRESSION_ENABLED),
                        bufferSize);
        BatchingStateChangeUploader batchingStore =
                new BatchingStateChangeUploader(
                        config.get(PERSIST_DELAY).toMillis(),
                        config.get(PERSIST_SIZE_THRESHOLD).getBytes(),
                        RetryPolicy.fromConfig(config),
                        store,
                        config.get(NUM_UPLOAD_THREADS),
                        config.get(IN_FLIGHT_DATA_LIMIT).getBytes());
        return batchingStore;
    }

    @ThreadSafe
    final class UploadTask {
        final Collection<StateChangeSet> changeSets;
        final BiConsumer<List<SequenceNumber>, Throwable> failureCallback;
        final Consumer<List<UploadResult>> successCallback;
        final AtomicBoolean finished = new AtomicBoolean();

        public UploadTask(
                Collection<StateChangeSet> changeSets,
                Consumer<List<UploadResult>> successCallback,
                BiConsumer<List<SequenceNumber>, Throwable> failureCallback) {
            this.changeSets = new ArrayList<>(changeSets);
            this.failureCallback = failureCallback;
            this.successCallback = successCallback;
        }

        public void complete(List<UploadResult> results) {
            if (finished.compareAndSet(false, true)) {
                successCallback.accept(results);
            }
        }

        public void fail(Throwable error) {
            if (finished.compareAndSet(false, true)) {
                failureCallback.accept(
                        changeSets.stream()
                                .map(StateChangeSet::getSequenceNumber)
                                .collect(toList()),
                        error);
            }
        }

        public long getSize() {
            long size = 0;
            for (StateChangeSet set : changeSets) {
                size = set.getSize();
            }
            return size;
        }

        @Override
        public String toString() {
            return "changeSets=" + changeSets;
        }
    }
}
