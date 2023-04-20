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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.StateChangeUploadScheduler.UploadTask;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;

/**
 * The purpose of this interface is to abstract the different implementations of uploading state
 * changelog parts. It has a single {@link #upload} method with a collection of {@link UploadTask}
 * argument which is meant to initiate such an upload.
 */
@Internal
public interface StateChangeUploader extends AutoCloseable {
    /**
     * Execute the upload task and return the results. It is the caller responsibility to {@link
     * UploadTask#complete(List) complete} the tasks.
     */
    UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException;

    /** Result of executing one or more {@link UploadTask upload tasks}. */
    final class UploadTasksResult {
        private final Map<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> tasksOffsets;
        private final StreamStateHandle handle;
        private final StreamStateHandle localHandle;

        public UploadTasksResult(
                Map<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> tasksOffsets,
                StreamStateHandle handle) {
            this(tasksOffsets, handle, null);
        }

        public UploadTasksResult(
                Map<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> tasksOffsets,
                StreamStateHandle handle,
                @Nullable StreamStateHandle localHandle) {
            this.tasksOffsets = unmodifiableMap(tasksOffsets);
            this.handle = Preconditions.checkNotNull(handle);
            this.localHandle = localHandle;
        }

        public void complete() {
            for (Map.Entry<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> entry :
                    tasksOffsets.entrySet()) {
                UploadTask task = entry.getKey();
                Map<StateChangeSet, Tuple2<Long, Long>> offsets = entry.getValue();
                task.complete(buildResults(handle, offsets));
            }
        }

        private List<UploadResult> buildResults(
                StreamStateHandle handle, Map<StateChangeSet, Tuple2<Long, Long>> offsets) {
            return offsets.entrySet().stream()
                    .map(
                            e ->
                                    UploadResult.of(
                                            handle,
                                            localHandle,
                                            e.getKey(),
                                            e.getValue().f0,
                                            e.getValue().f1))
                    .collect(toList());
        }

        public long getStateSize() {
            return handle.getStateSize();
        }

        public void discard() throws Exception {
            handle.discardState();
        }

        @VisibleForTesting
        public StreamStateHandle getStreamStateHandle() {
            return handle;
        }
    }
}
