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

import org.apache.flink.changelog.fs.StateChangeUploadScheduler.UploadTask;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

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
interface StateChangeUploader extends AutoCloseable {
    /**
     * Execute the upload task and return the results. It is the caller responsibility to {@link
     * UploadTask#complete(List) complete} the tasks.
     */
    UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException;

    final class UploadTasksResult {
        private final Map<UploadTask, Map<StateChangeSet, Long>> tasksOffsets;
        private final StreamStateHandle handle;

        public UploadTasksResult(
                Map<UploadTask, Map<StateChangeSet, Long>> tasksOffsets, StreamStateHandle handle) {
            this.tasksOffsets = unmodifiableMap(tasksOffsets);
            this.handle = Preconditions.checkNotNull(handle);
        }

        public void complete() {
            for (Map.Entry<UploadTask, Map<StateChangeSet, Long>> entry : tasksOffsets.entrySet()) {
                UploadTask task = entry.getKey();
                Map<StateChangeSet, Long> offsets = entry.getValue();
                task.complete(buildResults(handle, offsets));
            }
        }

        private List<UploadResult> buildResults(
                StreamStateHandle handle, Map<StateChangeSet, Long> offsets) {
            return offsets.entrySet().stream()
                    .map(e -> UploadResult.of(handle, e.getKey(), e.getValue()))
                    .collect(toList());
        }

        public long getStateSize() {
            return handle.getStateSize();
        }

        public void discard() throws Exception {
            handle.discardState();
        }
    }
}
