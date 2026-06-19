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

package org.apache.flink.changelog.fs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.StateChangeUploadScheduler.UploadTask;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestingStreamStateHandle;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link StateChangeUploader} implementation which can verify that the returned StreamStateHandle
 * was deleted.
 */
public class DiscardRecordableStateChangeUploader implements StateChangeUploader {
    private final TaskChangelogRegistry changelogRegistry;

    public DiscardRecordableStateChangeUploader(TaskChangelogRegistry changelogRegistry) {
        this.changelogRegistry = changelogRegistry;
    }

    @Override
    public UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException {
        Map<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> tasksOffsets = new HashMap<>();

        for (UploadTask task : tasks) {
            Map<StateChangeSet, Tuple2<Long, Long>> offsets = new HashMap<>();
            for (StateChangeSet changeSet : task.getChangeSets()) {
                offsets.put(changeSet, Tuple2.of(0L, 0L)); // fake offsets
            }
            tasksOffsets.put(task, offsets);
        }

        long numOfChangeSets = tasks.stream().flatMap(t -> t.getChangeSets().stream()).count();

        // fake StreamStateHandle without data, just for discarding records
        StreamStateHandle handle = new TestingStreamStateHandle();
        StreamStateHandle localHandle = new TestingStreamStateHandle();
        changelogRegistry.startTracking(handle, numOfChangeSets);
        return new UploadTasksResult(tasksOffsets, handle, localHandle);
    }

    public boolean isDiscarded(StreamStateHandle handle) {
        if (handle instanceof TestingStreamStateHandle) {
            return ((TestingStreamStateHandle) handle).isDisposed();
        } else {
            throw new IllegalStateException(
                    "only accept StreamStateHandle created by DiscardRecordableStateChangeUploader");
        }
    }

    @Override
    public void close() throws Exception {}
}
