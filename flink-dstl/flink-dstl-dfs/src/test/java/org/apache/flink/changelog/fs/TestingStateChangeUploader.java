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
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

class TestingStateChangeUploader implements StateChangeUploader {
    private final Collection<StateChangeSet> uploaded = new CopyOnWriteArrayList<>();
    private final List<UploadTask> tasks;
    private boolean closed;

    TestingStateChangeUploader() {
        tasks = new CopyOnWriteArrayList<>();
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException {
        for (UploadTask uploadTask : tasks) {
            this.uploaded.addAll(uploadTask.changeSets);
            this.tasks.add(uploadTask);
        }
        return new UploadTasksResult(emptyMap(), new ByteStreamStateHandle("", new byte[0]));
    }

    public Collection<StateChangeSet> getUploaded() {
        return uploaded;
    }

    public boolean isClosed() {
        return closed;
    }

    public void reset() {
        uploaded.clear();
        tasks.clear();
    }

    public void failUpload(RuntimeException exception) {
        tasks.forEach(t -> t.fail(exception));
    }

    public void completeUpload() {
        tasks.forEach(
                t ->
                        t.complete(
                                uploaded.stream()
                                        .map(
                                                changeSet ->
                                                        new UploadResult(
                                                                asBytesHandle(changeSet),
                                                                0L,
                                                                changeSet.getSequenceNumber(),
                                                                changeSet.getSize()))
                                        .collect(toList())));
    }

    private StreamStateHandle asBytesHandle(StateChangeSet changeSet) {
        byte[] bytes = new byte[(int) changeSet.getSize()];
        int offset = 0;
        for (StateChange change : changeSet.getChanges()) {
            for (int i = 0; i < change.getChange().length; i++) {
                bytes[offset++] = change.getChange()[i];
            }
        }
        return new ByteStreamStateHandle("", bytes);
    }
}
