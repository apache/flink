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

import java.util.ArrayList;
import java.util.Collection;

class TestingStateChangeStore implements StateChangeStore {
    private final Collection<StateChangeSet> saved = new ArrayList<>();
    private final Collection<StoreTask> savedTasks = new ArrayList<>();
    private boolean closed;

    @Override
    public void close() {
        this.closed = true;
    }

    public Collection<StateChangeSet> getSaved() {
        return saved;
    }

    public boolean isClosed() {
        return closed;
    }

    void reset() {
        saved.clear();
    }

    void finalizeUpload(boolean confirmed) {
        for (StateChangeSet stateChangeSet : saved) {
            // todo
            // stateChangeSet.setUploadStarted();
            // stateChangeSet.setUploaded(new StoreResult(null, 0, SequenceNumber.of(0)));
            // stateChangeSet.setSentToJm();
            // if (confirmed) {
            //     stateChangeSet.setConfirmed();
            // }
        }
    }

    public void failUpload() {
        for (StateChangeSet stateChangeSet : saved) {
            // todo
            // stateChangeSet.setFailed(new RuntimeException());
        }
    }

    @Override
    public void save(StoreTask storeTask) {
        savedTasks.add(storeTask);
        saved.addAll(storeTask.changeSets);
    }

    public Collection<StoreTask> getSavedTasks() {
        return savedTasks;
    }
}
