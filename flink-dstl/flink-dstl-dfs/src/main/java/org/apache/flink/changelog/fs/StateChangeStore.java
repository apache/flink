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

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.BASE_PATH;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.BATCH_ENABLED;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PERSIST_DELAY_MS;
import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PERSIST_SIZE_THRESHOLD;

// todo in MVP or later: implement CheckpointStreamFactory / CheckpointStorageWorkerView - based
// store
// Considerations:
// 0. need for checkpointId in the current API to resolve the location
//   option a: pass checkpointId (race condition?)
//   option b: pass location (race condition?)
//   option c: add FsCheckpointStorageAccess.createSharedStateStream
// 1. different settings for materialized/changelog (e.g. timeouts)
// 2. re-use closeAndGetHandle
// 3. re-use in-memory handles (.metadata)
// 4. handle in-memory handles duplication
interface StateChangeStore extends AutoCloseable {

    @ThreadSafe
    class StoreTask {
        final Collection<StateChangeSet> changeSets;
        final CompletableFuture<List<StoreResult>> resultFuture = new CompletableFuture<>();

        public StoreTask(Collection<StateChangeSet> changeSets) {
            this.changeSets = changeSets;
        }

        public CompletableFuture<List<StoreResult>> getResultFuture() {
            return resultFuture;
        }

        public void complete(List<StoreResult> results) {
            resultFuture.complete(results);
        }

        public void fail(Throwable error) {
            resultFuture.completeExceptionally(error);
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

    void save(StoreTask storeTask) throws IOException;

    default void save(Collection<StoreTask> tasks) throws IOException {
        for (StoreTask task : tasks) {
            save(task);
        }
    }

    static StateChangeStore fromConfig(ReadableConfig config) throws IOException {
        DirectFsStateChangeStore store = createDirectFsStore(new Path(config.get(BASE_PATH)));
        if (config.get(BATCH_ENABLED)) {
            return createBatchingStore(
                    config.get(PERSIST_DELAY_MS),
                    config.get(PERSIST_SIZE_THRESHOLD).getBytes(),
                    store);
        } else {
            return store;
        }
    }

    static DirectFsStateChangeStore createDirectFsStore(Path basePath) throws IOException {
        return new DirectFsStateChangeStore(basePath, basePath.getFileSystem());
    }

    static StateChangeStore createBatchingStore(
            long persistDelayMs, long persistSizeThreshold, DirectFsStateChangeStore delegate) {
        return new BatchingStateChangeStore(persistDelayMs, persistSizeThreshold, delegate);
    }
}
