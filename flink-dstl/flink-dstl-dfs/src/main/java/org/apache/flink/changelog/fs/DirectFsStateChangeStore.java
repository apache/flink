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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

class DirectFsStateChangeStore implements StateChangeStore {
    private static final Logger LOG = LoggerFactory.getLogger(DirectFsStateChangeStore.class);

    private final Path basePath;
    private final FileSystem fileSystem;
    private final StateChangeFormat format;

    public DirectFsStateChangeStore(Path basePath, FileSystem fileSystem) {
        this.basePath = basePath;
        this.fileSystem = fileSystem;
        this.format = new StateChangeFormat();
    }

    @Override
    public void save(StoreTask storeTask) throws IOException {
        save(Collections.singleton(storeTask));
    }

    @Override
    public void save(Collection<StoreTask> tasks) throws IOException {
        final String fileName = generateFileName();
        LOG.debug("upload {} to {}", tasks, fileName);
        Path path = new Path(basePath, fileName);
        try {
            try (FSDataOutputStream os = fileSystem.create(path, NO_OVERWRITE)) {
                upload(tasks, path, os);
            }
        } catch (IOException e) {
            tasks.forEach(cs -> cs.fail(e));
            handleError(path, e);
        }
    }

    private void handleError(Path path, IOException e) throws IOException {
        try {
            fileSystem.delete(path, true);
        } catch (IOException cleanupError) {
            LOG.warn("unable to delete after failure: " + path, cleanupError);
            e.addSuppressed(cleanupError);
        }
        throw e;
    }

    private void upload(Collection<StoreTask> tasks, Path path, FSDataOutputStream os)
            throws IOException {
        Map<StoreTask, Map<StateChangeSet, Long>> tasksOffsets = new HashMap<>();
        for (StoreTask task : tasks) {
            tasksOffsets.put(task, format.write(os, task.changeSets));
        }
        final long size = os.getPos();
        os.close(); // todo: finally / outside
        final StreamStateHandle handle = new FileStateHandle(path, size);
        tasksOffsets.forEach((task, offsets) -> task.complete(buildResults(handle, offsets)));
        // todo: validate nontempty
        //        if (offsets.isEmpty()) {
        //            LOG.info(
        //                "nothing to upload (cancelled concurrently), cancelling upload {} {}",
        //                changeSets.size(),
        //                path);
        //            fileSystem.delete(path, true); // todo
        //        }
    }

    private List<StoreResult> buildResults(
            StreamStateHandle handle, Map<StateChangeSet, Long> offsets) {
        return offsets.entrySet().stream()
                .map(e -> new StoreResult(handle, e.getValue(), e.getKey().getSequenceNumber()))
                .collect(Collectors.toList());
    }

    private String generateFileName() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void close() {}
}
