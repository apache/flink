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
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

/**
 * A synchronous {@link StateChangeUploader} implementation that uploads the changes using {@link
 * FileSystem}.
 */
class StateChangeFsUploader implements StateChangeUploader {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeFsUploader.class);

    private final Path basePath;
    private final FileSystem fileSystem;
    private final StateChangeFormat format;
    private final boolean compression;
    private final int bufferSize;

    public StateChangeFsUploader(
            Path basePath, FileSystem fileSystem, boolean compression, int bufferSize) {
        this.basePath = basePath;
        this.fileSystem = fileSystem;
        this.format = new StateChangeFormat();
        this.compression = compression;
        this.bufferSize = bufferSize;
    }

    @Override
    public void upload(UploadTask uploadTask) throws IOException {
        upload(singletonList(uploadTask));
    }

    public void upload(Collection<UploadTask> tasks) throws IOException {
        final String fileName = generateFileName();
        LOG.debug("upload {} tasks to {}", tasks.size(), fileName);
        Path path = new Path(basePath, fileName);

        try {
            LocalResult result = upload(path, tasks);
            result.tasksOffsets.forEach(
                    (task, offsets) -> task.complete(buildResults(result.handle, offsets)));
        } catch (IOException e) {
            try (Closer closer = Closer.create()) {
                closer.register(
                        () -> {
                            throw e;
                        });
                tasks.forEach(cs -> closer.register(() -> cs.fail(e)));
                closer.register(() -> fileSystem.delete(path, true));
            }
        }
    }

    private LocalResult upload(Path path, Collection<UploadTask> tasks) throws IOException {
        try (FSDataOutputStream fsStream = fileSystem.create(path, NO_OVERWRITE)) {
            fsStream.write(compression ? 1 : 0);
            try (OutputStreamWithPos stream = wrap(fsStream); ) {
                final Map<UploadTask, Map<StateChangeSet, Long>> tasksOffsets = new HashMap<>();
                for (UploadTask task : tasks) {
                    tasksOffsets.put(task, format.write(stream, task.changeSets));
                }
                FileStateHandle handle = new FileStateHandle(path, stream.getPos());
                // WARN: streams have to be closed before returning the results
                // otherwise JM may receive invalid handles
                return new LocalResult(tasksOffsets, handle);
            }
        }
    }

    private static final class LocalResult {
        private final Map<UploadTask, Map<StateChangeSet, Long>> tasksOffsets;
        private final StreamStateHandle handle;

        public LocalResult(
                Map<UploadTask, Map<StateChangeSet, Long>> tasksOffsets, StreamStateHandle handle) {
            this.tasksOffsets = tasksOffsets;
            this.handle = handle;
        }
    }

    private OutputStreamWithPos wrap(FSDataOutputStream fsStream) throws IOException {
        StreamCompressionDecorator instance =
                compression
                        ? SnappyStreamCompressionDecorator.INSTANCE
                        : UncompressedStreamCompressionDecorator.INSTANCE;
        OutputStream compressed =
                compression ? instance.decorateWithCompression(fsStream) : fsStream;
        return new OutputStreamWithPos(new BufferedOutputStream(compressed, bufferSize));
    }

    private List<UploadResult> buildResults(
            StreamStateHandle handle, Map<StateChangeSet, Long> offsets) {
        return offsets.entrySet().stream()
                .map(e -> UploadResult.of(handle, e.getKey(), e.getValue()))
                .collect(toList());
    }

    private String generateFileName() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void close() {}
}
