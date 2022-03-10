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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

/**
 * A synchronous {@link StateChangeUploadScheduler} implementation that uploads the changes using
 * {@link FileSystem}.
 */
class StateChangeFsUploader implements StateChangeUploader {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeFsUploader.class);

    private final Path basePath;
    private final FileSystem fileSystem;
    private final StateChangeFormat format;
    private final boolean compression;
    private final int bufferSize;
    private final ChangelogStorageMetricGroup metrics;
    private final Clock clock;

    public StateChangeFsUploader(
            Path basePath,
            FileSystem fileSystem,
            boolean compression,
            int bufferSize,
            ChangelogStorageMetricGroup metrics) {
        this.basePath = basePath;
        this.fileSystem = fileSystem;
        this.format = new StateChangeFormat();
        this.compression = compression;
        this.bufferSize = bufferSize;
        this.metrics = metrics;
        this.clock = SystemClock.getInstance();
    }

    public UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException {
        final String fileName = generateFileName();
        LOG.debug("upload {} tasks to {}", tasks.size(), fileName);
        Path path = new Path(basePath, fileName);

        try {
            return uploadWithMetrics(path, tasks);
        } catch (IOException e) {
            metrics.getUploadFailuresCounter().inc();
            try (Closer closer = Closer.create()) {
                closer.register(
                        () -> {
                            throw e;
                        });
                tasks.forEach(cs -> closer.register(() -> cs.fail(e)));
                closer.register(() -> fileSystem.delete(path, true));
            }
        }
        return null; // closer above throws an exception
    }

    private UploadTasksResult uploadWithMetrics(Path path, Collection<UploadTask> tasks)
            throws IOException {
        metrics.getUploadsCounter().inc();
        long start = clock.relativeTimeNanos();
        UploadTasksResult result = upload(path, tasks);
        metrics.getUploadLatenciesNanos().update(clock.relativeTimeNanos() - start);
        metrics.getUploadSizes().update(result.getStateSize());
        return result;
    }

    private UploadTasksResult upload(Path path, Collection<UploadTask> tasks) throws IOException {
        boolean wrappedStreamClosed = false;
        FSDataOutputStream fsStream = fileSystem.create(path, NO_OVERWRITE);
        try {
            fsStream.write(compression ? 1 : 0);
            try (OutputStreamWithPos stream = wrap(fsStream)) {
                final Map<UploadTask, Map<StateChangeSet, Long>> tasksOffsets = new HashMap<>();
                for (UploadTask task : tasks) {
                    tasksOffsets.put(task, format.write(stream, task.changeSets));
                }
                FileStateHandle handle = new FileStateHandle(path, stream.getPos());
                // WARN: streams have to be closed before returning the results
                // otherwise JM may receive invalid handles
                return new UploadTasksResult(tasksOffsets, handle);
            } finally {
                wrappedStreamClosed = true;
            }
        } finally {
            if (!wrappedStreamClosed) {
                fsStream.close();
            }
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

    private String generateFileName() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void close() {}
}
