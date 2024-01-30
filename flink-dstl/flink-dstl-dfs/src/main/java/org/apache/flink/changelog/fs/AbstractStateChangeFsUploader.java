/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.apache.flink.shaded.guava31.com.google.common.io.Closer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

/** Base implementation of StateChangeUploader. */
public abstract class AbstractStateChangeFsUploader implements StateChangeUploader {

    private final StateChangeFormat format;
    private final Clock clock;
    private final TaskChangelogRegistry changelogRegistry;
    private final BiFunction<Path, Long, StreamStateHandle> handleFactory;
    protected final ChangelogStorageMetricGroup metrics;
    protected final boolean compression;
    protected final int bufferSize;

    public AbstractStateChangeFsUploader(
            boolean compression,
            int bufferSize,
            ChangelogStorageMetricGroup metrics,
            TaskChangelogRegistry changelogRegistry,
            BiFunction<Path, Long, StreamStateHandle> handleFactory) {
        this.format = new StateChangeFormat();
        this.compression = compression;
        this.bufferSize = bufferSize;
        this.metrics = metrics;
        this.clock = SystemClock.getInstance();
        this.changelogRegistry = changelogRegistry;
        this.handleFactory = handleFactory;
    }

    abstract OutputStreamWithPos prepareStream() throws IOException;

    @Override
    public UploadTasksResult upload(Collection<UploadTask> tasks) throws IOException {
        metrics.getUploadsCounter().inc();
        long start = clock.relativeTimeNanos();
        UploadTasksResult result = uploadInternal(tasks);
        metrics.getUploadLatenciesNanos().update(clock.relativeTimeNanos() - start);
        metrics.getUploadSizes().update(result.getStateSize());
        return result;
    }

    private UploadTasksResult uploadInternal(Collection<UploadTask> tasks) throws IOException {
        try (OutputStreamWithPos stream = prepareStream()) {
            final Map<UploadTask, Map<StateChangeSet, Tuple2<Long, Long>>> tasksOffsets =
                    new HashMap<>();
            for (UploadTask task : tasks) {
                tasksOffsets.put(task, format.write(stream, task.changeSets));
            }

            long numOfChangeSets = tasks.stream().flatMap(t -> t.getChangeSets().stream()).count();

            StreamStateHandle handle = stream.getHandle(handleFactory);
            changelogRegistry.startTracking(handle, numOfChangeSets);

            if (stream instanceof DuplicatingOutputStreamWithPos) {
                StreamStateHandle localHandle =
                        ((DuplicatingOutputStreamWithPos) stream).getSecondaryHandle(handleFactory);
                changelogRegistry.startTracking(localHandle, numOfChangeSets);
                return new UploadTasksResult(tasksOffsets, handle, localHandle);
            }
            // WARN: streams have to be closed before returning the results
            // otherwise JM may receive invalid handles
            return new UploadTasksResult(tasksOffsets, handle);
        } catch (IOException e) {
            metrics.getUploadFailuresCounter().inc();
            try (Closer closer = Closer.create()) {
                closer.register(
                        () -> {
                            throw e;
                        });
                tasks.forEach(cs -> closer.register(() -> cs.fail(e)));
            }
        }
        return null; // closer above throws an exception
    }

    protected String generateFileName() {
        return UUID.randomUUID().toString();
    }
}
