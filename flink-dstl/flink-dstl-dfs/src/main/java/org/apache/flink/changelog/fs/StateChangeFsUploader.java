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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * A synchronous {@link StateChangeUploadScheduler} implementation that uploads the changes using
 * {@link FileSystem}.
 */
public class StateChangeFsUploader extends AbstractStateChangeFsUploader {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeFsUploader.class);

    @VisibleForTesting public static final String PATH_SUB_DIR = "dstl";

    private final Path basePath;
    private final FileSystem fileSystem;

    @VisibleForTesting
    public StateChangeFsUploader(
            JobID jobID,
            Path basePath,
            FileSystem fileSystem,
            boolean compression,
            int bufferSize,
            ChangelogStorageMetricGroup metrics,
            TaskChangelogRegistry changelogRegistry) {
        this(
                jobID,
                basePath,
                fileSystem,
                compression,
                bufferSize,
                metrics,
                changelogRegistry,
                FileStateHandle::new);
    }

    public StateChangeFsUploader(
            JobID jobID,
            Path basePath,
            FileSystem fileSystem,
            boolean compression,
            int bufferSize,
            ChangelogStorageMetricGroup metrics,
            TaskChangelogRegistry changelogRegistry,
            BiFunction<Path, Long, StreamStateHandle> handleFactory) {
        super(compression, bufferSize, metrics, changelogRegistry, handleFactory);
        this.basePath =
                new Path(basePath, String.format("%s/%s", jobID.toHexString(), PATH_SUB_DIR));
        this.fileSystem = fileSystem;
    }

    @VisibleForTesting
    public Path getBasePath() {
        return this.basePath;
    }

    @Override
    public OutputStreamWithPos prepareStream() throws IOException {
        final String fileName = generateFileName();
        LOG.debug("upload tasks to {}", fileName);
        Path path = new Path(basePath, fileName);
        OutputStreamWithPos outputStream =
                new OutputStreamWithPos(fileSystem.create(path, WriteMode.NO_OVERWRITE), path);
        outputStream.wrap(this.compression, this.bufferSize);
        return outputStream;
    }

    @Override
    public void close() {}
}
