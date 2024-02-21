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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.ChangelogTaskLocalStateStore;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.changelog.LocalChangelogRegistry;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.changelog.fs.StateChangeFsUploader.PATH_SUB_DIR;
import static org.apache.flink.runtime.state.ChangelogTaskLocalStateStore.getLocalTaskOwnedDirectory;

/**
 * A StateChangeFsUploader implementation that writes the changes to remote and local.
 * <li>Local dstl files are only managed by TM side, {@link LocalChangelogRegistry}, {@link
 *     TaskChangelogRegistry} and {@link ChangelogTaskLocalStateStore} are responsible for managing
 *     them.
 * <li>Remote dstl files are managed by TM side and JM side, {@link TaskChangelogRegistry} is
 *     responsible for TM side, and {@link SharedStateRegistry} is responsible for JM side.
 *
 *     <p>The total discard logic of local dstl files is:
 *
 *     <ol>
 *       <li>Register files to {@link TaskChangelogRegistry#startTracking} on {@link #upload}.
 *       <li>Store the meta of files into {@link ChangelogTaskLocalStateStore} by
 *           AsyncCheckpointRunnable#reportCompletedSnapshotStates().
 *       <li>Pass control of the file to {@link LocalChangelogRegistry#register} when
 *           FsStateChangelogWriter#persist , files of the previous checkpoint will be deleted by
 *           {@link LocalChangelogRegistry#discardUpToCheckpoint} when the checkpoint is confirmed.
 *       <li>When ChangelogTruncateHelper#materialized() or
 *           ChangelogTruncateHelper#checkpointSubsumed() is called, {@link
 *           TaskChangelogRegistry#release} is responsible for deleting local files.
 *       <li>When one checkpoint is aborted, all accumulated local dstl files will be deleted at
 *           once.
 *     </ol>
 */
public class DuplicatingStateChangeFsUploader extends AbstractStateChangeFsUploader {

    private static final Logger LOG =
            LoggerFactory.getLogger(DuplicatingStateChangeFsUploader.class);

    private final Path basePath;
    private final FileSystem fileSystem;
    private final LocalRecoveryDirectoryProvider localRecoveryDirectoryProvider;
    private final JobID jobID;

    public DuplicatingStateChangeFsUploader(
            JobID jobID,
            Path basePath,
            FileSystem fileSystem,
            boolean compression,
            int bufferSize,
            ChangelogStorageMetricGroup metrics,
            TaskChangelogRegistry changelogRegistry,
            LocalRecoveryDirectoryProvider localRecoveryDirectoryProvider) {
        super(compression, bufferSize, metrics, changelogRegistry, FileStateHandle::new);
        this.basePath =
                new Path(basePath, String.format("%s/%s", jobID.toHexString(), PATH_SUB_DIR));
        this.fileSystem = fileSystem;
        this.localRecoveryDirectoryProvider = localRecoveryDirectoryProvider;
        this.jobID = jobID;
    }

    @Override
    public OutputStreamWithPos prepareStream() throws IOException {
        final String fileName = generateFileName();
        LOG.debug("upload tasks to {}", fileName);
        Path path = new Path(basePath, fileName);
        FSDataOutputStream primaryStream = fileSystem.create(path, WriteMode.NO_OVERWRITE);
        Path localPath =
                new Path(
                        getLocalTaskOwnedDirectory(localRecoveryDirectoryProvider, jobID),
                        fileName);
        FSDataOutputStream secondaryStream =
                localPath.getFileSystem().create(localPath, WriteMode.NO_OVERWRITE);
        DuplicatingOutputStreamWithPos outputStream =
                new DuplicatingOutputStreamWithPos(primaryStream, path, secondaryStream, localPath);
        outputStream.wrap(this.compression, this.bufferSize);
        return outputStream;
    }

    @Override
    public void close() throws Exception {}
}
