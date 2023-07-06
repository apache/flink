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

package org.apache.flink.runtime.checkpoint.filemerging;

import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.LogicalFile.LogicalFileId;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.checkpoint.filemerging.PhysicalFile.PhysicalFileDeleter;

/** Base implementation of {@link FileMergingSnapshotManager}. */
public abstract class FileMergingSnapshotManagerBase implements FileMergingSnapshotManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileMergingSnapshotManager.class);

    /** The identifier of this manager. */
    private final String id;

    /** The executor for I/O operations in this manager. */
    protected final Executor ioExecutor;

    /** The {@link FileSystem} that this manager works on. */
    protected FileSystem fs;

    // checkpoint directories
    protected Path checkpointDir;
    protected Path sharedStateDir;
    protected Path taskOwnedStateDir;

    /**
     * The file system should only be initialized once.
     *
     * @see FileMergingSnapshotManager#initFileSystem for the reason why a throttle is needed.
     */
    private boolean fileSystemInitiated = false;

    /**
     * File-system dependent value. Mark whether the file system this manager running on need sync
     * for visibility. If true, DO a file sync after writing each segment .
     */
    protected boolean shouldSyncAfterClosingLogicalFile;

    protected PhysicalFileDeleter physicalFileDeleter = this::deletePhysicalFile;

    /**
     * Currently the shared state files are merged within each subtask, files are split by different
     * directories.
     */
    private final Map<SubtaskKey, Path> managedSharedStateDir = new ConcurrentHashMap<>();

    /**
     * The private state files are merged across subtasks, there is only one directory for
     * merged-files within one TM per job.
     */
    protected Path managedExclusiveStateDir;

    public FileMergingSnapshotManagerBase(String id, Executor ioExecutor) {
        this.id = id;
        this.ioExecutor = ioExecutor;
    }

    @Override
    public void initFileSystem(
            FileSystem fileSystem,
            Path checkpointBaseDir,
            Path sharedStateDir,
            Path taskOwnedStateDir)
            throws IllegalArgumentException {
        if (fileSystemInitiated) {
            Preconditions.checkArgument(
                    checkpointBaseDir.equals(this.checkpointDir),
                    "The checkpoint base dir is not deterministic across subtasks.");
            Preconditions.checkArgument(
                    sharedStateDir.equals(this.sharedStateDir),
                    "The shared checkpoint dir is not deterministic across subtasks.");
            Preconditions.checkArgument(
                    taskOwnedStateDir.equals(this.taskOwnedStateDir),
                    "The task-owned checkpoint dir is not deterministic across subtasks.");
            return;
        }
        this.fs = fileSystem;
        this.checkpointDir = Preconditions.checkNotNull(checkpointBaseDir);
        this.sharedStateDir = Preconditions.checkNotNull(sharedStateDir);
        this.taskOwnedStateDir = Preconditions.checkNotNull(taskOwnedStateDir);
        this.fileSystemInitiated = true;
        this.shouldSyncAfterClosingLogicalFile = shouldSyncAfterClosingLogicalFile(fileSystem);
        // Initialize the managed exclusive path using id as the child path name.
        // Currently, we use the task-owned directory to place the merged private state. According
        // to the FLIP-306, we later consider move these files to the new introduced
        // task-manager-owned directory.
        Path managedExclusivePath = new Path(taskOwnedStateDir, id);
        createManagedDirectory(managedExclusivePath);
        this.managedExclusiveStateDir = managedExclusivePath;
    }

    @Override
    public void registerSubtaskForSharedStates(SubtaskKey subtaskKey) {
        String managedDirName = subtaskKey.getManagedDirName();
        Path managedPath = new Path(sharedStateDir, managedDirName);
        if (!managedSharedStateDir.containsKey(subtaskKey)) {
            createManagedDirectory(managedPath);
            managedSharedStateDir.put(subtaskKey, managedPath);
        }
    }

    // ------------------------------------------------------------------------
    //  logical & physical file
    // ------------------------------------------------------------------------

    /**
     * Create a logical file on a physical file.
     *
     * @param physicalFile the underlying physical file.
     * @param startOffset the offset of the physical file that the logical file start from.
     * @param length the length of the logical file.
     * @param subtaskKey the id of the subtask that the logical file belongs to.
     * @return the created logical file.
     */
    protected LogicalFile createLogicalFile(
            @Nonnull PhysicalFile physicalFile,
            int startOffset,
            int length,
            @Nonnull SubtaskKey subtaskKey) {
        LogicalFileId fileID = LogicalFileId.generateRandomId();
        return new LogicalFile(fileID, physicalFile, startOffset, length, subtaskKey);
    }

    /**
     * Create a physical file in right location (managed directory), which is specified by scope of
     * this checkpoint and current subtask.
     *
     * @param subtaskKey the {@link SubtaskKey} of current subtask.
     * @param scope the scope of the checkpoint.
     * @return the created physical file.
     * @throws IOException if anything goes wrong with file system.
     */
    @Nonnull
    protected PhysicalFile createPhysicalFile(SubtaskKey subtaskKey, CheckpointedStateScope scope)
            throws IOException {
        PhysicalFile result;
        Exception latestException = null;

        Path dirPath = getManagedDir(subtaskKey, scope);

        if (dirPath == null) {
            throw new IOException(
                    "Could not get "
                            + scope
                            + " path for subtask "
                            + subtaskKey
                            + ", the directory may have not been created.");
        }

        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                OutputStreamAndPath streamAndPath =
                        EntropyInjector.createEntropyAware(
                                fs,
                                generatePhysicalFilePath(dirPath),
                                FileSystem.WriteMode.NO_OVERWRITE);
                FSDataOutputStream outputStream = streamAndPath.stream();
                Path filePath = streamAndPath.path();
                result = new PhysicalFile(outputStream, filePath, this.physicalFileDeleter, scope);
                updateFileCreationMetrics(filePath);
                return result;
            } catch (Exception e) {
                latestException = e;
            }
        }

        throw new IOException(
                "Could not open output stream for state file merging.", latestException);
    }

    private void updateFileCreationMetrics(Path path) {
        // TODO: FLINK-32091 add io metrics
        LOG.debug("Create a new physical file {} for checkpoint file merging.", path);
    }

    /**
     * Generate a file path for a physical file.
     *
     * @param dirPath the parent directory path for the physical file.
     * @return the generated file path for a physical file.
     */
    protected Path generatePhysicalFilePath(Path dirPath) {
        // this must be called after initFileSystem() is called
        // so the checkpoint directories must be not null if we reach here
        final String fileName = UUID.randomUUID().toString();
        return new Path(dirPath, fileName);
    }

    /**
     * Delete a physical file by given file path. Use the io executor to do the deletion.
     *
     * @param filePath the given file path to delete.
     */
    protected final void deletePhysicalFile(Path filePath) {
        ioExecutor.execute(
                () -> {
                    try {
                        fs.delete(filePath, false);
                        LOG.debug("Physical file deleted: {}.", filePath);
                    } catch (IOException e) {
                        LOG.warn("Fail to delete file: {}", filePath);
                    }
                });
    }

    // ------------------------------------------------------------------------
    //  abstract methods
    // ------------------------------------------------------------------------

    /**
     * Get a reused physical file or create one. This will be called in checkpoint output stream
     * creation logic.
     *
     * <p>TODO (FLINK-32073): Implement a CheckpointStreamFactory for file-merging that uses this
     * method to create or reuse physical files.
     *
     * <p>Basic logic of file reusing: whenever a physical file is needed, this method is called
     * with necessary information provided for acquiring a file. The file will not be reused until
     * it is written and returned to the reused pool by calling {@link
     * #returnPhysicalFileForNextReuse}.
     *
     * @param subtaskKey the subtask key for the caller
     * @param checkpointId the checkpoint id
     * @param scope checkpoint scope
     * @return the requested physical file.
     * @throws IOException thrown if anything goes wrong with file system.
     */
    @Nonnull
    protected abstract PhysicalFile getOrCreatePhysicalFileForCheckpoint(
            SubtaskKey subtaskKey, long checkpointId, CheckpointedStateScope scope)
            throws IOException;

    /**
     * Try to return an existing physical file to the manager for next reuse. If this physical file
     * is no longer needed (for reusing), it will be closed.
     *
     * <p>Basic logic of file reusing, see {@link #getOrCreatePhysicalFileForCheckpoint}.
     *
     * @param subtaskKey the subtask key for the caller
     * @param checkpointId in which checkpoint this physical file is requested.
     * @param physicalFile the returning checkpoint
     * @throws IOException thrown if anything goes wrong with file system.
     * @see #getOrCreatePhysicalFileForCheckpoint(SubtaskKey, long, CheckpointedStateScope)
     */
    protected abstract void returnPhysicalFileForNextReuse(
            SubtaskKey subtaskKey, long checkpointId, PhysicalFile physicalFile) throws IOException;

    // ------------------------------------------------------------------------
    //  file system
    // ------------------------------------------------------------------------

    @Override
    public Path getManagedDir(SubtaskKey subtaskKey, CheckpointedStateScope scope) {
        if (scope.equals(CheckpointedStateScope.SHARED)) {
            return managedSharedStateDir.get(subtaskKey);
        } else {
            return managedExclusiveStateDir;
        }
    }

    static boolean shouldSyncAfterClosingLogicalFile(FileSystem fileSystem) {
        // Currently, we do file sync regardless of the file system.
        // TODO: Determine whether do file sync more wisely. Add an interface to FileSystem if
        // needed.
        return true;
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private void createManagedDirectory(Path managedPath) {
        try {
            FileStatus fileStatus = null;
            try {
                fileStatus = fs.getFileStatus(managedPath);
            } catch (FileNotFoundException e) {
                // expected exception when the path not exist, and we ignore it.
            }
            if (fileStatus == null) {
                fs.mkdirs(managedPath);
                LOG.info("Created a directory {} for checkpoint file-merging.", managedPath);
            } else if (fileStatus.isDir()) {
                LOG.info("Reusing previous directory {} for checkpoint file-merging.", managedPath);
            } else {
                throw new FlinkRuntimeException(
                        "The managed path "
                                + managedPath
                                + " for file-merging is occupied by another file. Cannot create directory.");
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Cannot create directory " + managedPath + " for file-merging ", e);
        }
    }

    @Override
    public void close() throws IOException {}
}
