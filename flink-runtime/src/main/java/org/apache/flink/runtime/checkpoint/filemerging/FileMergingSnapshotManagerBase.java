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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.LogicalFile.LogicalFileId;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filemerging.DirectoryStreamStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.filesystem.FileMergingCheckpointStateOutputStream;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static org.apache.flink.runtime.checkpoint.filemerging.PhysicalFile.PhysicalFileDeleter;

/** Base implementation of {@link FileMergingSnapshotManager}. */
public abstract class FileMergingSnapshotManagerBase implements FileMergingSnapshotManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileMergingSnapshotManager.class);

    /** The identifier of this manager. */
    private final String id;

    /** The executor for I/O operations in this manager. */
    protected final Executor ioExecutor;

    /** Guard for uploadedStates. */
    protected final Object lock = new Object();

    @GuardedBy("lock")
    protected TreeMap<Long, Set<LogicalFile>> uploadedStates = new TreeMap<>();

    /** The map that holds all the known live logical files. */
    private final Map<LogicalFileId, LogicalFile> knownLogicalFiles = new ConcurrentHashMap<>();

    /** The {@link FileSystem} that this manager works on. */
    protected FileSystem fs;

    // checkpoint directories
    protected Path checkpointDir;
    protected Path sharedStateDir;
    protected Path taskOwnedStateDir;

    /** The buffer size for writing files to the file system. */
    protected int writeBufferSize;

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

    /** Max size for a physical file. */
    protected long maxPhysicalFileSize;

    /** Type of physical file pool. */
    protected PhysicalFilePool.Type filePoolType;

    protected PhysicalFileDeleter physicalFileDeleter = this::deletePhysicalFile;

    /**
     * Currently the shared state files are merged within each subtask, files are split by different
     * directories.
     */
    private final Map<SubtaskKey, Path> managedSharedStateDir = new ConcurrentHashMap<>();

    /**
     * The {@link DirectoryStreamStateHandle} for shared state directories, one for each subtask.
     */
    private final Map<SubtaskKey, DirectoryStreamStateHandle> managedSharedStateDirHandles =
            new ConcurrentHashMap<>();

    /**
     * The private state files are merged across subtasks, there is only one directory for
     * merged-files within one TM per job.
     */
    protected Path managedExclusiveStateDir;

    /**
     * The {@link DirectoryStreamStateHandle} for private state directory, one for each task
     * manager.
     */
    protected DirectoryStreamStateHandle managedExclusiveStateDirHandle;

    /** The current space statistic, updated on file creation/deletion. */
    protected SpaceStat spaceStat;

    public FileMergingSnapshotManagerBase(
            String id, long maxFileSize, PhysicalFilePool.Type filePoolType, Executor ioExecutor) {
        this.id = id;
        this.maxPhysicalFileSize = maxFileSize;
        this.filePoolType = filePoolType;
        this.ioExecutor = ioExecutor;
        this.spaceStat = new SpaceStat();
    }

    @Override
    public void initFileSystem(
            FileSystem fileSystem,
            Path checkpointBaseDir,
            Path sharedStateDir,
            Path taskOwnedStateDir,
            int writeBufferSize)
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
        this.managedExclusiveStateDirHandle =
                DirectoryStreamStateHandle.forPathWithZeroSize(
                        new File(managedExclusivePath.getPath()).toPath());
        this.writeBufferSize = writeBufferSize;
    }

    @Override
    public void registerSubtaskForSharedStates(SubtaskKey subtaskKey) {
        String managedDirName = subtaskKey.getManagedDirName();
        Path managedPath = new Path(sharedStateDir, managedDirName);
        if (!managedSharedStateDir.containsKey(subtaskKey)) {
            createManagedDirectory(managedPath);
            managedSharedStateDir.put(subtaskKey, managedPath);
            managedSharedStateDirHandles.put(
                    subtaskKey,
                    DirectoryStreamStateHandle.forPathWithZeroSize(
                            new File(managedPath.getPath()).toPath()));
        }
    }

    // ------------------------------------------------------------------------
    //  logical & physical file
    // ------------------------------------------------------------------------

    /**
     * Create a logical file on a physical file.
     *
     * @param physicalFile the underlying physical file.
     * @param startOffset the offset in the physical file that the logical file starts from.
     * @param length the length of the logical file.
     * @param subtaskKey the id of the subtask that the logical file belongs to.
     * @return the created logical file.
     */
    protected LogicalFile createLogicalFile(
            @Nonnull PhysicalFile physicalFile,
            long startOffset,
            long length,
            @Nonnull SubtaskKey subtaskKey) {
        LogicalFileId fileID = LogicalFileId.generateRandomId();
        LogicalFile file = new LogicalFile(fileID, physicalFile, startOffset, length, subtaskKey);
        knownLogicalFiles.put(fileID, file);
        if (physicalFile.isOwned()) {
            spaceStat.onLogicalFileCreate(length);
        }
        return file;
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

    @Override
    public FileMergingCheckpointStateOutputStream createCheckpointStateOutputStream(
            SubtaskKey subtaskKey, long checkpointId, CheckpointedStateScope scope) {

        return new FileMergingCheckpointStateOutputStream(
                writeBufferSize,
                new FileMergingCheckpointStateOutputStream.FileMergingSnapshotManagerProxy() {
                    PhysicalFile physicalFile;
                    LogicalFile logicalFile;

                    @Override
                    public Tuple2<FSDataOutputStream, Path> providePhysicalFile()
                            throws IOException {
                        physicalFile =
                                getOrCreatePhysicalFileForCheckpoint(
                                        subtaskKey, checkpointId, scope);
                        return new Tuple2<>(
                                physicalFile.getOutputStream(), physicalFile.getFilePath());
                    }

                    @Override
                    public SegmentFileStateHandle closeStreamAndCreateStateHandle(
                            Path filePath, long startPos, long stateSize) throws IOException {
                        if (physicalFile == null) {
                            return null;
                        } else {
                            // deal with logical file
                            logicalFile =
                                    createLogicalFile(
                                            physicalFile, startPos, stateSize, subtaskKey);
                            logicalFile.advanceLastCheckpointId(checkpointId);

                            // track the logical file
                            synchronized (lock) {
                                uploadedStates
                                        .computeIfAbsent(checkpointId, key -> new HashSet<>())
                                        .add(logicalFile);
                            }

                            // deal with physicalFile file
                            returnPhysicalFileForNextReuse(subtaskKey, checkpointId, physicalFile);

                            return new SegmentFileStateHandle(
                                    physicalFile.getFilePath(),
                                    startPos,
                                    stateSize,
                                    scope,
                                    logicalFile.getFileId());
                        }
                    }

                    @Override
                    public void closeStreamExceptionally() throws IOException {
                        if (physicalFile != null) {
                            if (logicalFile != null) {
                                discardSingleLogicalFile(logicalFile, checkpointId);
                            } else {
                                // The physical file should be closed anyway. This is because the
                                // last segmented write on this file is likely to have failed, and
                                // we want to prevent further reusing of this file.
                                physicalFile.close();
                                physicalFile.deleteIfNecessary();
                            }
                        }
                    }
                });
    }

    private void updateFileCreationMetrics(Path path) {
        // TODO: FLINK-32091 add io metrics
        spaceStat.onPhysicalFileCreate();
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

    @VisibleForTesting
    boolean isResponsibleForFile(Path filePath) {
        Path parent = filePath.getParent();
        return parent.equals(managedExclusiveStateDir)
                || managedSharedStateDir.containsValue(parent);
    }

    /**
     * Delete a physical file by given file path. Use the io executor to do the deletion.
     *
     * @param filePath the given file path to delete.
     */
    protected final void deletePhysicalFile(Path filePath, long size) {
        ioExecutor.execute(
                () -> {
                    try {
                        fs.delete(filePath, false);
                        spaceStat.onPhysicalFileDelete(size);
                        LOG.debug("Physical file deleted: {}.", filePath);
                    } catch (IOException e) {
                        LOG.warn("Fail to delete file: {}", filePath);
                    }
                });
    }

    /**
     * Create physical pool by filePoolType.
     *
     * @return physical file pool.
     */
    protected final PhysicalFilePool createPhysicalPool() {
        switch (filePoolType) {
            case NON_BLOCKING:
                return new NonBlockingPhysicalFilePool(
                        maxPhysicalFileSize, this::createPhysicalFile);
            case BLOCKING:
                return new BlockingPhysicalFilePool(maxPhysicalFileSize, this::createPhysicalFile);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type of physical file pool: " + filePoolType);
        }
    }

    // ------------------------------------------------------------------------
    //  abstract methods
    // ------------------------------------------------------------------------

    /**
     * Get a reused physical file or create one. This will be called in checkpoint output stream
     * creation logic.
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

    /**
     * The callback which will be triggered when all subtasks discarded (aborted or subsumed).
     *
     * @param checkpointId the discarded checkpoint id.
     * @throws IOException if anything goes wrong with file system.
     */
    protected abstract void discardCheckpoint(long checkpointId) throws IOException;

    // ------------------------------------------------------------------------
    //  Checkpoint Listener
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(SubtaskKey subtaskKey, long checkpointId)
            throws Exception {
        // does nothing
    }

    @Override
    public void notifyCheckpointAborted(SubtaskKey subtaskKey, long checkpointId) throws Exception {
        synchronized (lock) {
            Set<LogicalFile> logicalFilesForCurrentCp = uploadedStates.get(checkpointId);
            if (logicalFilesForCurrentCp == null) {
                return;
            }
            if (discardLogicalFiles(subtaskKey, checkpointId, logicalFilesForCurrentCp)) {
                uploadedStates.remove(checkpointId);
            }
        }
    }

    @Override
    public void notifyCheckpointSubsumed(SubtaskKey subtaskKey, long checkpointId)
            throws Exception {
        synchronized (lock) {
            Iterator<Map.Entry<Long, Set<LogicalFile>>> uploadedStatesIterator =
                    uploadedStates.headMap(checkpointId, true).entrySet().iterator();
            while (uploadedStatesIterator.hasNext()) {
                Map.Entry<Long, Set<LogicalFile>> entry = uploadedStatesIterator.next();
                if (discardLogicalFiles(subtaskKey, checkpointId, entry.getValue())) {
                    uploadedStatesIterator.remove();
                }
            }
        }
    }

    @Override
    public void reusePreviousStateHandle(
            long checkpointId, Collection<? extends StreamStateHandle> stateHandles) {
        for (StreamStateHandle stateHandle : stateHandles) {
            if (stateHandle instanceof SegmentFileStateHandle) {
                LogicalFile file =
                        knownLogicalFiles.get(
                                ((SegmentFileStateHandle) stateHandle).getLogicalFileId());
                if (file != null) {
                    file.advanceLastCheckpointId(checkpointId);
                }
            }
        }
    }

    public void discardSingleLogicalFile(LogicalFile logicalFile, long checkpointId)
            throws IOException {
        logicalFile.discardWithCheckpointId(checkpointId);
        if (logicalFile.getPhysicalFile().isOwned()) {
            spaceStat.onLogicalFileDelete(logicalFile.getLength());
        }
    }

    private boolean discardLogicalFiles(
            SubtaskKey subtaskKey, long checkpointId, Set<LogicalFile> logicalFiles)
            throws Exception {
        Iterator<LogicalFile> logicalFileIterator = logicalFiles.iterator();
        while (logicalFileIterator.hasNext()) {
            LogicalFile logicalFile = logicalFileIterator.next();
            if (logicalFile.getSubtaskKey().equals(subtaskKey)
                    && logicalFile.getLastUsedCheckpointID() <= checkpointId) {
                discardSingleLogicalFile(logicalFile, checkpointId);
                logicalFileIterator.remove();
                knownLogicalFiles.remove(logicalFile.getFileId());
            }
        }

        if (logicalFiles.isEmpty()) {
            discardCheckpoint(checkpointId);
            return true;
        }
        return false;
    }

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

    @Override
    public DirectoryStreamStateHandle getManagedDirStateHandle(
            SubtaskKey subtaskKey, CheckpointedStateScope scope) {
        if (scope.equals(CheckpointedStateScope.SHARED)) {
            return managedSharedStateDirHandles.get(subtaskKey);
        } else {
            return managedExclusiveStateDirHandle;
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

    // ------------------------------------------------------------------------
    //  restore
    // ------------------------------------------------------------------------

    @Override
    public void restoreStateHandles(
            long checkpointId, SubtaskKey subtaskKey, Stream<SegmentFileStateHandle> stateHandles) {

        synchronized (lock) {
            Set<LogicalFile> restoredLogicalFiles =
                    uploadedStates.computeIfAbsent(checkpointId, id -> new HashSet<>());

            Map<Path, PhysicalFile> knownPhysicalFiles = new HashMap<>();
            knownLogicalFiles.values().stream()
                    .map(LogicalFile::getPhysicalFile)
                    .forEach(file -> knownPhysicalFiles.putIfAbsent(file.getFilePath(), file));

            stateHandles.forEach(
                    fileHandle -> {
                        PhysicalFile physicalFile =
                                knownPhysicalFiles.computeIfAbsent(
                                        fileHandle.getFilePath(),
                                        path -> {
                                            boolean managedByFileMergingManager =
                                                    isManagedByFileMergingManager(
                                                            path,
                                                            subtaskKey,
                                                            fileHandle.getScope());
                                            if (managedByFileMergingManager) {
                                                spaceStat.onPhysicalFileCreate();
                                            }
                                            return new PhysicalFile(
                                                    null,
                                                    path,
                                                    physicalFileDeleter,
                                                    fileHandle.getScope(),
                                                    managedByFileMergingManager);
                                        });

                        LogicalFileId logicalFileId = fileHandle.getLogicalFileId();
                        LogicalFile logicalFile =
                                new LogicalFile(
                                        logicalFileId,
                                        physicalFile,
                                        fileHandle.getStartPos(),
                                        fileHandle.getStateSize(),
                                        subtaskKey);

                        if (physicalFile.isOwned()) {
                            spaceStat.onLogicalFileCreate(logicalFile.getLength());
                        }
                        knownLogicalFiles.put(logicalFileId, logicalFile);
                        logicalFile.advanceLastCheckpointId(checkpointId);
                        restoredLogicalFiles.add(logicalFile);
                    });
        }
    }

    /**
     * Distinguish whether the given filePath is managed by the FileMergingSnapshotManager. If the
     * filePath is located under managedDir (managedSharedStateDir or managedExclusiveStateDir) as a
     * subFile, it should be managed by the FileMergingSnapshotManager.
     */
    private boolean isManagedByFileMergingManager(
            Path filePath, SubtaskKey subtaskKey, CheckpointedStateScope scope) {
        if (scope == CheckpointedStateScope.SHARED) {
            Path managedDir = managedSharedStateDir.get(subtaskKey);
            return filePath.toString().startsWith(managedDir.toString());
        }
        if (scope == CheckpointedStateScope.EXCLUSIVE) {
            return filePath.toString().startsWith(managedExclusiveStateDir.toString());
        }
        throw new UnsupportedOperationException("Unsupported CheckpointStateScope " + scope);
    }

    @VisibleForTesting
    public LogicalFile getLogicalFile(LogicalFileId fileId) {
        return knownLogicalFiles.get(fileId);
    }

    @VisibleForTesting
    TreeMap<Long, Set<LogicalFile>> getUploadedStates() {
        return uploadedStates;
    }
}
