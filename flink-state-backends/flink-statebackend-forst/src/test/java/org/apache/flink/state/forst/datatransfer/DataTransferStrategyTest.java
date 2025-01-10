/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.datatransfer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.state.forst.fs.filemapping.FileOwnershipDecider;
import org.apache.flink.state.forst.fs.filemapping.HandleBackedMappingEntrySource;
import org.apache.flink.state.forst.fs.filemapping.MappingEntrySource;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link ReusableDataTransferStrategy}. */
@ExtendWith(ParameterizedTestExtension.class)
public class DataTransferStrategyTest {

    /** Container for checkpoint paths. */
    static class CheckpointPathsContainer {
        Path checkpointPathBase;
        File checkpointPrivateFolder;
        Path checkpointPrivateDirectory;
        File checkpointSharedFolder;
        Path checkpointSharedDirectory;

        CheckpointPathsContainer() throws IOException {
            checkpointPathBase = new Path(tempDir.toString(), "checkpoint");
            checkpointPrivateFolder = TempDirUtils.newFolder(tempDir, "private");
            checkpointPrivateDirectory = Path.fromLocalFile(checkpointPrivateFolder);
            checkpointSharedFolder = TempDirUtils.newFolder(tempDir, "shared");
            checkpointSharedDirectory = Path.fromLocalFile(checkpointSharedFolder);
        }
    }

    /** Container for DB files. */
    static class DBFilesContainer {
        static CheckpointPathsContainer cpPathContainer = null;

        FileSystem realFileSystem;

        Path dbCheckpointBase;

        protected CheckpointStreamFactory checkpointStreamFactory;

        protected CloseableRegistry closeableRegistry;
        protected CloseableRegistry tmpResourcesRegistry;

        ForStFlinkFileSystem dbDelegateFileSystem;

        Path dbLocalBase;
        Path dbRemoteBase;

        Map<String, Path> dbFilePaths = new HashMap<>();

        FileOwnershipDecider fileOwnershipDecider;

        DBFilesContainer(
                Path dbLocalBase, Path dbRemoteBase, FileOwnershipDecider fileOwnershipDecider)
                throws IOException {
            realFileSystem = LocalFileSystem.getLocalFileSystem();

            // prepare db paths
            this.dbDelegateFileSystem = ForStFlinkFileSystem.get(dbRemoteBase.toUri());
            this.dbLocalBase = dbLocalBase;
            this.dbRemoteBase = dbRemoteBase;

            // prepare checkpoint resources
            if (cpPathContainer == null) {
                cpPathContainer = new CheckpointPathsContainer();
            }
            dbCheckpointBase = cpPathContainer.checkpointPathBase;
            checkpointStreamFactory =
                    new FsCheckpointStreamFactory(
                            realFileSystem,
                            cpPathContainer.checkpointPrivateDirectory,
                            cpPathContainer.checkpointSharedDirectory,
                            1024,
                            4096);
            tmpResourcesRegistry = new CloseableRegistry();
            closeableRegistry = new CloseableRegistry();

            this.fileOwnershipDecider = fileOwnershipDecider;
        }

        private void createDbFiles(List<String> fileNames) throws IOException {
            for (String fileName : fileNames) {
                Path dir =
                        FileOwnershipDecider.shouldAlwaysBeLocal(new Path(fileName))
                                ? dbLocalBase
                                : dbRemoteBase;
                FSDataOutputStream output =
                        dbDelegateFileSystem.create(
                                new Path(dir, fileName), FileSystem.WriteMode.OVERWRITE);
                output.write(fileName.getBytes(StandardCharsets.UTF_8));
                output.sync();
                output.close();
                dbFilePaths.put(fileName, new Path(dir, fileName));
            }
        }

        private List<String> randomlySelectFile(double probability) {
            Preconditions.checkArgument(probability >= 0 && probability <= 1);

            List<String> selectedFiles = new ArrayList<>();
            for (String fileName : dbFilePaths.keySet()) {
                if (Math.random() < probability) {
                    selectedFiles.add(fileName);
                }
            }

            return selectedFiles;
        }

        private void removeFile(List<String> fileNames) throws IOException {
            for (String fileName : fileNames) {
                Path dbFilePath = dbFilePaths.remove(fileName);
                dbDelegateFileSystem.delete(dbFilePath, false);
            }
        }

        private void clear() throws IOException {
            List<String> dbFiles = new ArrayList<>(dbFilePaths.keySet());

            removeFile(dbFiles);
        }

        private void checkStateHandleFilesExist(List<HandleAndLocalPath> handles)
                throws IOException {
            for (HandleAndLocalPath handleAndLocalPath : handles) {
                StreamStateHandle handle = handleAndLocalPath.getHandle();
                if (handle instanceof FileStateHandle) {
                    Path filePath = ((FileStateHandle) handle).getFilePath();
                    assertThat(realFileSystem.exists(filePath)).isTrue();
                }
            }
        }

        private void checkDbFilesExist(List<String> fileNames) throws IOException {
            for (String fileName : fileNames) {
                Path dbFilePath = dbFilePaths.get(fileName);
                MappingEntrySource source =
                        dbDelegateFileSystem.getMappingEntry(dbFilePath).getSource();
                if (source instanceof HandleBackedMappingEntrySource
                        && !(((HandleBackedMappingEntrySource) source).getStateHandle()
                                instanceof FileStateHandle)) {
                    // source is backed by a non-file state handle, skip checking
                    continue;
                }
                Path sourceFileRealPath = source.getFilePath();
                assertThat(realFileSystem.exists(sourceFileRealPath)).isTrue();
            }
        }

        private void assertFilesReusedToCheckpoint(List<HandleAndLocalPath> checkpointHandles) {
            // assert that the DB files are reused to checkpoints
            for (HandleAndLocalPath handleAndLocalPath : checkpointHandles) {
                StreamStateHandle handle = handleAndLocalPath.getHandle();
                if (handle instanceof FileStateHandle) {
                    Path cpFilePath = ((FileStateHandle) handle).getFilePath();
                    String fileName = cpFilePath.getName();
                    if (!FileOwnershipDecider.shouldAlwaysBeLocal(new Path(fileName))) {
                        assertThat(dbFilePaths.containsKey(fileName)).isTrue();
                        assertThat(dbFilePaths.get(fileName)).isEqualTo(cpFilePath);
                    }
                }
            }
        }

        private DBFilesSnapshot snapshot(DataTransferStrategy strategy) throws IOException {
            DBFilesSnapshot snapshot = new DBFilesSnapshot();
            for (String fileName : dbFilePaths.keySet()) {
                Path dbFilePath = dbFilePaths.get(fileName);
                HandleAndLocalPath handleAndLocalPath =
                        strategy.transferToCheckpoint(
                                dbFilePath,
                                MAX_TRANSFER_BYTES,
                                checkpointStreamFactory,
                                CheckpointedStateScope.SHARED,
                                closeableRegistry,
                                tmpResourcesRegistry);
                snapshot.add(fileName, dbFilePath, handleAndLocalPath);
            }
            return snapshot;
        }

        private void restoreFromSnapshot(DataTransferStrategy strategy, DBFilesSnapshot snapshot)
                throws IOException {
            for (Tuple2<Path, HandleAndLocalPath> tuple : snapshot.dbSnapshotFiles.values()) {
                // get target path
                Path dbFilePreviousPath = tuple.f0;
                String fileName = dbFilePreviousPath.getName();
                Path dir =
                        FileOwnershipDecider.shouldAlwaysBeLocal(new Path(fileName))
                                ? dbLocalBase
                                : dbRemoteBase;
                Path dbFileNewPath = new Path(dir, fileName);

                // transfer data from checkpoint
                strategy.transferFromCheckpoint(
                        tuple.f1.getHandle(), dbFileNewPath, closeableRegistry);

                // add to db files
                dbFilePaths.put(fileName, dbFileNewPath);
            }
        }
    }

    /** Container for DB files of a snapshot. */
    static class DBFilesSnapshot {

        Map<String, Tuple2<Path, HandleAndLocalPath>> dbSnapshotFiles;

        DBFilesSnapshot() {
            dbSnapshotFiles = new HashMap<>();
        }

        void add(String fileName, Path dbFilePath, HandleAndLocalPath handleAndLocalPath) {
            dbSnapshotFiles.put(fileName, new Tuple2<>(dbFilePath, handleAndLocalPath));
        }

        List<HandleAndLocalPath> getStateHandles() {
            List<HandleAndLocalPath> handles = new ArrayList<>();
            dbSnapshotFiles
                    .values()
                    .forEach(
                            tuple -> {
                                handles.add(tuple.f1);
                            });
            return handles;
        }

        List<Path> getFilePaths() {
            List<Path> filePaths = new ArrayList<>();
            dbSnapshotFiles
                    .values()
                    .forEach(
                            tuple -> {
                                HandleAndLocalPath handleAndLocalPath = tuple.f1;
                                if (handleAndLocalPath.getHandle() instanceof FileStateHandle) {
                                    Path filePath =
                                            ((FileStateHandle) handleAndLocalPath.getHandle())
                                                    .getFilePath();
                                    filePaths.add(filePath);
                                }
                            });
            return filePaths;
        }

        List<String> getDbFiles() {
            return new ArrayList<>(dbSnapshotFiles.keySet());
        }

        void checkAllFilesExist() {
            getFilePaths()
                    .forEach(
                            path -> {
                                try {
                                    assertThat(path.getFileSystem().exists(path)).isTrue();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
        }
    }

    static class FileNameGenerator {
        int sstFileCount = 0;
        int miscFileCount = 0;

        FileNameGenerator() {}

        String genSSTFile() {
            return String.format("sst-%d.sst", sstFileCount++);
        }

        String genMiscFile() {
            return String.format("misc-%d", miscFileCount++);
        }

        List<String> genMultipleFileNames(int sstFileNumber, int miscFileNumber) {
            List<String> fileNames = new ArrayList<>();
            for (int i = 0; i < sstFileNumber; i++) {
                fileNames.add(genSSTFile());
            }
            for (int i = 0; i < miscFileNumber; i++) {
                fileNames.add(genMiscFile());
            }
            return fileNames;
        }
    }

    @Parameters(name = " recoveryClaimMode = {0}, dbDirUnderCpDir = {1}")
    public static List<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {RecoveryClaimMode.NO_CLAIM, false},
                    {RecoveryClaimMode.NO_CLAIM, true},
                    {RecoveryClaimMode.CLAIM, false},
                    {RecoveryClaimMode.CLAIM, true},
                });
    }

    @Parameter public RecoveryClaimMode recoveryClaimMode;

    @Parameter(1)
    public Boolean dbDirUnderCpDir;

    @TempDir static java.nio.file.Path tempDir;

    private static final long MAX_TRANSFER_BYTES = Long.MAX_VALUE;

    private DBFilesContainer createDb(
            JobID jobID,
            int subtaskIndex,
            int subtaskParallelism,
            boolean dbDirUnderCpDir,
            FileOwnershipDecider fileOwnershipDecider)
            throws IOException {
        String dbIdentifier = String.format("%s-db-%d-%d", jobID, subtaskIndex, subtaskParallelism);
        Path dbLocalBase = new Path(tempDir.toString(), String.format("local/%s", dbIdentifier));
        Path dbRemoteBase =
                new Path(
                        tempDir.toString(),
                        dbDirUnderCpDir
                                ? String.format("checkpoint/%s", dbIdentifier)
                                : String.format("remote/%s", dbIdentifier));
        DBFilesContainer db = new DBFilesContainer(dbLocalBase, dbRemoteBase, fileOwnershipDecider);
        db.clear();

        return db;
    }

    private DataTransferStrategy createDataTransferStrategy(
            DBFilesContainer db, RecoveryClaimMode claimMode) {
        return DataTransferStrategyBuilder.buildForSnapshot(
                db.dbDelegateFileSystem, dbDirUnderCpDir);
    }

    private Tuple2<DBFilesContainer, DataTransferStrategy> createOrRestoreDb(
            JobID jobID,
            int subtaskIndex,
            int subtaskParallelism,
            boolean dbDirUnderCpDir,
            RecoveryClaimMode claimMode)
            throws IOException {
        DBFilesContainer db =
                createDb(
                        jobID,
                        subtaskIndex,
                        subtaskParallelism,
                        dbDirUnderCpDir,
                        new FileOwnershipDecider(claimMode));
        DataTransferStrategy strategy = createDataTransferStrategy(db, claimMode);
        return new Tuple2<>(db, strategy);
    }

    @AfterEach
    void cleanTmpDir() throws IOException {
        // delete everything in temp dir
        FileUtils.cleanDirectory(tempDir.toFile());
    }

    @TestTemplate
    void simpleCaseTestRestore() throws IOException {
        JobID jobID = new JobID();
        Tuple2<DBFilesContainer, DataTransferStrategy> dbAndStrategy =
                createOrRestoreDb(jobID, 0, 1, dbDirUnderCpDir, recoveryClaimMode);
        DBFilesContainer db = dbAndStrategy.f0;
        DataTransferStrategy strategy = dbAndStrategy.f1;

        // create db files
        List<String> dbFiles = List.of(new String[] {"1.sst", "2.sst", "OPTIONS-000001", "LOG"});
        db.createDbFiles(dbFiles);
        db.checkDbFilesExist(dbFiles);

        // select some files to checkpoint
        List<String> cp1Files = List.of(new String[] {"1.sst", "OPTIONS-000001", "LOG"});
        DBFilesSnapshot snapshot1 = db.snapshot(strategy);
        db.assertFilesReusedToCheckpoint(snapshot1.getStateHandles());
        db.removeFile(cp1Files);
        db.checkStateHandleFilesExist(snapshot1.getStateHandles());

        // clear db
        db.clear();
        db.checkStateHandleFilesExist(snapshot1.getStateHandles());

        // restore from snapshot
        db.restoreFromSnapshot(strategy, snapshot1);
        List<String> restoredDbFiles = snapshot1.getDbFiles();
        db.checkDbFilesExist(restoredDbFiles);

        // clear db
        db.clear();
        db.checkStateHandleFilesExist(snapshot1.getStateHandles());
    }

    @TestTemplate
    public void testRestoreWithSameJobID() throws IOException {
        FileNameGenerator fileNameGenerator = new FileNameGenerator();
        JobID jobID = new JobID();
        Tuple2<DBFilesContainer, DataTransferStrategy> dbAndStrategy =
                createOrRestoreDb(jobID, 0, 1, dbDirUnderCpDir, recoveryClaimMode);
        DBFilesContainer db = dbAndStrategy.f0;
        DataTransferStrategy strategy = dbAndStrategy.f1;

        // run 'snapshot' and 'restore' multiple times
        DBFilesSnapshot lastSnapshot = null;
        for (int epoch = 0; epoch < 10; epoch++) {
            // create new files for DB
            List<String> newDbFiles = fileNameGenerator.genMultipleFileNames(4, 4);
            db.createDbFiles(newDbFiles);
            db.checkDbFilesExist(newDbFiles);

            // create a snapshot
            lastSnapshot = db.snapshot(strategy);
            db.assertFilesReusedToCheckpoint(lastSnapshot.getStateHandles());

            // remove files from DB should not affect the snapshot
            List<String> filesToRemove = db.randomlySelectFile(0.5);
            db.removeFile(filesToRemove);
            db.checkStateHandleFilesExist(lastSnapshot.getStateHandles());
            db.clear();

            // restore DB from snapshot
            dbAndStrategy = createOrRestoreDb(jobID, 0, 1, dbDirUnderCpDir, recoveryClaimMode);
            db = dbAndStrategy.f0;
            strategy = dbAndStrategy.f1;
            db.restoreFromSnapshot(strategy, lastSnapshot);
            List<String> restoredDbFiles = lastSnapshot.getDbFiles();
            lastSnapshot.checkAllFilesExist();
            db.checkDbFilesExist(restoredDbFiles);

            // remove files from DB should not affect the snapshot
            filesToRemove = db.randomlySelectFile(0.1);
            db.removeFile(filesToRemove);
            lastSnapshot.checkAllFilesExist();
        }

        db.clear();
        db.checkStateHandleFilesExist(lastSnapshot.getStateHandles());
    }
}
