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

import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.state.forst.StateHandleTransferSpec;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test class for {@link ForStStateDataTransfer}. */
class ForStStateDataTransferTest extends TestLogger {

    @TempDir private java.nio.file.Path temporaryFolder;

    /** Test that the exception arose in the thread pool will rethrow to the main thread. */
    @Test
    void testMultiThreadTransferThreadPoolExceptionRethrow() throws IOException {
        SpecifiedException expectedException =
                new SpecifiedException("throw exception while multi thread transfer states.");

        CheckpointStateOutputStream outputStream =
                createFailingCheckpointStateOutputStream(expectedException);
        CheckpointStreamFactory checkpointStreamFactory =
                new CheckpointStreamFactory() {
                    @Override
                    public CheckpointStateOutputStream createCheckpointStateOutputStream(
                            CheckpointedStateScope scope) {
                        return outputStream;
                    }

                    @Override
                    public boolean canFastDuplicate(
                            StreamStateHandle stateHandle, CheckpointedStateScope scope) {
                        return false;
                    }

                    @Override
                    public List<StreamStateHandle> duplicate(
                            List<StreamStateHandle> stateHandles, CheckpointedStateScope scope) {
                        return null;
                    }
                };

        File file = TempDirUtils.newFile(temporaryFolder, String.valueOf(UUID.randomUUID()));
        generateRandomFileContent(file.getPath(), 20);

        List<Path> filePaths = new ArrayList<>(1);
        filePaths.add(Path.fromLocalFile(file));
        try (ForStStateDataTransfer stateTransfer = new ForStStateDataTransfer(5)) {
            assertThatThrownBy(
                            () ->
                                    stateTransfer.transferFilesToCheckpointFs(
                                            filePaths,
                                            checkpointStreamFactory,
                                            CheckpointedStateScope.SHARED,
                                            new CloseableRegistry(),
                                            new CloseableRegistry()))
                    .isEqualTo(expectedException);
        }
    }

    @Test
    void testTransferredSstCanBeCleanedUp() throws Exception {
        SpecifiedException expectedException =
                new SpecifiedException("throw exception while multi thread transfer states.");

        File checkpointPrivateFolder = TempDirUtils.newFolder(temporaryFolder, "private");
        Path checkpointPrivateDirectory = Path.fromLocalFile(checkpointPrivateFolder);

        File checkpointSharedFolder = TempDirUtils.newFolder(temporaryFolder, "shared");
        Path checkpointSharedDirectory = Path.fromLocalFile(checkpointSharedFolder);

        FileSystem fileSystem = checkpointPrivateDirectory.getFileSystem();

        int sstFileCount = 6;
        int fileStateSizeThreshold = 1024;
        int writeBufferSize = 4096;
        CheckpointStreamFactory checkpointStreamFactory =
                new FsCheckpointStreamFactory(
                        fileSystem,
                        checkpointPrivateDirectory,
                        checkpointSharedDirectory,
                        fileStateSizeThreshold,
                        writeBufferSize);

        String localFolder = "local";
        TempDirUtils.newFolder(temporaryFolder, localFolder);

        List<Path> filePaths =
                generateRandomSstFiles(localFolder, sstFileCount, fileStateSizeThreshold);
        CloseableRegistry tmpResourcesRegistry = new CloseableRegistry();
        try (ForStStateDataTransfer stateTransfer = new ForStStateDataTransfer(1)) {
            stateTransfer.transferFilesToCheckpointFs(
                    filePaths,
                    checkpointStreamFactory,
                    CheckpointedStateScope.SHARED,
                    new CloseableRegistry(),
                    tmpResourcesRegistry);

            assertThatThrownBy(
                            () ->
                                    stateTransfer.transferFilesToCheckpointFs(
                                            filePaths,
                                            new LastFailingCheckpointStateOutputStreamFactory(
                                                    checkpointStreamFactory,
                                                    sstFileCount,
                                                    expectedException),
                                            CheckpointedStateScope.SHARED,
                                            new CloseableRegistry(),
                                            tmpResourcesRegistry))
                    .isEqualTo(expectedException);
            assertThat(checkpointPrivateFolder.list()).isEmpty();
            assertThat(checkpointSharedFolder.list()).isNotEmpty();

            tmpResourcesRegistry.close();
            // Check whether the temporary file before the exception can be cleaned up
            assertThat(checkpointPrivateFolder.list()).isEmpty();
            assertThat(checkpointSharedFolder.list()).isEmpty();

            Path first = filePaths.stream().findFirst().get();
            assertThatThrownBy(
                            () ->
                                    stateTransfer.transferFilesToCheckpointFs(
                                            Collections.singletonList(first),
                                            checkpointStreamFactory,
                                            CheckpointedStateScope.SHARED,
                                            new CloseableRegistry(),
                                            tmpResourcesRegistry))
                    .as("Cannot register Closeable, registry is already closed. Closing argument.")
                    .isInstanceOf(IOException.class);

            // Check whether the temporary file after the exception can be cleaned up.
            assertThat(checkpointPrivateFolder.list()).isEmpty();
            assertThat(checkpointSharedFolder.list()).isEmpty();
        }
    }

    /** Test that transfer file head part correctly. */
    @Test
    void testTransferHeadPartCorrectly() throws Exception {
        File checkpointPrivateFolder = TempDirUtils.newFolder(temporaryFolder, "private");
        Path checkpointPrivateDirectory = Path.fromLocalFile(checkpointPrivateFolder);

        File checkpointSharedFolder = TempDirUtils.newFolder(temporaryFolder, "shared");
        Path checkpointSharedDirectory = Path.fromLocalFile(checkpointSharedFolder);

        FileSystem fileSystem = checkpointPrivateDirectory.getFileSystem();
        int fileStateSizeThreshold = 1024;
        int headBytes = 512; // make sure just a part of origin state file
        int writeBufferSize = 4096;
        FsCheckpointStreamFactory checkpointStreamFactory =
                new FsCheckpointStreamFactory(
                        fileSystem,
                        checkpointPrivateDirectory,
                        checkpointSharedDirectory,
                        fileStateSizeThreshold,
                        writeBufferSize);

        String localFolder = "local";
        TempDirUtils.newFolder(temporaryFolder, localFolder);

        Path sstFile = generateRandomSstFile(localFolder, 1, fileStateSizeThreshold);

        try (ForStStateDataTransfer stateTransfer = new ForStStateDataTransfer(5)) {
            HandleAndLocalPath handleAndLocalPath =
                    stateTransfer.transferFileToCheckpointFs(
                            sstFile,
                            headBytes,
                            checkpointStreamFactory,
                            CheckpointedStateScope.SHARED,
                            new CloseableRegistry(),
                            new CloseableRegistry());

            assertStateContentEqual(
                    sstFile, headBytes, handleAndLocalPath.getHandle().openInputStream());
        }
    }

    /** Test that transfer files with multi-thread correctly. */
    @Test
    void testMultiThreadTransferCorrectly() throws Exception {
        File checkpointPrivateFolder = TempDirUtils.newFolder(temporaryFolder, "private");
        Path checkpointPrivateDirectory = Path.fromLocalFile(checkpointPrivateFolder);

        File checkpointSharedFolder = TempDirUtils.newFolder(temporaryFolder, "shared");
        Path checkpointSharedDirectory = Path.fromLocalFile(checkpointSharedFolder);

        FileSystem fileSystem = checkpointPrivateDirectory.getFileSystem();
        int fileStateSizeThreshold = 1024;
        int writeBufferSize = 4096;
        FsCheckpointStreamFactory checkpointStreamFactory =
                new FsCheckpointStreamFactory(
                        fileSystem,
                        checkpointPrivateDirectory,
                        checkpointSharedDirectory,
                        fileStateSizeThreshold,
                        writeBufferSize);

        String localFolder = "local";
        TempDirUtils.newFolder(temporaryFolder, localFolder);

        int sstFileCount = 6;
        List<Path> sstFilePaths =
                generateRandomSstFiles(localFolder, sstFileCount, fileStateSizeThreshold);

        try (ForStStateDataTransfer stateTransfer = new ForStStateDataTransfer(5)) {
            List<HandleAndLocalPath> sstFiles =
                    stateTransfer.transferFilesToCheckpointFs(
                            sstFilePaths,
                            checkpointStreamFactory,
                            CheckpointedStateScope.SHARED,
                            new CloseableRegistry(),
                            new CloseableRegistry());

            for (Path path : sstFilePaths) {
                assertStateContentEqual(
                        path,
                        -1,
                        sstFiles.stream()
                                .filter(e -> e.getLocalPath().equals(path.getName()))
                                .findFirst()
                                .get()
                                .getHandle()
                                .openInputStream());
            }
        }
    }

    private static CheckpointStateOutputStream createFailingCheckpointStateOutputStream(
            IOException failureException) {
        return new CheckpointStateOutputStream() {
            @Nullable
            @Override
            public StreamStateHandle closeAndGetHandle() {
                return new ByteStreamStateHandle("testHandle", "testHandle".getBytes());
            }

            @Override
            public void close() {}

            @Override
            public long getPos() {
                return 0;
            }

            @Override
            public void flush() {}

            @Override
            public void sync() {}

            @Override
            public void write(int b) throws IOException {
                throw failureException;
            }
        };
    }

    private List<Path> generateRandomSstFiles(
            String localFolder, int fileCount, int fileStateSizeThreshold) throws IOException {
        List<Path> sstFilePaths = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; ++i) {
            sstFilePaths.add(generateRandomSstFile(localFolder, i, fileStateSizeThreshold));
        }
        return sstFilePaths;
    }

    private Path generateRandomSstFile(String localFolder, int fileNum, int fileStateSizeThreshold)
            throws IOException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        File file =
                TempDirUtils.newFile(
                        temporaryFolder, String.format("%s/%d.sst", localFolder, fileNum));
        generateRandomFileContent(
                file.getPath(), random.nextInt(1_000_000) + fileStateSizeThreshold);

        return Path.fromLocalFile(file);
    }

    private void generateRandomFileContent(String filePath, int fileLength) throws IOException {
        FileOutputStream fileStream = new FileOutputStream(filePath);
        byte[] contents = new byte[fileLength];
        ThreadLocalRandom.current().nextBytes(contents);
        fileStream.write(contents);
        fileStream.close();
    }

    private void assertStateContentEqual(
            Path stateFilePath, long headBytes, FSDataInputStream inputStream) throws IOException {
        byte[] excepted = readHeadBytes(stateFilePath, headBytes);
        byte[] actual = new byte[excepted.length];
        IOUtils.readFully(inputStream, actual, 0, actual.length);
        // make sure there is no more bytes in inputStream
        assertThat(inputStream.read()).isEqualTo(-1);
        assertThat(actual).isEqualTo(excepted);
        inputStream.close();
    }

    private byte[] readHeadBytes(Path path, long headBytes) throws IOException {
        FileSystem fileSystem = path.getFileSystem();
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        Preconditions.checkNotNull(fileStatus);

        long len = fileStatus.getLen();
        Preconditions.checkState(len >= headBytes);

        try (FSDataInputStream inputStream = fileSystem.open(path)) {

            int toRead = (int) (headBytes > 0 ? headBytes : len);
            byte[] content = new byte[toRead];

            int offset = 0;
            final int singleReadSize = 16 * 1024;

            while (toRead > 0) {
                int num = inputStream.read(content, offset, Math.min(toRead, singleReadSize));
                if (num == -1) {
                    break;
                }
                offset += num;
                toRead -= num;
            }

            return content;
        }
    }

    private static class SpecifiedException extends IOException {
        SpecifiedException(String message) {
            super(message);
        }
    }

    /** The last stream will be broken stream. */
    private static class LastFailingCheckpointStateOutputStreamFactory
            implements CheckpointStreamFactory {

        private final CheckpointStreamFactory checkpointStreamFactory;
        private final int streamTotalCount;
        private final AtomicInteger streamCount;
        private final IOException expectedException;

        private LastFailingCheckpointStateOutputStreamFactory(
                CheckpointStreamFactory checkpointStreamFactory,
                int streamTotalCount,
                IOException expectedException) {
            this.checkpointStreamFactory = checkpointStreamFactory;
            this.streamTotalCount = streamTotalCount;
            this.expectedException = expectedException;
            this.streamCount = new AtomicInteger();
        }

        @Override
        public CheckpointStateOutputStream createCheckpointStateOutputStream(
                CheckpointedStateScope scope) throws IOException {
            if (streamCount.incrementAndGet() == streamTotalCount) {
                return createFailingCheckpointStateOutputStream(expectedException);
            }
            return checkpointStreamFactory.createCheckpointStateOutputStream(scope);
        }

        @Override
        public boolean canFastDuplicate(
                StreamStateHandle stateHandle, CheckpointedStateScope scope) {
            return false;
        }

        @Override
        public List<StreamStateHandle> duplicate(
                List<StreamStateHandle> stateHandles, CheckpointedStateScope scope)
                throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /** Test that the exception arose in the thread pool will rethrow to the main thread. */
    @Test
    public void testMultiThreadRestoreThreadPoolExceptionRethrow() {
        SpecifiedException expectedCause =
                new SpecifiedException("throw exception while multi thread restore.");
        StreamStateHandle stateHandle = new ThrowingStateHandle(expectedCause);

        List<HandleAndLocalPath> stateHandles = new ArrayList<>(1);
        stateHandles.add(HandleAndLocalPath.of(stateHandle, "state1"));

        IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                        1,
                        stateHandles,
                        stateHandles,
                        stateHandle);

        try (ForStStateDataTransfer stateTransfer = new ForStStateDataTransfer(5)) {
            stateTransfer.transferAllStateDataToDirectory(
                    Collections.singletonList(
                            new StateHandleTransferSpec(
                                    incrementalKeyedStateHandle,
                                    Path.fromLocalFile(TempDirUtils.newFolder(temporaryFolder)))),
                    new CloseableRegistry(),
                    RecoveryClaimMode.DEFAULT);
            fail();
        } catch (Exception e) {
            assertEquals(expectedCause, e.getCause());
        }
    }

    /** Tests that transfer files to forst working dir with multi-thread correctly. */
    @Test
    public void testMultiThreadRestoreCorrectly() throws Exception {
        int numRemoteHandles = 3;
        int numSubHandles = 6;
        byte[][][] contents = createContents(numRemoteHandles, numSubHandles);
        List<StateHandleTransferSpec> transferRequests = new ArrayList<>(numRemoteHandles);
        for (int i = 0; i < numRemoteHandles; ++i) {
            transferRequests.add(
                    createTransferRequestForContent(
                            Path.fromLocalFile(TempDirUtils.newFolder(temporaryFolder)),
                            contents[i],
                            i));
        }

        try (ForStStateDataTransfer stateTransfer = new ForStStateDataTransfer(4)) {
            stateTransfer.transferAllStateDataToDirectory(
                    transferRequests, new CloseableRegistry(), RecoveryClaimMode.DEFAULT);
        }

        for (int i = 0; i < numRemoteHandles; ++i) {
            StateHandleTransferSpec transferRequest = transferRequests.get(i);
            Path dstPath = transferRequest.getTransferDestination();

            assertTrue(dstPath.getFileSystem().exists(dstPath));

            for (int j = 0; j < numSubHandles; ++j) {
                assertStateContentEqual(
                        contents[i][j],
                        new Path(dstPath, String.format("sharedState-%d-%d", i, j)));
            }
        }
    }

    /** Tests cleanup on transfer failures. */
    @Test
    public void testMultiThreadCleanupOnFailure() throws Exception {
        int numRemoteHandles = 3;
        int numSubHandles = 6;
        byte[][][] contents = createContents(numRemoteHandles, numSubHandles);
        List<StateHandleTransferSpec> transferRequests = new ArrayList<>(numRemoteHandles);
        for (int i = 0; i < numRemoteHandles; ++i) {
            transferRequests.add(
                    createTransferRequestForContent(
                            Path.fromLocalFile(TempDirUtils.newFolder(temporaryFolder)),
                            contents[i],
                            i));
        }

        IncrementalRemoteKeyedStateHandle stateHandle =
                transferRequests.get(transferRequests.size() - 1).getStateHandle();

        // Add a state handle that induces an exception
        stateHandle
                .getSharedState()
                .add(
                        HandleAndLocalPath.of(
                                new ThrowingStateHandle(new IOException("Test exception.")),
                                "error-handle"));

        CloseableRegistry closeableRegistry = new CloseableRegistry();
        try (ForStStateDataTransfer stateTransfer = new ForStStateDataTransfer(5)) {
            stateTransfer.transferAllStateDataToDirectory(
                    transferRequests, closeableRegistry, RecoveryClaimMode.DEFAULT);
            fail("Exception is expected");
        } catch (IOException ignore) {
        }

        // Check that all transfer directories have been deleted
        for (StateHandleTransferSpec transferRequest : transferRequests) {
            Path dstPath = transferRequest.getTransferDestination();
            assertFalse(dstPath.getFileSystem().exists(dstPath));
        }
        // The passed in closable registry should not be closed by us on failure.
        assertFalse(closeableRegistry.isClosed());
    }

    private void assertStateContentEqual(byte[] expected, Path path) throws IOException {
        byte[] actual = readHeadBytes(path, -1);
        Assertions.assertEquals(expected.length, actual.length);
        assertThat(actual).isEqualTo(expected);
    }

    private static class ThrowingStateHandle implements TestStreamStateHandle {
        private static final long serialVersionUID = -2102069659550694805L;

        private final IOException expectedException;

        private ThrowingStateHandle(IOException expectedException) {
            this.expectedException = expectedException;
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            throw expectedException;
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return 0;
        }
    }

    private byte[][][] createContents(int numRemoteHandles, int numSubHandles) {
        Random random = new Random();
        byte[][][] contents = new byte[numRemoteHandles][numSubHandles][];
        for (int i = 0; i < numRemoteHandles; ++i) {
            for (int j = 0; j < numSubHandles; ++j) {
                contents[i][j] = new byte[random.nextInt(100000) + 1];
                random.nextBytes(contents[i][j]);
            }
        }
        return contents;
    }

    private StateHandleTransferSpec createTransferRequestForContent(
            Path dstPath, byte[][] content, int remoteHandleId) {
        int numSubHandles = content.length;
        List<StreamStateHandle> handles = new ArrayList<>(numSubHandles);
        for (int i = 0; i < numSubHandles; ++i) {
            handles.add(
                    new ByteStreamStateHandle(
                            String.format("state-%d-%d", remoteHandleId, i), content[i]));
        }

        List<HandleAndLocalPath> sharedStates = new ArrayList<>(numSubHandles);
        List<HandleAndLocalPath> privateStates = new ArrayList<>(numSubHandles);
        for (int i = 0; i < numSubHandles; ++i) {
            sharedStates.add(
                    HandleAndLocalPath.of(
                            handles.get(i), String.format("sharedState-%d-%d", remoteHandleId, i)));
            privateStates.add(
                    HandleAndLocalPath.of(
                            handles.get(i),
                            String.format("privateState-%d-%d", remoteHandleId, i)));
        }

        IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.of(0, 1),
                        1,
                        sharedStates,
                        privateStates,
                        handles.get(0));

        return new StateHandleTransferSpec(incrementalKeyedStateHandle, dstPath);
    }
}
