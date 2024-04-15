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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link RocksDBStateUploader}. */
public class RocksDBStateUploaderTest extends TestLogger {

    @TempDir private Path temporaryFolder;

    /** Test that the exception arose in the thread pool will rethrow to the main thread. */
    @Test
    void testMultiThreadUploadThreadPoolExceptionRethrow() throws IOException {
        SpecifiedException expectedException =
                new SpecifiedException("throw exception while multi thread upload states.");

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
        filePaths.add(file.toPath());
        try (RocksDBStateUploader rocksDBStateUploader = new RocksDBStateUploader(5)) {
            assertThatThrownBy(
                            () ->
                                    rocksDBStateUploader.uploadFilesToCheckpointFs(
                                            filePaths,
                                            checkpointStreamFactory,
                                            CheckpointedStateScope.SHARED,
                                            new CloseableRegistry(),
                                            new CloseableRegistry()))
                    .isEqualTo(expectedException);
        }
    }

    @Test
    void testUploadedSstCanBeCleanedUp() throws Exception {
        SpecifiedException expectedException =
                new SpecifiedException("throw exception while multi thread upload states.");

        File checkpointPrivateFolder = TempDirUtils.newFolder(temporaryFolder, "private");
        org.apache.flink.core.fs.Path checkpointPrivateDirectory =
                org.apache.flink.core.fs.Path.fromLocalFile(checkpointPrivateFolder);

        File checkpointSharedFolder = TempDirUtils.newFolder(temporaryFolder, "shared");
        org.apache.flink.core.fs.Path checkpointSharedDirectory =
                org.apache.flink.core.fs.Path.fromLocalFile(checkpointSharedFolder);

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
        try (RocksDBStateUploader rocksDBStateUploader = new RocksDBStateUploader(1)) {
            rocksDBStateUploader.uploadFilesToCheckpointFs(
                    filePaths,
                    checkpointStreamFactory,
                    CheckpointedStateScope.SHARED,
                    new CloseableRegistry(),
                    tmpResourcesRegistry);

            assertThatThrownBy(
                            () ->
                                    rocksDBStateUploader.uploadFilesToCheckpointFs(
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
                                    rocksDBStateUploader.uploadFilesToCheckpointFs(
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

    /** Test that upload files with multi-thread correctly. */
    @Test
    void testMultiThreadUploadCorrectly() throws Exception {
        File checkpointPrivateFolder = TempDirUtils.newFolder(temporaryFolder, "private");
        org.apache.flink.core.fs.Path checkpointPrivateDirectory =
                org.apache.flink.core.fs.Path.fromLocalFile(checkpointPrivateFolder);

        File checkpointSharedFolder = TempDirUtils.newFolder(temporaryFolder, "shared");
        org.apache.flink.core.fs.Path checkpointSharedDirectory =
                org.apache.flink.core.fs.Path.fromLocalFile(checkpointSharedFolder);

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

        try (RocksDBStateUploader rocksDBStateUploader = new RocksDBStateUploader(5)) {
            List<HandleAndLocalPath> sstFiles =
                    rocksDBStateUploader.uploadFilesToCheckpointFs(
                            sstFilePaths,
                            checkpointStreamFactory,
                            CheckpointedStateScope.SHARED,
                            new CloseableRegistry(),
                            new CloseableRegistry());

            for (Path path : sstFilePaths) {
                assertStateContentEqual(
                        path,
                        sstFiles.stream()
                                .filter(e -> e.getLocalPath().equals(path.getFileName().toString()))
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
            String localFolder, int sstFileCount, int fileStateSizeThreshold) throws IOException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        List<Path> sstFilePaths = new ArrayList<>(sstFileCount);
        for (int i = 0; i < sstFileCount; ++i) {
            File file =
                    TempDirUtils.newFile(
                            temporaryFolder, String.format("%s/%d.sst", localFolder, i));
            generateRandomFileContent(
                    file.getPath(), random.nextInt(1_000_000) + fileStateSizeThreshold);
            sstFilePaths.add(file.toPath());
        }
        return sstFilePaths;
    }

    private void generateRandomFileContent(String filePath, int fileLength) throws IOException {
        FileOutputStream fileStream = new FileOutputStream(filePath);
        byte[] contents = new byte[fileLength];
        ThreadLocalRandom.current().nextBytes(contents);
        fileStream.write(contents);
        fileStream.close();
    }

    private void assertStateContentEqual(Path stateFilePath, FSDataInputStream inputStream)
            throws IOException {
        byte[] excepted = Files.readAllBytes(stateFilePath);
        byte[] actual = new byte[excepted.length];
        IOUtils.readFully(inputStream, actual, 0, actual.length);
        assertThat(inputStream.read()).isEqualTo(-1);
        assertThat(actual).isEqualTo(excepted);
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
}
