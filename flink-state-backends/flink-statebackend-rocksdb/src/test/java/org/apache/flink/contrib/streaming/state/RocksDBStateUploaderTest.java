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
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test class for {@link RocksDBStateUploader}. */
public class RocksDBStateUploaderTest extends TestLogger {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    /** Test that the exception arose in the thread pool will rethrow to the main thread. */
    @Test
    public void testMultiThreadUploadThreadPoolExceptionRethrow() throws IOException {
        SpecifiedException expectedException =
                new SpecifiedException("throw exception while multi thread upload states.");

        CheckpointStateOutputStream outputStream =
                createFailingCheckpointStateOutputStream(expectedException);
        CheckpointStreamFactory checkpointStreamFactory =
                new CheckpointStreamFactory() {
                    @Override
                    public CheckpointStateOutputStream createCheckpointStateOutputStream(
                            CheckpointedStateScope scope) throws IOException {
                        return outputStream;
                    }

                    @Override
                    public boolean canFastDuplicate(
                            StreamStateHandle stateHandle, CheckpointedStateScope scope)
                            throws IOException {
                        return false;
                    }

                    @Override
                    public List<StreamStateHandle> duplicate(
                            List<StreamStateHandle> stateHandles, CheckpointedStateScope scope)
                            throws IOException {
                        return null;
                    }
                };

        File file = temporaryFolder.newFile(String.valueOf(UUID.randomUUID()));
        generateRandomFileContent(file.getPath(), 20);

        Map<StateHandleID, Path> filePaths = new HashMap<>(1);
        filePaths.put(new StateHandleID("mockHandleID"), file.toPath());
        try (RocksDBStateUploader rocksDBStateUploader = new RocksDBStateUploader(5)) {
            rocksDBStateUploader.uploadFilesToCheckpointFs(
                    filePaths,
                    checkpointStreamFactory,
                    CheckpointedStateScope.SHARED,
                    new CloseableRegistry(),
                    new CloseableRegistry());
            fail();
        } catch (Exception e) {
            assertEquals(expectedException, e);
        }
    }

    @Test
    public void testUploadedSstCanBeCleanedUp() throws Exception {
        SpecifiedException expectedException =
                new SpecifiedException("throw exception while multi thread upload states.");

        File checkpointPrivateFolder = temporaryFolder.newFolder("private");
        org.apache.flink.core.fs.Path checkpointPrivateDirectory =
                org.apache.flink.core.fs.Path.fromLocalFile(checkpointPrivateFolder);

        File checkpointSharedFolder = temporaryFolder.newFolder("shared");
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
        temporaryFolder.newFolder(localFolder);

        Map<StateHandleID, Path> filePaths =
                generateRandomSstFiles(localFolder, sstFileCount, fileStateSizeThreshold);
        CloseableRegistry tmpResourcesRegistry = new CloseableRegistry();
        try (RocksDBStateUploader rocksDBStateUploader = new RocksDBStateUploader(1)) {
            rocksDBStateUploader.uploadFilesToCheckpointFs(
                    filePaths,
                    checkpointStreamFactory,
                    CheckpointedStateScope.SHARED,
                    new CloseableRegistry(),
                    tmpResourcesRegistry);

            try {
                rocksDBStateUploader.uploadFilesToCheckpointFs(
                        filePaths,
                        new LastFailingCheckpointStateOutputStreamFactory(
                                checkpointStreamFactory, sstFileCount, expectedException),
                        CheckpointedStateScope.SHARED,
                        new CloseableRegistry(),
                        tmpResourcesRegistry);
                fail();
            } catch (Exception e) {
                assertEquals(expectedException, e);
            }
            assertEquals(0, checkNotNull(checkpointPrivateFolder.list()).length);
            assertTrue(checkNotNull(checkpointSharedFolder.list()).length > 0);

            tmpResourcesRegistry.close();
            // Check whether the temporary file before the exception can be cleaned up
            assertEquals(0, checkNotNull(checkpointPrivateFolder.list()).length);
            assertEquals(0, checkNotNull(checkpointSharedFolder.list()).length);

            Map.Entry<StateHandleID, Path> first = filePaths.entrySet().stream().findFirst().get();
            try {
                rocksDBStateUploader.uploadFilesToCheckpointFs(
                        Collections.singletonMap(first.getKey(), first.getValue()),
                        checkpointStreamFactory,
                        CheckpointedStateScope.SHARED,
                        new CloseableRegistry(),
                        tmpResourcesRegistry);
            } catch (Exception e) {
                assertTrue(e instanceof IOException);
            }

            // Check whether the temporary file after the exception can be cleaned up.
            assertEquals(0, checkNotNull(checkpointPrivateFolder.list()).length);
            assertEquals(0, checkNotNull(checkpointSharedFolder.list()).length);
        }
    }

    /** Test that upload files with multi-thread correctly. */
    @Test
    public void testMultiThreadUploadCorrectly() throws Exception {
        File checkpointPrivateFolder = temporaryFolder.newFolder("private");
        org.apache.flink.core.fs.Path checkpointPrivateDirectory =
                org.apache.flink.core.fs.Path.fromLocalFile(checkpointPrivateFolder);

        File checkpointSharedFolder = temporaryFolder.newFolder("shared");
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
        temporaryFolder.newFolder(localFolder);

        int sstFileCount = 6;
        Map<StateHandleID, Path> sstFilePaths =
                generateRandomSstFiles(localFolder, sstFileCount, fileStateSizeThreshold);

        try (RocksDBStateUploader rocksDBStateUploader = new RocksDBStateUploader(5)) {
            Map<StateHandleID, StreamStateHandle> sstFiles =
                    rocksDBStateUploader.uploadFilesToCheckpointFs(
                            sstFilePaths,
                            checkpointStreamFactory,
                            CheckpointedStateScope.SHARED,
                            new CloseableRegistry(),
                            new CloseableRegistry());

            for (Map.Entry<StateHandleID, Path> entry : sstFilePaths.entrySet()) {
                assertStateContentEqual(
                        entry.getValue(), sstFiles.get(entry.getKey()).openInputStream());
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

    private Map<StateHandleID, Path> generateRandomSstFiles(
            String localFolder, int sstFileCount, int fileStateSizeThreshold) throws IOException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        Map<StateHandleID, Path> sstFilePaths = new HashMap<>(sstFileCount);
        for (int i = 0; i < sstFileCount; ++i) {
            File file = temporaryFolder.newFile(String.format("%s/%d.sst", localFolder, i));
            generateRandomFileContent(
                    file.getPath(), random.nextInt(1_000_000) + fileStateSizeThreshold);
            sstFilePaths.put(new StateHandleID(String.valueOf(i)), file.toPath());
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
        assertEquals(-1, inputStream.read());
        assertArrayEquals(excepted, actual);
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
