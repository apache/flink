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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableFsDataOutputStream;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FsCheckpointMetadataOutputStream}. */
@ExtendWith(ParameterizedTestExtension.class)
public class FsCheckpointMetadataOutputStreamTest {

    @Parameters(name = "{0}")
    public static Collection<FileSystem> getFileSystems() {
        return Arrays.asList(new FsWithRecoverableWriter(), new FsWithoutRecoverableWriter());
    }

    @Parameter public FileSystem fileSystem;

    @TempDir private java.nio.file.Path tempDir;

    @TestTemplate
    void testFileExistence() throws Exception {
        Path metaDataFilePath = baseFolder();
        FsCheckpointMetadataOutputStream stream = createTestStream(metaDataFilePath, fileSystem);

        if (fileSystem instanceof FsWithoutRecoverableWriter) {
            assertThat(fileSystem.exists(metaDataFilePath)).isTrue();
        } else {
            assertThat(fileSystem.exists(metaDataFilePath)).isFalse();
        }

        stream.closeAndFinalizeCheckpoint();

        assertThat(fileSystem.exists(metaDataFilePath)).isTrue();
    }

    @TestTemplate
    void testCleanupWhenClosed() throws Exception {
        Path metaDataFilePath = baseFolder();
        FsCheckpointMetadataOutputStream stream = createTestStream(metaDataFilePath, fileSystem);
        stream.close();
        assertThat(fileSystem.exists(metaDataFilePath)).isFalse();
    }

    @TestTemplate
    void testCleanupWhenCommitFailed() throws Exception {
        Path metaDataFilePath = baseFolder();

        if (fileSystem instanceof FsWithoutRecoverableWriter) {
            fileSystem =
                    ((FsWithoutRecoverableWriter) fileSystem)
                            .withStreamFactory(
                                    (path) -> new FailingCloseStream(new File(path.getPath())));
        } else {
            fileSystem =
                    ((FsWithRecoverableWriter) fileSystem)
                            .withStreamFactory(
                                    (path, temp) ->
                                            new FailingRecoverableFsStream(
                                                    new File(path.getPath()),
                                                    new File(temp.getPath())));
        }

        FsCheckpointMetadataOutputStream stream = createTestStream(metaDataFilePath, fileSystem);
        assertThatThrownBy(stream::closeAndFinalizeCheckpoint)
                .withFailMessage("Exception expected when committing the meta file.")
                .isInstanceOf(IOException.class);
        assertThat(fileSystem.exists(metaDataFilePath)).isFalse();

        if (fileSystem instanceof FsWithoutRecoverableWriter) {
            ((FsWithoutRecoverableWriter) fileSystem).resetStreamFactory();
        } else {
            ((FsWithRecoverableWriter) fileSystem).resetStreamFactory();
        }
    }

    private FsCheckpointMetadataOutputStream createTestStream(
            Path metaDataFilePath, FileSystem fileSystem) throws IOException {
        FsCheckpointMetadataOutputStream stream =
                new FsCheckpointMetadataOutputStream(
                        fileSystem, metaDataFilePath, new Path("fooBarName"));

        for (int i = 0; i < 100; ++i) {
            stream.write(0x42);
        }
        return stream;
    }

    private Path baseFolder() throws Exception {
        return new Path(
                new File(TempDirUtils.newFolder(tempDir), UUID.randomUUID().toString()).toURI());
    }

    private static class FsWithoutRecoverableWriter extends LocalFileSystem {
        private FunctionWithException<Path, FSDataOutputStream, IOException> streamFactory;

        private FileSystem withStreamFactory(
                FunctionWithException<Path, FSDataOutputStream, IOException> streamFactory) {
            this.streamFactory = streamFactory;
            return this;
        }

        private void resetStreamFactory() {
            this.streamFactory = null;
        }

        @Override
        public FSDataOutputStream create(Path filePath, WriteMode overwrite) throws IOException {
            if (streamFactory == null) {
                return super.create(filePath, overwrite);
            }
            return streamFactory.apply(filePath);
        }

        @Override
        public LocalRecoverableWriter createRecoverableWriter() throws IOException {
            throw new UnsupportedOperationException(
                    "This file system does not support recoverable writers.");
        }

        @Override
        public String toString() {
            return "FileSystem without RecoverableWriter";
        }
    }

    private static class FsWithRecoverableWriter extends LocalFileSystem {
        private BiFunctionWithException<Path, Path, LocalRecoverableFsDataOutputStream, IOException>
                streamFactory;

        private FsWithRecoverableWriter withStreamFactory(
                BiFunctionWithException<Path, Path, LocalRecoverableFsDataOutputStream, IOException>
                        streamFactory) {
            this.streamFactory = streamFactory;
            return this;
        }

        private void resetStreamFactory() {
            this.streamFactory = null;
        }

        @Override
        public LocalRecoverableWriter createRecoverableWriter() throws IOException {
            if (streamFactory == null) {
                return super.createRecoverableWriter();
            }
            return new FsLocalRecoverableWriter(this, streamFactory);
        }

        @Override
        public String toString() {
            return "FileSystem with RecoverableWriter";
        }
    }

    private static class FsLocalRecoverableWriter extends LocalRecoverableWriter {
        private final BiFunctionWithException<
                        Path, Path, LocalRecoverableFsDataOutputStream, IOException>
                streamFactory;
        private final LocalFileSystem fs;

        public FsLocalRecoverableWriter(
                LocalFileSystem fs,
                BiFunctionWithException<Path, Path, LocalRecoverableFsDataOutputStream, IOException>
                        streamFactory) {
            super(fs);
            this.fs = fs;
            this.streamFactory = streamFactory;
        }

        @Override
        public RecoverableFsDataOutputStream open(Path filePath) throws IOException {
            File temp = generateStagingTempFilePath(fs.pathToFile(filePath));
            return streamFactory.apply(filePath, new Path(temp.getPath()));
        }
    }

    private static class FailingRecoverableFsStream extends LocalRecoverableFsDataOutputStream {
        public FailingRecoverableFsStream(File targetFile, File tempFile) throws IOException {
            super(targetFile, tempFile);
        }

        @Override
        public void close() throws IOException {
            throw new IOException();
        }
    }

    private static class FailingCloseStream extends LocalDataOutputStream {

        FailingCloseStream(File file) throws IOException {
            super(file);
        }

        @Override
        public void close() throws IOException {
            throw new IOException();
        }
    }
}
