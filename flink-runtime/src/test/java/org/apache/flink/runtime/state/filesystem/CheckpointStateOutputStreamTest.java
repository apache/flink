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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Abstract base class for tests against checkpointing streams. */
@ExtendWith(ParameterizedTestExtension.class)
public class CheckpointStateOutputStreamTest {

    @TempDir private java.nio.file.Path tmp;

    private enum CheckpointStateOutputStreamType {
        FileBasedState,
        FsCheckpointMetaData
    }

    @Parameters
    public static Collection<CheckpointStateOutputStreamType> getCheckpointStateOutputStreamType() {
        return Arrays.asList(CheckpointStateOutputStreamType.values());
    }

    @Parameter public CheckpointStateOutputStreamType stateOutputStreamType;

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    /** Validates that even empty streams create a file and a file state handle. */
    @TestTemplate
    void testEmptyState() throws Exception {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final Path folder = baseFolder();
        final String fileName = "myFileName";
        final Path filePath = new Path(folder, fileName);

        final FileStateHandle handle;
        try (FSDataOutputStream stream = createTestStream(fs, folder, fileName)) {
            handle = closeAndGetResult(stream);
        }

        // must have created a handle
        assertThat(handle).isNotNull();
        assertThat(handle.getFilePath()).isEqualTo(filePath);

        // the pointer path should exist as a directory
        assertThat(fs.exists(handle.getFilePath())).isTrue();
        assertThat(fs.getFileStatus(filePath).isDir()).isFalse();

        // the contents should be empty
        try (FSDataInputStream in = handle.openInputStream()) {
            assertThat(in.read()).isEqualTo(-1);
        }
    }

    /** Simple write and read test. */
    @TestTemplate
    void testWriteAndRead() throws Exception {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final Path folder = baseFolder();
        final String fileName = "fooBarName";

        final Random rnd = new Random();
        final byte[] data = new byte[1694523];

        // write the data (mixed single byte writes and array writes)
        final FileStateHandle handle;
        try (FSDataOutputStream stream = createTestStream(fs, folder, fileName)) {
            for (int i = 0; i < data.length; ) {
                if (rnd.nextBoolean()) {
                    stream.write(data[i++]);
                } else {
                    int len = rnd.nextInt(Math.min(data.length - i, 32));
                    stream.write(data, i, len);
                    i += len;
                }
            }
            handle = closeAndGetResult(stream);
        }

        // (1) stream from handle must hold the contents
        try (FSDataInputStream in = handle.openInputStream()) {
            byte[] buffer = new byte[data.length];
            readFully(in, buffer);
            assertThat(buffer).isEqualTo(data);
        }

        // (2) the pointer must point to a file with that contents
        try (FSDataInputStream in = fs.open(handle.getFilePath())) {
            byte[] buffer = new byte[data.length];
            readFully(in, buffer);
            assertThat(buffer).isEqualTo(data);
        }
    }

    /** Tests that the underlying stream file is deleted upon calling close. */
    @TestTemplate
    void testCleanupWhenClosingStream() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final Path folder = new Path(TempDirUtils.newFolder(tmp).toURI());
        final String fileName = "nonCreativeTestFileName";
        final Path path = new Path(folder, fileName);

        // write some test data and close the stream
        try (FSDataOutputStream stream = createTestStream(fs, folder, fileName)) {
            Random rnd = new Random();
            for (int i = 0; i < rnd.nextInt(1000); i++) {
                stream.write(rnd.nextInt(100));
            }
        }

        assertThat(fs.exists(path)).isFalse();
    }

    /** Tests that the underlying stream file is deleted if the closeAndGetHandle method fails. */
    @TestTemplate
    void testCleanupWhenFailingCloseAndGetHandle() throws IOException {
        final Path folder = new Path(TempDirUtils.newFolder(tmp).toURI());
        final String fileName = "test_name";
        final Path filePath = new Path(folder, fileName);

        final FileSystem fs =
                spy(
                        new FsWithoutRecoverableWriter(
                                (path) -> new FailingCloseStream(new File(path.getPath()))));

        FSDataOutputStream stream = createTestStream(fs, folder, fileName);
        stream.write(new byte[] {1, 2, 3, 4, 5});

        assertThatThrownBy(() -> closeAndGetResult(stream)).isInstanceOf(IOException.class);

        verify(fs).delete(filePath, false);
    }

    /**
     * This test validates that a close operation can happen even while a 'closeAndGetHandle()' call
     * is in progress.
     *
     * <p>That behavior is essential for fast cancellation (concurrent cleanup).
     */
    @TestTemplate
    void testCloseDoesNotLock() throws Exception {
        final Path folder = new Path(TempDirUtils.newFolder(tmp).toURI());
        final String fileName = "this-is-ignored-anyways.file";

        final FileSystem fileSystem =
                spy(new FsWithoutRecoverableWriter((path) -> new BlockerStream()));

        final FSDataOutputStream checkpointStream = createTestStream(fileSystem, folder, fileName);

        final OneShotLatch sync = new OneShotLatch();

        final CheckedThread thread =
                new CheckedThread() {

                    @Override
                    public void go() throws Exception {
                        sync.trigger();
                        // that call should now block, because it accesses the position
                        closeAndGetResult(checkpointStream);
                    }
                };
        thread.start();

        sync.await();
        checkpointStream.close();

        // the thread may or may not fail, that depends on the thread race
        // it is not important for this test, important is that the thread does not freeze/lock up
        try {
            thread.sync();
        } catch (IOException ignored) {
        }
    }

    /** Creates a new test stream instance. */
    private FSDataOutputStream createTestStream(FileSystem fs, Path dir, String fileName)
            throws IOException {
        switch (stateOutputStreamType) {
            case FileBasedState:
                return new FileBasedStateOutputStream(fs, new Path(dir, fileName));
            case FsCheckpointMetaData:
                Path fullPath = new Path(dir, fileName);
                return new FsCheckpointMetadataOutputStream(fs, fullPath, dir);
            default:
                throw new IllegalStateException("Unsupported checkpoint stream output type.");
        }
    }

    /** Closes the stream successfully and returns a FileStateHandle to the result. */
    private FileStateHandle closeAndGetResult(FSDataOutputStream stream) throws IOException {
        switch (stateOutputStreamType) {
            case FileBasedState:
                return ((FileBasedStateOutputStream) stream).closeAndGetHandle();
            case FsCheckpointMetaData:
                return ((FsCheckpointMetadataOutputStream) stream)
                        .closeAndFinalizeCheckpoint()
                        .getMetadataHandle();
            default:
                throw new IllegalStateException("Unsupported checkpoint stream output type.");
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private Path baseFolder() throws Exception {
        return new Path(
                new File(TempDirUtils.newFolder(tmp), UUID.randomUUID().toString()).toURI());
    }

    private static void readFully(InputStream in, byte[] buffer) throws IOException {
        int pos = 0;
        int remaining = buffer.length;

        while (remaining > 0) {
            int read = in.read(buffer, pos, remaining);
            if (read == -1) {
                throw new EOFException();
            }

            pos += read;
            remaining -= read;
        }
    }

    private static class BlockerStream extends FSDataOutputStream {

        private final OneShotLatch blocker = new OneShotLatch();

        @Override
        public long getPos() throws IOException {
            block();
            return 0L;
        }

        @Override
        public void write(int b) throws IOException {
            block();
        }

        @Override
        public void flush() throws IOException {
            block();
        }

        @Override
        public void sync() throws IOException {
            block();
        }

        @Override
        public void close() throws IOException {
            blocker.trigger();
        }

        private void block() throws IOException {
            try {
                blocker.await();
            } catch (InterruptedException e) {
                throw new IOException("interrupted");
            }
            throw new IOException("closed");
        }
    }

    // ------------------------------------------------------------------------

    private static class FailingCloseStream extends LocalDataOutputStream {

        FailingCloseStream(File file) throws IOException {
            super(file);
        }

        @Override
        public void close() throws IOException {
            throw new IOException();
        }
    }

    private static class FsWithoutRecoverableWriter extends LocalFileSystem {

        private final FunctionWithException<Path, FSDataOutputStream, IOException> streamFactory;

        FsWithoutRecoverableWriter(
                FunctionWithException<Path, FSDataOutputStream, IOException> streamFactory) {
            this.streamFactory = streamFactory;
        }

        @Override
        public FSDataOutputStream create(Path filePath, WriteMode overwrite) throws IOException {
            return streamFactory.apply(filePath);
        }

        @Override
        public LocalRecoverableWriter createRecoverableWriter() throws IOException {
            throw new UnsupportedOperationException(
                    "This file system does not support recoverable writers.");
        }
    }
}
