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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link FsCheckpointStateOutputStream}. */
@ExtendWith(ParameterizedTestExtension.class)
public class FsCheckpointStateOutputStreamTest {

    @Parameters(name = "relativePaths = {0}")
    public static List<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @Parameter public boolean relativePaths;

    @TempDir private java.nio.file.Path tempDir;

    @TestTemplate
    void testWrongParameters() throws Exception {
        // this should fail
        assertThatThrownBy(
                        () ->
                                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                                        FileSystem.getLocalFileSystem(),
                                        4000,
                                        5000,
                                        relativePaths))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testEmptyState() throws Exception {
        CheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                        FileSystem.getLocalFileSystem(),
                        1024,
                        512,
                        relativePaths);

        StreamStateHandle handle = stream.closeAndGetHandle();
        assertThat(handle).isNull();
    }

    @TestTemplate
    void testStateBelowMemThreshold() throws Exception {
        runTest(999, 1024, 1000, false);
    }

    @TestTemplate
    void testStateOneBufferAboveThreshold() throws Exception {
        runTest(896, 1024, 15, true);
    }

    @TestTemplate
    void testStateAboveMemThreshold() throws Exception {
        runTest(576446, 259, 17, true);
    }

    @TestTemplate
    void testZeroThreshold() throws Exception {
        runTest(16678, 4096, 0, true);
    }

    @TestTemplate
    void testGetPos() throws Exception {
        CheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                        FileSystem.getLocalFileSystem(),
                        31,
                        17,
                        relativePaths);

        for (int i = 0; i < 64; ++i) {
            assertThat(stream.getPos()).isEqualTo(i);
            stream.write(0x42);
        }

        stream.closeAndGetHandle();

        // ----------------------------------------------------

        stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                        FileSystem.getLocalFileSystem(),
                        31,
                        17,
                        relativePaths);

        byte[] data = "testme!".getBytes(ConfigConstants.DEFAULT_CHARSET);

        for (int i = 0; i < 7; ++i) {
            assertThat(stream.getPos()).isEqualTo(i * (1L + data.length));
            stream.write(0x42);
            stream.write(data);
        }

        stream.closeAndGetHandle();
    }

    /** Tests that the underlying stream file is deleted upon calling close. */
    @TestTemplate
    void testCleanupWhenClosingStream() throws IOException {

        final FileSystem fs = mock(FileSystem.class);
        final FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

        final ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);

        when(fs.create(pathCaptor.capture(), any(FileSystem.WriteMode.class)))
                .thenReturn(outputStream);

        CheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                        fs,
                        4,
                        0,
                        relativePaths);

        // this should create the underlying file stream
        stream.write(new byte[] {1, 2, 3, 4, 5});

        verify(fs).create(any(Path.class), any(FileSystem.WriteMode.class));

        stream.close();

        verify(fs).delete(eq(pathCaptor.getValue()), anyBoolean());
    }

    /** Tests that the underlying stream file is deleted if the closeAndGetHandle method fails. */
    @TestTemplate
    void testCleanupWhenFailingCloseAndGetHandle() throws IOException {
        final FileSystem fs = mock(FileSystem.class);
        final FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

        final ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);

        when(fs.create(pathCaptor.capture(), any(FileSystem.WriteMode.class)))
                .thenReturn(outputStream);
        doThrow(new IOException("Test IOException.")).when(outputStream).close();

        CheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                        fs,
                        4,
                        0,
                        relativePaths);

        // this should create the underlying file stream
        stream.write(new byte[] {1, 2, 3, 4, 5});

        verify(fs).create(any(Path.class), any(FileSystem.WriteMode.class));

        assertThatThrownBy(stream::closeAndGetHandle).isInstanceOf(IOException.class);

        verify(fs).delete(eq(pathCaptor.getValue()), anyBoolean());
    }

    private void runTest(int numBytes, int bufferSize, int threshold, boolean expectFile)
            throws Exception {
        CheckpointStateOutputStream stream =
                new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                        FileSystem.getLocalFileSystem(),
                        bufferSize,
                        threshold,
                        relativePaths);

        Random rnd = new Random();
        byte[] original = new byte[numBytes];
        byte[] bytes = new byte[original.length];

        rnd.nextBytes(original);
        System.arraycopy(original, 0, bytes, 0, original.length);

        // the test writes a mixture of writing individual bytes and byte arrays
        int pos = 0;
        while (pos < bytes.length) {
            boolean single = rnd.nextBoolean();
            if (single) {
                stream.write(bytes[pos++]);
            } else {
                int num =
                        rnd.nextBoolean() ? (bytes.length - pos) : rnd.nextInt(bytes.length - pos);
                stream.write(bytes, pos, num);
                pos += num;
            }
        }

        StreamStateHandle handle = stream.closeAndGetHandle();
        if (expectFile) {
            assertThat(handle).isInstanceOf(FileStateHandle.class);
        } else {
            assertThat(handle).isInstanceOf(ByteStreamStateHandle.class);
        }

        // make sure the writing process did not alter the original byte array
        assertThat(bytes).isEqualTo(original);

        try (InputStream inStream = handle.openInputStream()) {
            byte[] validation = new byte[bytes.length];

            DataInputStream dataInputStream = new DataInputStream(inStream);
            dataInputStream.readFully(validation);

            assertThat(validation).isEqualTo(bytes);
        }

        handle.discardState();
    }

    @TestTemplate
    void testWriteFailsFastWhenClosed() throws Exception {
        FsCheckpointStateOutputStream stream =
                new FsCheckpointStateOutputStream(
                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                        FileSystem.getLocalFileSystem(),
                        1024,
                        512,
                        relativePaths);

        assertThat(stream.isClosed()).isFalse();

        stream.close();
        assertThat(stream.isClosed()).isTrue();

        assertThatThrownBy(() -> stream.write(1)).isInstanceOf(IOException.class);

        assertThatThrownBy(() -> stream.write(new byte[4], 1, 2)).isInstanceOf(IOException.class);
    }

    @TestTemplate
    void testMixedBelowAndAboveThreshold() throws Exception {
        final byte[] state1 = new byte[1274673];
        final byte[] state2 = new byte[1];
        final byte[] state3 = new byte[0];
        final byte[] state4 = new byte[177];

        final Random rnd = new Random();
        rnd.nextBytes(state1);
        rnd.nextBytes(state2);
        rnd.nextBytes(state3);
        rnd.nextBytes(state4);

        final File directory = TempDirUtils.newFolder(tempDir);
        final Path basePath = Path.fromLocalFile(directory);

        final Supplier<CheckpointStateOutputStream> factory =
                () ->
                        new FsCheckpointStateOutputStream(
                                basePath, FileSystem.getLocalFileSystem(), 1024, 15, relativePaths);

        CheckpointStateOutputStream stream1 = factory.get();
        CheckpointStateOutputStream stream2 = factory.get();
        CheckpointStateOutputStream stream3 = factory.get();

        stream1.write(state1);
        stream2.write(state2);
        stream3.write(state3);

        FileStateHandle handle1 = (FileStateHandle) stream1.closeAndGetHandle();
        ByteStreamStateHandle handle2 = (ByteStreamStateHandle) stream2.closeAndGetHandle();
        ByteStreamStateHandle handle3 = (ByteStreamStateHandle) stream3.closeAndGetHandle();

        // use with try-with-resources
        StreamStateHandle handle4;
        try (CheckpointStateOutputStream stream4 = factory.get()) {
            stream4.write(state4);
            handle4 = stream4.closeAndGetHandle();
        }

        // close before accessing handle
        CheckpointStateOutputStream stream5 = factory.get();
        stream5.write(state4);
        stream5.close();
        assertThatThrownBy(stream5::closeAndGetHandle).isInstanceOf(IOException.class);

        validateBytesInStream(handle1.openInputStream(), state1);
        handle1.discardState();
        assertThat(isDirectoryEmpty(directory)).isFalse();
        ensureLocalFileDeleted(handle1.getFilePath());

        validateBytesInStream(handle2.openInputStream(), state2);
        handle2.discardState();
        assertThat(isDirectoryEmpty(directory)).isFalse();

        // nothing was written to the stream, so it will return nothing
        assertThat(handle3).isNull();
        assertThat(isDirectoryEmpty(directory)).isFalse();

        validateBytesInStream(handle4.openInputStream(), state4);
        handle4.discardState();
        assertThat(isDirectoryEmpty(directory)).isTrue();
    }

    // ------------------------------------------------------------------------
    //  Not deleting parent directories
    // ------------------------------------------------------------------------

    /**
     * This test checks that the stream does not check and clean the parent directory when
     * encountering a write error.
     */
    @Tag("org.apache.flink.testutils.junit.FailsInGHAContainerWithRootUser")
    @TestTemplate
    void testStreamDoesNotTryToCleanUpParentOnError() throws Exception {
        final File directory = TempDirUtils.newFolder(tempDir);

        // prevent creation of files in that directory
        // this operation does not work reliably on Windows, so we use an "assume" to skip the test
        // is this prerequisite operation is not supported.
        assumeThat(directory.setWritable(false, true)).isTrue();
        checkDirectoryNotWritable(directory);

        FileSystem fs = spy(FileSystem.getLocalFileSystem());

        FsCheckpointStateOutputStream stream1 =
                new FsCheckpointStateOutputStream(
                        Path.fromLocalFile(directory), fs, 1024, 1, relativePaths);

        FsCheckpointStateOutputStream stream2 =
                new FsCheckpointStateOutputStream(
                        Path.fromLocalFile(directory), fs, 1024, 1, relativePaths);

        stream1.write(new byte[61]);
        stream2.write(new byte[61]);

        assertThatThrownBy(stream1::closeAndGetHandle).isInstanceOf(IOException.class);

        stream2.close();

        // no delete call must have happened
        verify(fs, times(0)).delete(any(Path.class), anyBoolean());

        // the directory must still exist as a proper directory
        assertThat(directory).exists();
        assertThat(directory).isDirectory();
    }

    /**
     * FLINK-28984. This test checks that the inner stream should be closed when
     * FsCheckpointStateOutputStream#close() and FsCheckpointStateOutputStream#flushToFile() run
     * concurrently.
     */
    @TestTemplate
    public void testCleanupWhenCloseableRegistryClosedBeforeCreatingStream() throws Exception {
        OneShotLatch streamCreationLatch = new OneShotLatch();
        OneShotLatch startCloseLatch = new OneShotLatch();
        OneShotLatch endCloseLatch = new OneShotLatch();
        FileSystem fs = mock(FileSystem.class);
        FSDataOutputStream fsDataOutputStream = mock(FSDataOutputStream.class);

        // mock the FileSystem#create method to simulate concurrency situation with
        // FsCheckpointStateOutputStream#close thread
        doAnswer(
                        invocation -> {
                            // make sure stream creation thread goes first
                            streamCreationLatch.trigger();
                            // wait for CloseableRegistry#close (and
                            // FsCheckpointStateOutputStream#close) getting to be triggered
                            startCloseLatch.await();
                            // make sure the CloseableRegistry#close cannot be completed due to
                            // failing to acquire lock
                            assertThrows(
                                    TimeoutException.class,
                                    () -> endCloseLatch.await(1, TimeUnit.SECONDS));
                            return fsDataOutputStream;
                        })
                .when(fs)
                .create(any(Path.class), any(FileSystem.WriteMode.class));

        FsCheckpointStateOutputStream outputStream =
                new FsCheckpointStateOutputStream(
                        Path.fromLocalFile(TempDirUtils.newFolder(tempDir)),
                        fs,
                        1024,
                        1,
                        relativePaths);
        CompletableFuture<Void> flushFuture;
        CloseableRegistry closeableRegistry = new CloseableRegistry();
        closeableRegistry.registerCloseable(outputStream);
        flushFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                // try to create a stream
                                outputStream.flushToFile();
                            } catch (IOException e) {
                                // ignore this exception because we don't want to fail the test due
                                // to IO issue
                            }
                        },
                        Executors.newSingleThreadExecutor());
        // make sure stream creation thread goes first
        streamCreationLatch.await();
        // verify the outputStream and inner fsDataOutputStream is not closed
        assertFalse(outputStream.isClosed());
        verify(fsDataOutputStream, never()).close();

        // start to close the outputStream (inside closeableRegistry)
        startCloseLatch.trigger();
        closeableRegistry.close();
        // This endCloseLatch should not be triggered in time because the
        // FsCheckpointStateOutputStream#close will be blocked due to failing to acquire lock
        endCloseLatch.trigger();
        // wait for flush completed
        flushFuture.get();

        // verify the outputStream and inner fsDataOutputStream is correctly closed
        assertTrue(outputStream.isClosed());
        verify(fsDataOutputStream).close();
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static void ensureLocalFileDeleted(Path path) {
        URI uri = path.toUri();
        if ("file".equals(uri.getScheme())) {
            File file = new File(uri.getPath());
            assertThat(file).withFailMessage("file not properly deleted").doesNotExist();
        } else {
            throw new IllegalArgumentException("not a local path");
        }
    }

    private static boolean isDirectoryEmpty(File directory) {
        if (!directory.exists()) {
            return true;
        }
        String[] nested = directory.list();
        return nested == null || nested.length == 0;
    }

    private static void validateBytesInStream(InputStream is, byte[] data) throws IOException {
        try {
            byte[] holder = new byte[data.length];

            int pos = 0;
            int read;
            while (pos < holder.length
                    && (read = is.read(holder, pos, holder.length - pos)) != -1) {
                pos += read;
            }

            assertThat(pos).withFailMessage("not enough data").isEqualTo(holder.length);
            assertThat(is.read()).withFailMessage("too much data").isEqualTo(-1);
            assertThat(holder).withFailMessage("wrong data").isEqualTo(data);
        } finally {
            is.close();
        }
    }

    private static void checkDirectoryNotWritable(File directory) {
        assertThatThrownBy(
                        () -> {
                            try (FileOutputStream fos =
                                    new FileOutputStream(new File(directory, "temp"))) {
                                fos.write(42);
                                fos.flush();
                            }
                        })
                .withFailMessage("this should fail when writing is properly prevented")
                .isInstanceOf(IOException.class);
    }
}
