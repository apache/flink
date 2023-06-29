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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.PhysicalFile;
import org.apache.flink.runtime.checkpoint.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.runtime.state.CheckpointedStateScope.EXCLUSIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Test cases for {@link FileMergingCheckpointStateOutputStream}. */
public class FileMergingCheckpointStateOutputStreamTest {
    @Rule public final TemporaryFolder tempDir = new TemporaryFolder();

    private static boolean failWhenClosePhysicalFile = false;

    private static final String CLOSE_FILE_FAILURE_MESSAGE = "Cannot close physical file.";

    private static final int WRITE_BUFFER_SIZE = 256;

    private static boolean isPhysicalFileProvided = false;

    private static boolean physicalFileCanBeReused;

    private static PhysicalFile lastPhysicalFile;

    @Before
    public void setEnv() {
        failWhenClosePhysicalFile = false;
        physicalFileCanBeReused = false;
    }

    private FileMergingCheckpointStateOutputStream getNewStream() throws IOException {
        return getNewStream(false);
    }

    private FileMergingCheckpointStateOutputStream getNewStream(boolean reuseLastPhysicalFile)
            throws IOException {

        PhysicalFile physicalFile;
        if (reuseLastPhysicalFile) {
            assertThat(lastPhysicalFile).isNotNull();
            physicalFile = lastPhysicalFile;
        } else {
            Path dirPath = Path.fromLocalFile(tempDir.newFolder());
            String fileName = UUID.randomUUID().toString();
            Path physicalFilePath = new Path(dirPath, fileName);
            OutputStreamAndPath streamAndPath =
                    EntropyInjector.createEntropyAware(
                            dirPath.getFileSystem(),
                            physicalFilePath,
                            FileSystem.WriteMode.NO_OVERWRITE);
            physicalFile =
                    new PhysicalFile(
                            streamAndPath.stream(), physicalFilePath, (path) -> {}, EXCLUSIVE);
        }
        isPhysicalFileProvided = false;

        // a simplified implementation that excludes the meta info management of files
        return new FileMergingCheckpointStateOutputStream(
                WRITE_BUFFER_SIZE,
                new FileMergingCheckpointStateOutputStream.FileMergingSnapshotManagerProxy() {
                    @Override
                    public Tuple2<FSDataOutputStream, Path> providePhysicalFile() {
                        isPhysicalFileProvided = true;
                        lastPhysicalFile = physicalFile;

                        Preconditions.checkArgument(physicalFile.isOpen());
                        return new Tuple2<>(
                                physicalFile.getOutputStream(), physicalFile.getFilePath());
                    }

                    @Override
                    public SegmentFileStateHandle closeStreamAndCreateStateHandle(
                            Path filePath, long startPos, long stateSize) throws IOException {
                        if (isPhysicalFileProvided) {
                            if (failWhenClosePhysicalFile) {
                                throw new IOException(CLOSE_FILE_FAILURE_MESSAGE);
                            } else if (!physicalFileCanBeReused) {
                                physicalFile.close();
                            }
                        }
                        return new SegmentFileStateHandle(filePath, startPos, stateSize, EXCLUSIVE);
                    }

                    @Override
                    public void closeStreamExceptionally() throws IOException {
                        if (isPhysicalFileProvided) {
                            if (failWhenClosePhysicalFile) {
                                throw new IOException(CLOSE_FILE_FAILURE_MESSAGE);
                            } else {
                                physicalFile.close();
                            }
                        }
                    }
                });
    }

    @Test
    public void testGetHandleFromStream() throws IOException {

        FileMergingCheckpointStateOutputStream stream = getNewStream();
        assertThat(isPhysicalFileProvided).isFalse();
        assertThat(stream.closeAndGetHandle()).isNull();

        stream = getNewStream();
        stream.flush();
        assertThat(isPhysicalFileProvided).isFalse();
        assertThat(stream.closeAndGetHandle()).isNull();

        // return a non-null state handle if flushToFile has been called even if nothing was written
        stream = getNewStream();
        stream.flushToFile();
        assertThat(isPhysicalFileProvided).isTrue();
        SegmentFileStateHandle stateHandle = stream.closeAndGetHandle();
        assertThat(stateHandle).isNotNull();
        assertThat(stateHandle.getStateSize()).isEqualTo(0);

        stream = getNewStream();
        stream.write(new byte[0]);
        stream.flushToFile();
        stateHandle = stream.closeAndGetHandle();
        assertThat(stateHandle).isNotNull();
        assertThat(stateHandle.getStateSize()).isEqualTo(0);

        stream = getNewStream();
        stream.write(new byte[10]);
        stream.flushToFile();
        stateHandle = stream.closeAndGetHandle();
        assertThat(stateHandle).isNotNull();
        assertThat(stateHandle.getStateSize()).isEqualTo(10);

        // closeAndGetHandle should internally call flushToFile
        stream = getNewStream();
        stream.write(new byte[10]);
        stateHandle = stream.closeAndGetHandle();
        assertThat(stateHandle).isNotNull();
        assertThat(stateHandle.getStateSize()).isEqualTo(10);
    }

    @Test
    public void testGetHandleFromClosedStream() throws IOException {
        FileMergingCheckpointStateOutputStream stream = getNewStream();
        stream.close();
        try {
            stream.closeAndGetHandle();
        } catch (Exception ignored) {
            // expected
        }
    }

    @Test
    public void testWhetherFileIsCreatedWhenWritingStream() throws IOException {

        FileMergingCheckpointStateOutputStream stream = getNewStream();
        stream.write(new byte[WRITE_BUFFER_SIZE - 1]);
        assertThat(isPhysicalFileProvided).isFalse();
        stream.write(new byte[2]);
        assertThat(isPhysicalFileProvided).isTrue();

        stream = getNewStream();
        stream.write(new byte[WRITE_BUFFER_SIZE]);
        assertThat(isPhysicalFileProvided).isTrue();

        stream = getNewStream();
        stream.write(new byte[WRITE_BUFFER_SIZE - 1]);
        stream.close();
        assertThat(isPhysicalFileProvided).isFalse();

        stream = getNewStream();
        stream.write(new byte[WRITE_BUFFER_SIZE - 1]);
        stream.closeAndGetHandle();
        assertThat(isPhysicalFileProvided).isTrue();
    }

    @Test
    public void testCloseStream() throws IOException {

        // cannot write anything to a closed stream
        FileMergingCheckpointStateOutputStream stream = getNewStream();
        stream.flushToFile();
        stream.close();
        stream.write(new byte[0]);
        try {
            stream.write(new byte[1]);
        } catch (IOException e) {
            assertThat(e.getMessage()).isEqualTo("Cannot call flushToFile() to a closed stream.");
        }

        failWhenClosePhysicalFile = true;

        // close() throws no exception if it fails to close the file
        stream = getNewStream();
        stream.flushToFile();
        assertThat(isPhysicalFileProvided).isTrue();

        stream.close();

        // closeAndGetHandle() throws exception if it fails to close the file
        stream = getNewStream();
        stream.flushToFile();
        assertThat(isPhysicalFileProvided).isTrue();
        try {
            stream.closeAndGetHandle();
        } catch (IOException e) {
            if (!e.getMessage().equals(CLOSE_FILE_FAILURE_MESSAGE)) {
                throw e;
            }
        }
    }

    @Test
    public void testStateAboveBufferSize() throws Exception {
        runTest(576446);
    }

    @Test
    public void testStateUnderBufferSize() throws Exception {
        runTest(100);
    }

    @Test
    public void testGetPos() throws Exception {
        FileMergingCheckpointStateOutputStream stream = getNewStream();

        // write one byte one time
        for (int i = 0; i < 64; ++i) {
            assertThat(stream.getPos()).isEqualTo(i);
            stream.write(0x42);
        }

        stream.closeAndGetHandle();

        // write random number of bytes one time
        stream = getNewStream();

        Random rnd = new Random();
        long expectedPos = 0;
        for (int i = 0; i < 7; ++i) {
            int numBytes = rnd.nextInt(16);
            expectedPos += numBytes;
            stream.write(new byte[numBytes]);
            assertThat(stream.getPos()).isEqualTo(expectedPos);
        }

        physicalFileCanBeReused = true;
        SegmentFileStateHandle stateHandle = stream.closeAndGetHandle();

        // reuse the last physical file
        assertThat(stateHandle).isNotNull();
        expectedPos = 0;
        stream = getNewStream(true);
        stream.flushToFile();
        for (int i = 0; i < 7; ++i) {
            int numBytes = rnd.nextInt(16);
            expectedPos += numBytes;
            stream.write(new byte[numBytes]);
            assertThat(stream.getPos()).isEqualTo(expectedPos);
        }

        stream.closeAndGetHandle();
    }

    @Test
    public void testCannotReuseClosedFile() throws IOException {
        FileMergingCheckpointStateOutputStream stream = getNewStream();
        stream.flushToFile();
        assertThat(isPhysicalFileProvided).isTrue();

        stream.close();
        stream = getNewStream(true);
        try {
            stream.flushToFile();
            fail("Cannot reuse a closed physical file.");
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testWriteFailsFastWhenClosed() throws Exception {
        FileMergingCheckpointStateOutputStream stream = getNewStream();
        stream.flushToFile();
        assertThat(isPhysicalFileProvided).isTrue();

        stream.close();
        try {
            stream.write(1);
            fail("Cannot reuse a closed physical file.");
        } catch (IOException ignored) {
            // expected
        }
    }

    private void runTest(int numBytes) throws Exception {
        CheckpointStateOutputStream stream = getNewStream();

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
        assertThat(handle).isNotNull();

        // make sure the writing process did not alter the original byte array
        assertThat(bytes).containsExactly(original);

        try (InputStream inStream = handle.openInputStream()) {
            byte[] validation = new byte[bytes.length];

            DataInputStream dataInputStream = new DataInputStream(inStream);
            dataInputStream.readFully(validation);

            assertThat(validation).containsExactly(bytes);
        }

        handle.discardState();
    }
}
