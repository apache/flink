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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuplicatingCheckpointOutputStreamTest {

    /**
     * Test that all writes are duplicated to both streams and that the state reflects what was
     * written.
     */
    @Test
    void testDuplicatedWrite() throws Exception {
        int streamCapacity = 1024 * 1024;
        TestMemoryCheckpointOutputStream primaryStream =
                new TestMemoryCheckpointOutputStream(streamCapacity);
        TestMemoryCheckpointOutputStream secondaryStream =
                new TestMemoryCheckpointOutputStream(streamCapacity);
        TestMemoryCheckpointOutputStream referenceStream =
                new TestMemoryCheckpointOutputStream(streamCapacity);
        DuplicatingCheckpointOutputStream duplicatingStream =
                new DuplicatingCheckpointOutputStream(primaryStream, secondaryStream, 64);
        Random random = new Random(42);
        for (int i = 0; i < 500; ++i) {
            int choice = random.nextInt(3);
            if (choice == 0) {
                int val = random.nextInt();
                referenceStream.write(val);
                duplicatingStream.write(val);
            } else {
                byte[] bytes = new byte[random.nextInt(128)];
                random.nextBytes(bytes);
                if (choice == 1) {
                    referenceStream.write(bytes);
                    duplicatingStream.write(bytes);
                } else {
                    int off = bytes.length > 0 ? random.nextInt(bytes.length) : 0;
                    int len = bytes.length > 0 ? random.nextInt(bytes.length - off) : 0;
                    referenceStream.write(bytes, off, len);
                    duplicatingStream.write(bytes, off, len);
                }
            }
            assertThat(duplicatingStream.getPos()).isEqualTo(referenceStream.getPos());
        }

        StreamStateHandle refStateHandle = referenceStream.closeAndGetHandle();
        StreamStateHandle primaryStateHandle = duplicatingStream.closeAndGetPrimaryHandle();
        StreamStateHandle secondaryStateHandle = duplicatingStream.closeAndGetSecondaryHandle();

        assertThat(
                        CommonTestUtils.isStreamContentEqual(
                                refStateHandle.openInputStream(),
                                primaryStateHandle.openInputStream()))
                .isTrue();

        assertThat(
                        CommonTestUtils.isStreamContentEqual(
                                refStateHandle.openInputStream(),
                                secondaryStateHandle.openInputStream()))
                .isTrue();

        refStateHandle.discardState();
        primaryStateHandle.discardState();
        secondaryStateHandle.discardState();
    }

    /**
     * This is the first of a set of tests that check that exceptions from the secondary stream do
     * not impact that we can create a result for the first stream.
     */
    @Test
    void testSecondaryWriteFail() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingSecondary();
        testFailingSecondaryStream(
                duplicatingStream,
                () -> {
                    for (int i = 0; i < 128; i++) {
                        duplicatingStream.write(42);
                    }
                });
    }

    @Test
    void testFailingSecondaryWriteArrayFail() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingSecondary();
        testFailingSecondaryStream(duplicatingStream, () -> duplicatingStream.write(new byte[512]));
    }

    @Test
    void testFailingSecondaryWriteArrayOffsFail() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingSecondary();
        testFailingSecondaryStream(
                duplicatingStream, () -> duplicatingStream.write(new byte[512], 20, 130));
    }

    @Test
    void testFailingSecondaryFlush() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingSecondary();
        testFailingSecondaryStream(duplicatingStream, duplicatingStream::flush);
    }

    @Test
    void testFailingSecondarySync() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingSecondary();
        testFailingSecondaryStream(duplicatingStream, duplicatingStream::sync);
    }

    /**
     * This is the first of a set of tests that check that exceptions from the primary stream are
     * immediately reported.
     */
    @Test
    void testPrimaryWriteFail() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingPrimary();
        testFailingPrimaryStream(
                duplicatingStream,
                () -> {
                    for (int i = 0; i < 128; i++) {
                        duplicatingStream.write(42);
                    }
                });
    }

    @Test
    void testFailingPrimaryWriteArrayFail() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingPrimary();
        testFailingPrimaryStream(duplicatingStream, () -> duplicatingStream.write(new byte[512]));
    }

    @Test
    void testFailingPrimaryWriteArrayOffsFail() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingPrimary();
        testFailingPrimaryStream(
                duplicatingStream, () -> duplicatingStream.write(new byte[512], 20, 130));
    }

    @Test
    void testFailingPrimaryFlush() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingPrimary();
        testFailingPrimaryStream(duplicatingStream, duplicatingStream::flush);
    }

    @Test
    void testFailingPrimarySync() throws Exception {
        DuplicatingCheckpointOutputStream duplicatingStream =
                createDuplicatingStreamWithFailingPrimary();
        testFailingPrimaryStream(duplicatingStream, duplicatingStream::sync);
    }

    /**
     * Tests that an exception from interacting with the secondary stream does not effect
     * duplicating to the primary stream, but is reflected later when we want the secondary state
     * handle.
     */
    private void testFailingSecondaryStream(
            DuplicatingCheckpointOutputStream duplicatingStream, StreamTestMethod testMethod)
            throws Exception {

        testMethod.call();

        duplicatingStream.write(42);

        FailingCheckpointOutStream secondary =
                (FailingCheckpointOutStream) duplicatingStream.getSecondaryOutputStream();

        assertThat(secondary.isClosed()).isTrue();

        long pos = duplicatingStream.getPos();
        StreamStateHandle primaryHandle = duplicatingStream.closeAndGetPrimaryHandle();

        if (primaryHandle != null) {
            assertThat(primaryHandle.getStateSize()).isEqualTo(pos);
        }

        assertThatThrownBy(duplicatingStream::closeAndGetSecondaryHandle)
                .isInstanceOf(IOException.class)
                .hasCause(duplicatingStream.getSecondaryStreamException());
    }

    /** Test that a failing primary stream brings up an exception. */
    private void testFailingPrimaryStream(
            DuplicatingCheckpointOutputStream duplicatingStream, StreamTestMethod testMethod)
            throws Exception {
        try {
            assertThatThrownBy(testMethod::call).isInstanceOf(IOException.class);
        } finally {
            IOUtils.closeQuietly(duplicatingStream);
        }
    }

    /**
     * Tests that in case of unaligned stream positions, the secondary stream is closed and the
     * primary still works. This is important because some code may rely on seeking to stream
     * offsets in the created state files and if the streams are not aligned this code could fail.
     */
    @Test
    void testUnalignedStreamsException() throws IOException {
        int streamCapacity = 1024 * 1024;
        TestMemoryCheckpointOutputStream primaryStream =
                new TestMemoryCheckpointOutputStream(streamCapacity);
        TestMemoryCheckpointOutputStream secondaryStream =
                new TestMemoryCheckpointOutputStream(streamCapacity);

        primaryStream.write(42);

        DuplicatingCheckpointOutputStream stream =
                new DuplicatingCheckpointOutputStream(primaryStream, secondaryStream);

        assertThat(stream.getSecondaryStreamException()).isNotNull();
        assertThat(secondaryStream.isClosed()).isTrue();

        stream.write(23);

        assertThatThrownBy(stream::closeAndGetSecondaryHandle)
                .isInstanceOf(IOException.class)
                .hasCause(stream.getSecondaryStreamException());

        StreamStateHandle primaryHandle = stream.closeAndGetPrimaryHandle();

        try (FSDataInputStream inputStream = primaryHandle.openInputStream(); ) {
            assertThat(inputStream.read()).isEqualTo(42);
            assertThat(inputStream.read()).isEqualTo(23);
            assertThat(inputStream.read()).isEqualTo(-1);
        }
    }

    /** Helper */
    private DuplicatingCheckpointOutputStream createDuplicatingStreamWithFailingSecondary()
            throws IOException {
        int streamCapacity = 1024 * 1024;
        TestMemoryCheckpointOutputStream primaryStream =
                new TestMemoryCheckpointOutputStream(streamCapacity);
        FailingCheckpointOutStream failSecondaryStream = new FailingCheckpointOutStream();
        return new DuplicatingCheckpointOutputStream(primaryStream, failSecondaryStream, 64);
    }

    private DuplicatingCheckpointOutputStream createDuplicatingStreamWithFailingPrimary()
            throws IOException {
        int streamCapacity = 1024 * 1024;
        FailingCheckpointOutStream failPrimaryStream = new FailingCheckpointOutStream();
        TestMemoryCheckpointOutputStream secondary =
                new TestMemoryCheckpointOutputStream(streamCapacity);
        return new DuplicatingCheckpointOutputStream(failPrimaryStream, secondary, 64);
    }

    /** Stream that throws {@link IOException} on all relevant methods under test. */
    private static class FailingCheckpointOutStream extends CheckpointStateOutputStream {

        private boolean closed = false;

        @Nullable
        @Override
        public StreamStateHandle closeAndGetHandle() throws IOException {
            throw new IOException();
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public void write(int b) throws IOException {
            throw new IOException();
        }

        @Override
        public void flush() throws IOException {
            throw new IOException();
        }

        @Override
        public void sync() throws IOException {
            throw new IOException();
        }

        @Override
        public void close() throws IOException {
            this.closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    @FunctionalInterface
    private interface StreamTestMethod {
        void call() throws IOException;
    }
}
