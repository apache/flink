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

import org.apache.flink.api.common.io.InputStreamFSInputWrapper;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompressibleFSDataInputStream}. */
class CompressibleFSDataInputStreamTest {

    private static class TestingOutputStream extends FSDataOutputStream {

        private final ByteArrayOutputStreamWithPos delegate = new ByteArrayOutputStreamWithPos();

        @Override
        public long getPos() {
            return delegate.getPosition();
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void sync() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void write(int b) {
            delegate.write(b);
        }

        byte[] toByteArray() {
            return delegate.toByteArray();
        }
    }

    private static void verifyRecord(
            FSDataInputStream inputStream, Map<String, Long> positions, String record)
            throws IOException {
        inputStream.seek(Objects.requireNonNull(positions.get(record)));
        final byte[] readBuffer = new byte[record.getBytes(StandardCharsets.UTF_8).length];
        for (int i = 0; i < readBuffer.length; ++i) {
            readBuffer[i] = (byte) inputStream.read();
        }
        assertThat(readBuffer).asString(StandardCharsets.UTF_8).isEqualTo(record);
    }

    private static void verifyRecordPrefix(
            FSDataInputStream inputStream,
            Map<String, Long> positions,
            String record,
            String prefix)
            throws IOException {
        assertThat(record).startsWith(prefix);
        inputStream.seek(Objects.requireNonNull(positions.get(record)));
        final byte[] readBuffer = new byte[prefix.getBytes(StandardCharsets.UTF_8).length];
        for (int i = 0; i < readBuffer.length; ++i) {
            readBuffer[i] = (byte) inputStream.read();
        }
        assertThat(readBuffer).asString(StandardCharsets.UTF_8).isEqualTo(prefix);
    }

    private static Stream<Arguments> compressionDecorators() {
        return Stream.of(
                Arguments.of(new UncompressedStreamCompressionDecorator()),
                Arguments.of(new SnappyStreamCompressionDecorator()));
    }

    @ParameterizedTest
    @MethodSource("compressionDecorators")
    void testSeek(StreamCompressionDecorator streamCompressionDecorator) throws IOException {
        final List<String> records = Arrays.asList("first", "second", "third", "fourth", "fifth");
        final Map<String, Long> positions = new HashMap<>();

        byte[] compressedBytes;
        try (final TestingOutputStream outputStream = new TestingOutputStream();
                final CompressibleFSDataOutputStream compressibleOutputStream =
                        new CompressibleFSDataOutputStream(
                                outputStream, streamCompressionDecorator)) {
            for (String record : records) {
                positions.put(record, compressibleOutputStream.getPos());
                compressibleOutputStream.write(record.getBytes(StandardCharsets.UTF_8));
            }
            compressibleOutputStream.flush();
            compressedBytes = outputStream.toByteArray();
        }

        try (final FSDataInputStream inputStream =
                        new InputStreamFSInputWrapper(new ByteArrayInputStream(compressedBytes));
                final FSDataInputStream compressibleInputStream =
                        new CompressibleFSDataInputStream(
                                inputStream, streamCompressionDecorator)) {
            verifyRecord(compressibleInputStream, positions, "first");
            verifyRecord(compressibleInputStream, positions, "third");
            verifyRecord(compressibleInputStream, positions, "fifth");
        }

        // Verify read of partial records. This ensures that we skip any unread data in the
        // underlying buffers.
        try (final FSDataInputStream inputStream =
                        new InputStreamFSInputWrapper(new ByteArrayInputStream(compressedBytes));
                final FSDataInputStream compressibleInputStream =
                        new CompressibleFSDataInputStream(
                                inputStream, streamCompressionDecorator)) {
            verifyRecordPrefix(compressibleInputStream, positions, "first", "fir");
            verifyRecordPrefix(compressibleInputStream, positions, "third", "thi");
            verifyRecord(compressibleInputStream, positions, "fifth");
        }
    }

    @ParameterizedTest
    @MethodSource("compressionDecorators")
    void testBulkReadIsForwardedToDelegate(StreamCompressionDecorator streamCompressionDecorator)
            throws IOException {
        final byte[] record = new byte[64 * 1024];
        new Random(42).nextBytes(record);

        byte[] storedBytes;
        try (final TestingOutputStream outputStream = new TestingOutputStream();
                final CompressibleFSDataOutputStream compressibleOutputStream =
                        new CompressibleFSDataOutputStream(
                                outputStream, streamCompressionDecorator)) {
            compressibleOutputStream.write(record);
            compressibleOutputStream.flush();
            storedBytes = outputStream.toByteArray();
        }

        final AtomicInteger singleByteReads = new AtomicInteger();
        try (final FSDataInputStream inputStream =
                        new InputStreamFSInputWrapper(new ByteArrayInputStream(storedBytes)) {
                            @Override
                            public int read() throws IOException {
                                singleByteReads.incrementAndGet();
                                return super.read();
                            }
                        };
                final FSDataInputStream compressibleInputStream =
                        new CompressibleFSDataInputStream(
                                inputStream, streamCompressionDecorator)) {
            final byte[] readBuffer = new byte[record.length];
            int offset = 0;
            while (offset < readBuffer.length) {
                final int read =
                        compressibleInputStream.read(
                                readBuffer, offset, readBuffer.length - offset);
                assertThat(read).isPositive();
                offset += read;
            }
            assertThat(readBuffer).isEqualTo(record);
        }
        assertThat(singleByteReads).hasValue(0);
    }
}
