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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavior tests for the {@link BoundedBlockingSubpartition} and the {@link
 * BoundedBlockingSubpartitionReader}.
 *
 * <p>Full read / write tests for the partition and the reader are in {@link
 * BoundedBlockingSubpartitionWriteReadTest}.
 */
@ExtendWith(ParameterizedTestExtension.class)
class BoundedBlockingSubpartitionTest extends SubpartitionTestBase {

    private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();

    private static FileChannelManager fileChannelManager;

    private final BoundedBlockingSubpartitionType type;

    private final boolean sslEnabled;

    @Parameters(name = "type = {0}, sslEnabled = {1}")
    private static List<Object[]> parameters() {
        return Arrays.stream(BoundedBlockingSubpartitionType.values())
                .map((type) -> new Object[][] {{type, true}, {type, false}})
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }

    @TempDir private Path tmpFolder;

    @BeforeAll
    static void setUp() {
        fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
    }

    @AfterAll
    static void shutdown() throws Exception {
        fileChannelManager.close();
    }

    BoundedBlockingSubpartitionTest(BoundedBlockingSubpartitionType type, boolean sslEnabled) {
        this.type = type;
        this.sslEnabled = sslEnabled;
    }

    // ------------------------------------------------------------------------

    @TestTemplate
    void testCreateReaderBeforeFinished() throws Exception {
        final ResultSubpartition partition = createSubpartition();

        assertThatThrownBy(() -> partition.createReadView(new NoOpBufferAvailablityListener()))
                .isInstanceOf(IllegalStateException.class);

        partition.release();
    }

    @TestTemplate
    void testCloseBoundedData() throws Exception {
        final TestingBoundedDataReader reader = new TestingBoundedDataReader();
        final TestingBoundedData data = new TestingBoundedData(reader);
        final BoundedBlockingSubpartitionReader bbspr =
                new BoundedBlockingSubpartitionReader(
                        (BoundedBlockingSubpartition) createSubpartition(),
                        data,
                        10,
                        new NoOpBufferAvailablityListener());

        bbspr.releaseAllResources();

        assertThat(reader.closed).isTrue();
    }

    @TestTemplate
    void testRecycleCurrentBufferOnFailure(BoundedBlockingSubpartitionType type, boolean sslEnabled)
            throws Exception {
        final ResultPartition resultPartition =
                createPartition(ResultPartitionType.BLOCKING, fileChannelManager);
        final BoundedBlockingSubpartition subpartition =
                new BoundedBlockingSubpartition(
                        0,
                        resultPartition,
                        new FailingBoundedData(),
                        !sslEnabled && type == BoundedBlockingSubpartitionType.FILE);
        final BufferConsumer consumer =
                BufferBuilderTestUtils.createFilledFinishedBufferConsumer(100);

        try {
            subpartition.add(consumer);
            assertThatThrownBy(
                    () -> subpartition.createReadView(new NoOpBufferAvailablityListener()));

            assertThat(consumer.isRecycled()).isFalse();

            assertThat(subpartition.getCurrentBuffer()).isNotNull();
            assertThat(subpartition.getCurrentBuffer().isRecycled()).isFalse();
        } finally {
            subpartition.release();

            assertThat(consumer.isRecycled()).isTrue();

            assertThat(subpartition.getCurrentBuffer()).isNull();
        }
    }

    // ------------------------------------------------------------------------

    @Override
    ResultSubpartition createSubpartition() throws Exception {
        final ResultPartition resultPartition =
                createPartition(ResultPartitionType.BLOCKING, fileChannelManager);
        return type.create(
                0,
                resultPartition,
                new File(tmpFolder.toFile(), "subpartition"),
                BufferBuilderTestUtils.BUFFER_SIZE,
                sslEnabled);
    }

    @Override
    ResultSubpartition createFailingWritesSubpartition() throws Exception {
        final ResultPartition resultPartition =
                createPartition(ResultPartitionType.BLOCKING, fileChannelManager);

        return new BoundedBlockingSubpartition(
                0,
                resultPartition,
                new FailingBoundedData(),
                !sslEnabled && type == BoundedBlockingSubpartitionType.FILE);
    }

    // ------------------------------------------------------------------------

    private static class FailingBoundedData implements BoundedData {

        @Override
        public void writeBuffer(Buffer buffer) throws IOException {
            throw new IOException("test");
        }

        @Override
        public void finishWrite() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Reader createReader(ResultSubpartitionView subpartitionView) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getFilePath() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    private static class TestingBoundedData implements BoundedData {

        private BoundedData.Reader reader;

        private TestingBoundedData(BoundedData.Reader reader) {
            this.reader = checkNotNull(reader);
        }

        @Override
        public void writeBuffer(Buffer buffer) throws IOException {}

        @Override
        public void finishWrite() throws IOException {}

        @Override
        public Reader createReader(ResultSubpartitionView ignored) throws IOException {
            return reader;
        }

        @Override
        public long getSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getFilePath() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    private static class TestingBoundedDataReader implements BoundedData.Reader {

        boolean closed;

        @Nullable
        @Override
        public Buffer nextBuffer() throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
