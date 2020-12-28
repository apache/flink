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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Behavior tests for the {@link BoundedBlockingSubpartition} and the {@link
 * BoundedBlockingSubpartitionReader}.
 *
 * <p>Full read / write tests for the partition and the reader are in {@link
 * BoundedBlockingSubpartitionWriteReadTest}.
 */
@RunWith(Parameterized.class)
public class BoundedBlockingSubpartitionTest extends SubpartitionTestBase {

    private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();

    private static FileChannelManager fileChannelManager;

    private final BoundedBlockingSubpartitionType type;

    private final boolean sslEnabled;

    @Parameterized.Parameters(name = "type = {0}, sslEnabled = {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.stream(BoundedBlockingSubpartitionType.values())
                .map((type) -> new Object[][] {{type, true}, {type, false}})
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }

    @ClassRule public static final TemporaryFolder TMP_DIR = new TemporaryFolder();

    @BeforeClass
    public static void setUp() {
        fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
    }

    @AfterClass
    public static void shutdown() throws Exception {
        fileChannelManager.close();
    }

    public BoundedBlockingSubpartitionTest(
            BoundedBlockingSubpartitionType type, boolean sslEnabled) {
        this.type = type;
        this.sslEnabled = sslEnabled;
    }

    // ------------------------------------------------------------------------

    @Test
    public void testCreateReaderBeforeFinished() throws Exception {
        final ResultSubpartition partition = createSubpartition();

        try {
            partition.createReadView(new NoOpBufferAvailablityListener());
            fail("expected exception");
        } catch (IllegalStateException ignored) {
        }

        partition.release();
    }

    @Test
    public void testCloseBoundedData() throws Exception {
        final TestingBoundedDataReader reader = new TestingBoundedDataReader();
        final TestingBoundedData data = new TestingBoundedData(reader);
        final BoundedBlockingSubpartitionReader bbspr =
                new BoundedBlockingSubpartitionReader(
                        (BoundedBlockingSubpartition) createSubpartition(),
                        data,
                        10,
                        new NoOpBufferAvailablityListener());

        bbspr.releaseAllResources();

        assertTrue(reader.closed);
    }

    @Test
    public void testRecycleCurrentBufferOnFailure() throws Exception {
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
            try {
                subpartition.add(consumer);
                subpartition.createReadView(new NoOpBufferAvailablityListener());
                fail("should fail with an exception");
            } catch (Exception ignored) {
                // expected
            }

            assertFalse(consumer.isRecycled());

            assertNotNull(subpartition.getCurrentBuffer());
            assertFalse(subpartition.getCurrentBuffer().isRecycled());
        } finally {
            subpartition.release();

            assertTrue(consumer.isRecycled());

            assertNull(subpartition.getCurrentBuffer());
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
                new File(TMP_DIR.newFolder(), "subpartition"),
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
