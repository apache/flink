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
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.util.EnvironmentInformation;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;

import java.io.IOException;
import java.nio.file.Path;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSomeBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createView;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that read the BoundedBlockingSubpartition with multiple threads in parallel. */
class FileChannelBoundedDataTest extends BoundedDataTestBase {

    private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();

    private static FileChannelManager fileChannelManager;

    @BeforeAll
    static void setUp() {
        fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
    }

    @AfterAll
    static void shutdown() throws Exception {
        fileChannelManager.close();
    }

    @Override
    protected boolean isRegionBased() {
        return false;
    }

    @Override
    protected BoundedData createBoundedData(Path tempFilePath) throws IOException {
        return FileChannelBoundedData.create(tempFilePath, BUFFER_SIZE);
    }

    @Override
    protected BoundedData createBoundedDataWithRegion(Path tempFilePath, int regionSize) {
        throw new UnsupportedOperationException();
    }

    @TestTemplate
    void testReadNextBuffer() throws Exception {
        final int numberOfBuffers = 3;
        try (final BoundedData data = createBoundedData()) {
            writeBuffers(data, numberOfBuffers);

            final BoundedData.Reader reader = data.createReader();
            final Buffer buffer1 = reader.nextBuffer();
            final Buffer buffer2 = reader.nextBuffer();

            assertThat(buffer1).isNotNull();
            assertThat(buffer2).isNotNull();
            // there are only two available memory segments for reading data
            assertThat(reader.nextBuffer()).isNull();

            // cleanup
            buffer1.recycleBuffer();
            buffer2.recycleBuffer();
        }
    }

    @TestTemplate
    void testRecycleBufferForNotifyingSubpartitionView() throws Exception {
        final int numberOfBuffers = 2;
        try (final BoundedData data = createBoundedData()) {
            writeBuffers(data, numberOfBuffers);

            final VerifyNotificationResultSubpartitionView subpartitionView =
                    new VerifyNotificationResultSubpartitionView();
            final BoundedData.Reader reader = data.createReader(subpartitionView);
            final Buffer buffer1 = reader.nextBuffer();
            final Buffer buffer2 = reader.nextBuffer();
            assertThat(buffer1).isNotNull();
            assertThat(buffer2).isNotNull();

            assertThat(subpartitionView.isAvailable).isFalse();
            buffer1.recycleBuffer();
            // the view is notified while recycling buffer if reader has not tagged finished
            assertThat(subpartitionView.isAvailable).isTrue();

            subpartitionView.resetAvailable();
            assertThat(subpartitionView.isAvailable).isFalse();

            // the next buffer is null to make reader tag finished
            assertThat(reader.nextBuffer()).isNull();

            buffer2.recycleBuffer();
            // the view is not notified while recycling buffer if reader already finished
            assertThat(subpartitionView.isAvailable).isFalse();
        }
    }

    @TestTemplate
    void testRecycleBufferForNotifyingBufferAvailabilityListener() throws Exception {
        final ResultSubpartition subpartition = createFileBoundedBlockingSubpartition();
        final int numberOfBuffers = 2;
        writeBuffers(subpartition, numberOfBuffers);

        final VerifyNotificationBufferAvailabilityListener listener =
                new VerifyNotificationBufferAvailabilityListener();
        final ResultSubpartitionView subpartitionView = createView(subpartition, listener);
        assertThat(listener.isAvailable).isFalse();

        final BufferAndBacklog buffer1 = subpartitionView.getNextBuffer();
        final BufferAndBacklog buffer2 = subpartitionView.getNextBuffer();
        assertThat(buffer1).isNotNull();
        assertThat(buffer2).isNotNull();

        // the next buffer is null in view because FileBufferReader has no available buffers for
        // reading ahead
        assertThat(subpartitionView.getAvailabilityAndBacklog(true).isAvailable()).isFalse();
        // recycle a buffer to trigger notification of data available
        buffer1.buffer().recycleBuffer();
        assertThat(listener.isAvailable).isTrue();

        // cleanup
        buffer2.buffer().recycleBuffer();
        subpartitionView.releaseAllResources();
        subpartition.release();
    }

    private static ResultSubpartition createFileBoundedBlockingSubpartition() {
        final BoundedBlockingResultPartition resultPartition =
                (BoundedBlockingResultPartition)
                        new ResultPartitionBuilder()
                                .setNetworkBufferSize(BUFFER_SIZE)
                                .setResultPartitionType(ResultPartitionType.BLOCKING)
                                .setBoundedBlockingSubpartitionType(
                                        BoundedBlockingSubpartitionType.FILE)
                                .setFileChannelManager(fileChannelManager)
                                .setSSLEnabled(true)
                                .build();
        return resultPartition.subpartitions[0];
    }

    private static void writeBuffers(BoundedData data, int numberOfBuffers) throws IOException {
        for (int i = 0; i < numberOfBuffers; i++) {
            data.writeBuffer(buildSomeBuffer(BUFFER_SIZE));
        }
        data.finishWrite();
    }

    private static void writeBuffers(ResultSubpartition subpartition, int numberOfBuffers)
            throws IOException {
        for (int i = 0; i < numberOfBuffers; i++) {
            subpartition.add(createFilledFinishedBufferConsumer(BUFFER_SIZE));
        }
        subpartition.finish();
    }

    /**
     * This subpartition view is used for verifying the {@link
     * ResultSubpartitionView#notifyDataAvailable()} was ever called before.
     */
    private static class VerifyNotificationResultSubpartitionView
            extends NoOpResultSubpartitionView {

        private boolean isAvailable;

        @Override
        public void notifyDataAvailable() {
            isAvailable = true;
        }

        private void resetAvailable() {
            isAvailable = false;
        }
    }

    /**
     * This listener is used for verifying the notification logic in {@link
     * ResultSubpartitionView#notifyDataAvailable()}.
     */
    private static class VerifyNotificationBufferAvailabilityListener
            implements BufferAvailabilityListener {

        private boolean isAvailable;

        @Override
        public void notifyDataAvailable(ResultSubpartitionView view) {
            isAvailable = true;
        }

        private void resetAvailable() {
            isAvailable = false;
        }
    }
}
