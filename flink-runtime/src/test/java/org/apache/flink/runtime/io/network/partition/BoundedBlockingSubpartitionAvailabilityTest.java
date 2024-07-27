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

import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createView;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the availability handling of the BoundedBlockingSubpartitions with not constant
 * availability.
 */
class BoundedBlockingSubpartitionAvailabilityTest {

    @TempDir private static Path tmpFolder;

    private static final int BUFFER_SIZE = 32 * 1024;

    @Test
    void testInitiallyNotAvailable() throws Exception {
        final ResultSubpartition subpartition = createPartitionWithData(10);
        final CountingAvailabilityListener listener = new CountingAvailabilityListener();

        // test
        final ResultSubpartitionView subpartitionView = createView(subpartition, listener);

        // assert
        assertThat(listener.numNotifications).isZero();

        // cleanup
        subpartitionView.releaseAllResources();
        subpartition.release();
    }

    @Test
    void testUnavailableWhenBuffersExhausted() throws Exception {
        // setup
        final ResultSubpartition subpartition = createPartitionWithData(100_000);
        final CountingAvailabilityListener listener = new CountingAvailabilityListener();
        final ResultSubpartitionView reader = createView(subpartition, listener);

        // test
        final List<BufferAndBacklog> data = drainAvailableData(reader);

        // assert
        assertThat(reader.getAvailabilityAndBacklog(true).isAvailable()).isFalse();
        assertThat(data.get(data.size() - 1).isDataAvailable()).isFalse();

        // cleanup
        reader.releaseAllResources();
        subpartition.release();
    }

    @Test
    void testAvailabilityNotificationWhenBuffersReturn() throws Exception {
        // setup
        final ResultSubpartition subpartition = createPartitionWithData(100_000);
        final CountingAvailabilityListener listener = new CountingAvailabilityListener();
        final ResultSubpartitionView reader = createView(subpartition, listener);

        // test
        final List<ResultSubpartition.BufferAndBacklog> data = drainAvailableData(reader);
        data.get(0).buffer().recycleBuffer();
        data.get(1).buffer().recycleBuffer();

        // assert
        assertThat(reader.getAvailabilityAndBacklog(true).isAvailable()).isTrue();
        assertThat(listener.numNotifications).isOne();

        // cleanup
        reader.releaseAllResources();
        subpartition.release();
    }

    @Test
    void testNotAvailableWhenEmpty() throws Exception {
        // setup
        final ResultSubpartition subpartition = createPartitionWithData(100_000);
        final ResultSubpartitionView reader =
                subpartition.createReadView(new NoOpBufferAvailablityListener());

        // test
        drainAllData(reader);

        // assert
        assertThat(reader.getAvailabilityAndBacklog(true).isAvailable()).isFalse();

        // cleanup
        reader.releaseAllResources();
        subpartition.release();
    }

    // ------------------------------------------------------------------------

    private static ResultSubpartition createPartitionWithData(int numberOfBuffers)
            throws IOException {
        BoundedBlockingResultPartition parent =
                (BoundedBlockingResultPartition)
                        new ResultPartitionBuilder()
                                .setResultPartitionType(ResultPartitionType.BLOCKING_PERSISTENT)
                                .setBoundedBlockingSubpartitionType(
                                        BoundedBlockingSubpartitionType.FILE)
                                .setSSLEnabled(true)
                                .setFileChannelManager(
                                        new FileChannelManagerImpl(
                                                new String[] {
                                                    TempDirUtils.newFolder(tmpFolder)
                                                            .getAbsolutePath()
                                                },
                                                "data"))
                                .setNetworkBufferSize(BUFFER_SIZE)
                                .build();

        ResultSubpartition partition = parent.getAllPartitions()[0];

        writeBuffers(partition, numberOfBuffers);
        partition.finish();

        return partition;
    }

    private static void writeBuffers(ResultSubpartition partition, int numberOfBuffers)
            throws IOException {
        for (int i = 0; i < numberOfBuffers; i++) {
            partition.add(BufferBuilderTestUtils.createFilledFinishedBufferConsumer(BUFFER_SIZE));
        }
    }

    private static List<BufferAndBacklog> drainAvailableData(ResultSubpartitionView reader)
            throws Exception {
        final ArrayList<BufferAndBacklog> list = new ArrayList<>();

        BufferAndBacklog bab;
        while ((bab = reader.getNextBuffer()) != null) {
            list.add(bab);
        }

        return list;
    }

    private static void drainAllData(ResultSubpartitionView reader) throws Exception {
        BufferAndBacklog bab;
        while ((bab = reader.getNextBuffer()) != null) {
            bab.buffer().recycleBuffer();
        }
    }
}
