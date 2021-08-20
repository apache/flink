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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.taskmanager.ConsumableNotifyingResultPartitionWriterDecorator;
import org.apache.flink.runtime.taskmanager.NoOpTaskActions;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.concurrent.FutureConsumerWithException;

import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.verifyCreateSubpartitionViewThrowsException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ResultPartition}. */
public class ResultPartitionTest {

    private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();

    private static FileChannelManager fileChannelManager;

    private final int bufferSize = 1024;

    @BeforeClass
    public static void setUp() {
        fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
    }

    @AfterClass
    public static void shutdown() throws Exception {
        fileChannelManager.close();
    }

    @Test
    public void testResultSubpartitionInfo() {
        final int numPartitions = 2;
        final int numSubpartitions = 3;

        for (int i = 0; i < numPartitions; i++) {
            final PipelinedResultPartition partition =
                    (PipelinedResultPartition)
                            new ResultPartitionBuilder()
                                    .setResultPartitionIndex(i)
                                    .setNumberOfSubpartitions(numSubpartitions)
                                    .build();

            ResultSubpartition[] subpartitions = partition.getAllPartitions();
            for (int j = 0; j < subpartitions.length; j++) {
                ResultSubpartitionInfo subpartitionInfo = subpartitions[j].getSubpartitionInfo();

                assertEquals(i, subpartitionInfo.getPartitionIdx());
                assertEquals(j, subpartitionInfo.getSubPartitionIdx());
            }
        }
    }

    /** Tests notifyPartitionDataAvailable behaviour depending on the relevant flags. */
    @Test
    public void testNotifyPartitionDataAvailable() throws Exception {
        FutureConsumerWithException[] notificationCalls =
                new FutureConsumerWithException[] {
                    writer -> ((ResultPartitionWriter) writer).finish(),
                    writer ->
                            ((ResultPartitionWriter) writer)
                                    .emitRecord(ByteBuffer.allocate(bufferSize), 0),
                    writer ->
                            ((ResultPartitionWriter) writer)
                                    .broadcastEvent(EndOfPartitionEvent.INSTANCE, false),
                    writer ->
                            ((ResultPartitionWriter) writer)
                                    .broadcastRecord(ByteBuffer.allocate(bufferSize))
                };

        for (FutureConsumerWithException notificationCall : notificationCalls) {
            testNotifyPartitionDataAvailable(notificationCall);
        }
    }

    private void testNotifyPartitionDataAvailable(
            FutureConsumerWithException<ResultPartitionWriter, Exception> notificationCall)
            throws Exception {
        JobID jobId = new JobID();
        TaskActions taskActions = new NoOpTaskActions();

        {
            // Pipelined, send message => notify
            TestResultPartitionConsumableNotifier notifier =
                    new TestResultPartitionConsumableNotifier();
            ResultPartitionWriter consumableNotifyingPartitionWriter =
                    createConsumableNotifyingResultPartitionWriter(
                            ResultPartitionType.PIPELINED, taskActions, jobId, notifier);
            notificationCall.accept(consumableNotifyingPartitionWriter);
            notifier.check(
                    jobId, consumableNotifyingPartitionWriter.getPartitionId(), taskActions, 1);
        }

        {
            // Blocking, send message => don't notify
            TestResultPartitionConsumableNotifier notifier =
                    new TestResultPartitionConsumableNotifier();
            ResultPartitionWriter partition =
                    createConsumableNotifyingResultPartitionWriter(
                            ResultPartitionType.BLOCKING, taskActions, jobId, notifier);
            notificationCall.accept(partition);
            notifier.check(null, null, null, 0);
        }
    }

    @Test
    public void testAddOnFinishedPipelinedPartition() throws Exception {
        testAddOnFinishedPartition(ResultPartitionType.PIPELINED);
    }

    @Test
    public void testAddOnFinishedBlockingPartition() throws Exception {
        testAddOnFinishedPartition(ResultPartitionType.BLOCKING);
    }

    @Test
    public void testBlockingPartitionIsConsumableMultipleTimesIfNotReleasedOnConsumption()
            throws IOException {
        ResultPartitionManager manager = new ResultPartitionManager();

        final ResultPartition partition =
                new ResultPartitionBuilder()
                        .setResultPartitionManager(manager)
                        .setResultPartitionType(ResultPartitionType.BLOCKING)
                        .setFileChannelManager(fileChannelManager)
                        .build();

        manager.registerResultPartition(partition);
        partition.finish();

        assertThat(manager.getUnreleasedPartitions(), contains(partition.getPartitionId()));

        // a blocking partition that is not released on consumption should be consumable multiple
        // times
        for (int x = 0; x < 2; x++) {
            ResultSubpartitionView subpartitionView1 =
                    partition.createSubpartitionView(0, () -> {});
            subpartitionView1.releaseAllResources();

            // partition should not be released on consumption
            assertThat(manager.getUnreleasedPartitions(), contains(partition.getPartitionId()));
            assertFalse(partition.isReleased());
        }
    }

    /**
     * Tests {@link ResultPartition#emitRecord} on a partition which has already finished.
     *
     * @param partitionType the result partition type to set up
     */
    private void testAddOnFinishedPartition(final ResultPartitionType partitionType)
            throws Exception {
        TestResultPartitionConsumableNotifier notifier =
                new TestResultPartitionConsumableNotifier();
        BufferWritingResultPartition bufferWritingResultPartition =
                createResultPartition(partitionType);
        ResultPartitionWriter partitionWriter =
                ConsumableNotifyingResultPartitionWriterDecorator.decorate(
                        Collections.singleton(
                                PartitionTestUtils.createPartitionDeploymentDescriptor(
                                        partitionType)),
                        new ResultPartitionWriter[] {bufferWritingResultPartition},
                        new NoOpTaskActions(),
                        new JobID(),
                        notifier)[0];
        try {
            partitionWriter.finish();
            notifier.reset();
            // partitionWriter.emitRecord() should fail
            partitionWriter.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        } catch (IllegalStateException e) {
            // expected => ignored
        } finally {
            assertEquals(0, bufferWritingResultPartition.numBuffersOut.getCount());
            assertEquals(0, bufferWritingResultPartition.numBytesOut.getCount());
            assertEquals(
                    0,
                    bufferWritingResultPartition.getBufferPool().bestEffortGetNumOfUsedBuffers());
            // should not have notified either
            notifier.check(null, null, null, 0);
        }
    }

    @Test
    public void testAddOnReleasedPipelinedPartition() throws Exception {
        testAddOnReleasedPartition(ResultPartitionType.PIPELINED);
    }

    @Test
    public void testAddOnReleasedBlockingPartition() throws Exception {
        testAddOnReleasedPartition(ResultPartitionType.BLOCKING);
    }

    /**
     * Tests {@link ResultPartition#emitRecord} on a partition which has already been released.
     *
     * @param partitionType the result partition type to set up
     */
    private void testAddOnReleasedPartition(ResultPartitionType partitionType) throws Exception {
        TestResultPartitionConsumableNotifier notifier =
                new TestResultPartitionConsumableNotifier();
        BufferWritingResultPartition bufferWritingResultPartition =
                createResultPartition(partitionType);
        ResultPartitionWriter partitionWriter =
                ConsumableNotifyingResultPartitionWriterDecorator.decorate(
                        Collections.singleton(
                                PartitionTestUtils.createPartitionDeploymentDescriptor(
                                        partitionType)),
                        new ResultPartitionWriter[] {bufferWritingResultPartition},
                        new NoOpTaskActions(),
                        new JobID(),
                        notifier)[0];
        try {
            partitionWriter.release(null);
            // partitionWriter.emitRecord() should silently drop the given record
            partitionWriter.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        } finally {
            assertEquals(1, bufferWritingResultPartition.numBuffersOut.getCount());
            assertEquals(bufferSize, bufferWritingResultPartition.numBytesOut.getCount());
            // the buffer should be recycled for the result partition has already been released
            assertEquals(
                    0,
                    bufferWritingResultPartition.getBufferPool().bestEffortGetNumOfUsedBuffers());
            // should not have notified either
            notifier.check(null, null, null, 0);
        }
    }

    @Test
    public void testAddOnPipelinedPartition() throws Exception {
        testAddOnPartition(ResultPartitionType.PIPELINED);
    }

    @Test
    public void testAddOnBlockingPartition() throws Exception {
        testAddOnPartition(ResultPartitionType.BLOCKING);
    }

    /**
     * Tests {@link ResultPartitionManager#createSubpartitionView(ResultPartitionID, int,
     * BufferAvailabilityListener)} would throw a {@link PartitionNotFoundException} if the
     * registered partition was released from manager via {@link ResultPartition#fail(Throwable)}
     * before.
     */
    @Test
    public void testCreateSubpartitionOnFailingPartition() throws Exception {
        final ResultPartitionManager manager = new ResultPartitionManager();
        final ResultPartition partition =
                new ResultPartitionBuilder().setResultPartitionManager(manager).build();

        manager.registerResultPartition(partition);

        partition.fail(null);

        verifyCreateSubpartitionViewThrowsException(manager, partition.getPartitionId());
    }

    /**
     * Tests {@link ResultPartition#emitRecord} on a working partition.
     *
     * @param partitionType the result partition type to set up
     */
    private void testAddOnPartition(final ResultPartitionType partitionType) throws Exception {
        TestResultPartitionConsumableNotifier notifier =
                new TestResultPartitionConsumableNotifier();
        JobID jobId = new JobID();
        TaskActions taskActions = new NoOpTaskActions();
        BufferWritingResultPartition bufferWritingResultPartition =
                createResultPartition(partitionType);
        ResultPartitionWriter partitionWriter =
                ConsumableNotifyingResultPartitionWriterDecorator.decorate(
                        Collections.singleton(
                                PartitionTestUtils.createPartitionDeploymentDescriptor(
                                        partitionType)),
                        new ResultPartitionWriter[] {bufferWritingResultPartition},
                        taskActions,
                        jobId,
                        notifier)[0];
        try {
            // partitionWriter.emitRecord() will allocate a new buffer and copies the record to it
            partitionWriter.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        } finally {
            assertEquals(1, bufferWritingResultPartition.numBuffersOut.getCount());
            assertEquals(bufferSize, bufferWritingResultPartition.numBytesOut.getCount());
            assertEquals(
                    1,
                    bufferWritingResultPartition.getBufferPool().bestEffortGetNumOfUsedBuffers());
            // should have been notified for pipelined partitions
            if (partitionType.isPipelined()) {
                notifier.check(jobId, partitionWriter.getPartitionId(), taskActions, 1);
            }
        }
    }

    /**
     * Tests {@link ResultPartition#close()} and {@link ResultPartition#release()} on a working
     * pipelined partition.
     */
    @Test
    public void testReleaseMemoryOnPipelinedPartition() throws Exception {
        final int numAllBuffers = 10;
        final NettyShuffleEnvironment network =
                new NettyShuffleEnvironmentBuilder()
                        .setNumNetworkBuffers(numAllBuffers)
                        .setBufferSize(bufferSize)
                        .build();
        final ResultPartition resultPartition =
                createPartition(network, ResultPartitionType.PIPELINED, 1);
        try {
            resultPartition.setup();

            // take all buffers (more than the minimum required)
            for (int i = 0; i < numAllBuffers; ++i) {
                resultPartition.emitRecord(ByteBuffer.allocate(bufferSize - 1), 0);
            }
            assertEquals(0, resultPartition.getBufferPool().getNumberOfAvailableMemorySegments());

            resultPartition.close();
            assertTrue(resultPartition.getBufferPool().isDestroyed());
            assertEquals(
                    numAllBuffers, network.getNetworkBufferPool().getNumberOfUsedMemorySegments());

            resultPartition.release();
            assertEquals(0, network.getNetworkBufferPool().getNumberOfUsedMemorySegments());
        } finally {
            network.close();
        }
    }

    /** Tests {@link ResultPartition#getAvailableFuture()}. */
    @Test
    public void testIsAvailableOrNot() throws IOException {
        final int numAllBuffers = 10;
        final int bufferSize = 1024;
        final NettyShuffleEnvironment network =
                new NettyShuffleEnvironmentBuilder()
                        .setNumNetworkBuffers(numAllBuffers)
                        .setBufferSize(bufferSize)
                        .build();
        final ResultPartition resultPartition =
                createPartition(network, ResultPartitionType.PIPELINED, 1);

        try {
            resultPartition.setup();

            resultPartition.getBufferPool().setNumBuffers(2);

            assertTrue(resultPartition.getAvailableFuture().isDone());

            resultPartition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
            resultPartition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
            assertFalse(resultPartition.getAvailableFuture().isDone());
        } finally {
            resultPartition.release();
            network.close();
        }
    }

    @Test
    public void testPipelinedPartitionBufferPool() throws Exception {
        testPartitionBufferPool(ResultPartitionType.PIPELINED_BOUNDED);
    }

    @Test
    public void testBlockingPartitionBufferPool() throws Exception {
        testPartitionBufferPool(ResultPartitionType.BLOCKING);
    }

    private void testPartitionBufferPool(ResultPartitionType type) throws Exception {
        // setup
        final int networkBuffersPerChannel = 2;
        final int floatingNetworkBuffersPerGate = 8;
        final NetworkBufferPool globalPool = new NetworkBufferPool(20, 1);
        final ResultPartition partition =
                new ResultPartitionBuilder()
                        .setResultPartitionType(type)
                        .setFileChannelManager(fileChannelManager)
                        .setNetworkBuffersPerChannel(networkBuffersPerChannel)
                        .setFloatingNetworkBuffersPerGate(floatingNetworkBuffersPerGate)
                        .setNetworkBufferPool(globalPool)
                        .build();

        try {
            partition.setup();
            BufferPool bufferPool = partition.getBufferPool();
            // verify the amount of buffers in created local pool
            assertEquals(
                    partition.getNumberOfSubpartitions() + 1,
                    bufferPool.getNumberOfRequiredMemorySegments());
            if (type.isBounded()) {
                final int maxNumBuffers =
                        networkBuffersPerChannel * partition.getNumberOfSubpartitions()
                                + floatingNetworkBuffersPerGate;
                assertEquals(maxNumBuffers, bufferPool.getMaxNumberOfMemorySegments());
            } else {
                assertEquals(Integer.MAX_VALUE, bufferPool.getMaxNumberOfMemorySegments());
            }

        } finally {
            // cleanup
            globalPool.destroyAllBufferPools();
            globalPool.destroy();
        }
    }

    private ResultPartitionWriter createConsumableNotifyingResultPartitionWriter(
            ResultPartitionType partitionType,
            TaskActions taskActions,
            JobID jobId,
            ResultPartitionConsumableNotifier notifier)
            throws IOException {
        ResultPartition partition = createResultPartition(partitionType);
        return ConsumableNotifyingResultPartitionWriterDecorator.decorate(
                Collections.singleton(
                        PartitionTestUtils.createPartitionDeploymentDescriptor(partitionType)),
                new ResultPartitionWriter[] {partition},
                taskActions,
                jobId,
                notifier)[0];
    }

    private BufferWritingResultPartition createResultPartition(ResultPartitionType partitionType)
            throws IOException {
        NettyShuffleEnvironment network =
                new NettyShuffleEnvironmentBuilder()
                        .setNumNetworkBuffers(10)
                        .setBufferSize(bufferSize)
                        .build();
        ResultPartition resultPartition =
                createPartition(network, fileChannelManager, partitionType, 2);
        resultPartition.setup();
        return (BufferWritingResultPartition) resultPartition;
    }

    @Test
    public void testIdleAndBackPressuredTime() throws IOException, InterruptedException {
        // setup
        int bufferSize = 1024;
        NetworkBufferPool globalPool = new NetworkBufferPool(10, bufferSize);
        BufferPool localPool = globalPool.createBufferPool(1, 1, 1, Integer.MAX_VALUE);
        BufferWritingResultPartition resultPartition =
                (BufferWritingResultPartition)
                        new ResultPartitionBuilder().setBufferPoolFactory(() -> localPool).build();
        resultPartition.setup();

        resultPartition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        ResultSubpartitionView readView =
                resultPartition.createSubpartitionView(0, new NoOpBufferAvailablityListener());
        Buffer buffer = readView.getNextBuffer().buffer();
        assertNotNull(buffer);

        // back-pressured time is zero when there is buffer available.
        assertThat(resultPartition.getBackPressuredTimeMsPerSecond().getValue(), equalTo(0L));

        CountDownLatch syncLock = new CountDownLatch(1);
        final Thread requestThread =
                new Thread(
                        () -> {
                            try {
                                // notify that the request thread start to run.
                                syncLock.countDown();
                                // wait for buffer.
                                resultPartition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
                            } catch (Exception e) {
                            }
                        });
        requestThread.start();

        // wait until request thread start to run.
        syncLock.await();

        Thread.sleep(100);

        // recycle the buffer
        buffer.recycleBuffer();
        requestThread.join();

        Assert.assertThat(
                resultPartition.getBackPressuredTimeMsPerSecond().getCount(),
                Matchers.greaterThan(0L));
        assertNotNull(readView.getNextBuffer().buffer());
    }

    @Test
    public void testFlushBoundedBlockingResultPartition() throws IOException {
        int value = 1024;
        ResultPartition partition = createResultPartition(ResultPartitionType.BLOCKING);

        ByteBuffer record = ByteBuffer.allocate(4);
        record.putInt(value);

        record.rewind();
        partition.emitRecord(record, 0);
        partition.flush(0);

        record.rewind();
        partition.emitRecord(record, 0);

        record.rewind();
        partition.broadcastRecord(record);
        partition.flushAll();

        record.rewind();
        partition.broadcastRecord(record);
        partition.finish();
        record.rewind();

        ResultSubpartitionView readView1 =
                partition.createSubpartitionView(0, new NoOpBufferAvailablityListener());
        for (int i = 0; i < 4; ++i) {
            assertEquals(record, readView1.getNextBuffer().buffer().getNioBufferReadable());
        }
        assertFalse(readView1.getNextBuffer().buffer().isBuffer());
        assertNull(readView1.getNextBuffer());

        ResultSubpartitionView readView2 =
                partition.createSubpartitionView(1, new NoOpBufferAvailablityListener());
        for (int i = 0; i < 2; ++i) {
            assertEquals(record, readView2.getNextBuffer().buffer().getNioBufferReadable());
        }
        assertFalse(readView2.getNextBuffer().buffer().isBuffer());
        assertNull(readView2.getNextBuffer());
    }

    @Test
    public void testEmitRecordWithRecordSpanningMultipleBuffers() throws Exception {
        BufferWritingResultPartition bufferWritingResultPartition =
                createResultPartition(ResultPartitionType.PIPELINED);
        PipelinedSubpartition pipelinedSubpartition =
                (PipelinedSubpartition) bufferWritingResultPartition.subpartitions[0];
        int partialLength = bufferSize / 3;

        try {
            // emit the first record, record length = partialLength
            bufferWritingResultPartition.emitRecord(ByteBuffer.allocate(partialLength), 0);
            // emit the second record, record length = bufferSize
            bufferWritingResultPartition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        } finally {
            assertEquals(2, pipelinedSubpartition.getCurrentNumberOfBuffers());
            assertEquals(0, pipelinedSubpartition.getNextBuffer().getPartialRecordLength());
            assertEquals(
                    partialLength, pipelinedSubpartition.getNextBuffer().getPartialRecordLength());
        }
    }

    @Test
    public void testBroadcastRecordWithRecordSpanningMultipleBuffers() throws Exception {
        BufferWritingResultPartition bufferWritingResultPartition =
                createResultPartition(ResultPartitionType.PIPELINED);
        int partialLength = bufferSize / 3;

        try {
            // emit the first record, record length = partialLength
            bufferWritingResultPartition.broadcastRecord(ByteBuffer.allocate(partialLength));
            // emit the second record, record length = bufferSize
            bufferWritingResultPartition.broadcastRecord(ByteBuffer.allocate(bufferSize));
        } finally {
            for (ResultSubpartition resultSubpartition :
                    bufferWritingResultPartition.subpartitions) {
                PipelinedSubpartition pipelinedSubpartition =
                        (PipelinedSubpartition) resultSubpartition;
                assertEquals(2, pipelinedSubpartition.getCurrentNumberOfBuffers());
                assertEquals(0, pipelinedSubpartition.getNextBuffer().getPartialRecordLength());
                assertEquals(
                        partialLength,
                        pipelinedSubpartition.getNextBuffer().getPartialRecordLength());
            }
        }
    }

    private static class TestResultPartitionConsumableNotifier
            implements ResultPartitionConsumableNotifier {
        private JobID jobID;
        private ResultPartitionID partitionID;
        private TaskActions taskActions;
        private int numNotification;

        @Override
        public void notifyPartitionConsumable(
                JobID jobID, ResultPartitionID partitionID, TaskActions taskActions) {
            ++numNotification;
            this.jobID = jobID;
            this.partitionID = partitionID;
            this.taskActions = taskActions;
        }

        private void check(
                JobID jobID,
                ResultPartitionID partitionID,
                TaskActions taskActions,
                int numNotification) {
            assertEquals(jobID, this.jobID);
            assertEquals(partitionID, this.partitionID);
            assertEquals(taskActions, this.taskActions);
            assertEquals(numNotification, this.numNotification);
        }

        private void reset() {
            jobID = null;
            partitionID = null;
            taskActions = null;
            numNotification = 0;
        }
    }
}
