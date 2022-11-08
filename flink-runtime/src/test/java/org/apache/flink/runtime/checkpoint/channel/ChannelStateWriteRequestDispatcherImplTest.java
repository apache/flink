/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHANNEL_STATE_SHARED_STREAM_EXCEPTION;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteResultUtil.assertCheckpointFailureReason;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteResultUtil.assertHasSpecialCause;
import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link ChannelStateWriteRequestDispatcherImpl} test. */
class ChannelStateWriteRequestDispatcherImplTest {

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;

    @Test
    void testPartialInputChannelStateWrite() throws Exception {
        testBuffersRecycled(
                buffers ->
                        ChannelStateWriteRequest.write(
                                JOB_VERTEX_ID,
                                SUBTASK_INDEX,
                                1L,
                                new InputChannelInfo(1, 2),
                                ofElements(Buffer::recycleBuffer, buffers)));
    }

    @Test
    void testPartialResultSubpartitionStateWrite() throws Exception {
        testBuffersRecycled(
                buffers ->
                        ChannelStateWriteRequest.write(
                                JOB_VERTEX_ID,
                                SUBTASK_INDEX,
                                1L,
                                new ResultSubpartitionInfo(1, 2),
                                buffers));
    }

    private void testBuffersRecycled(
            Function<NetworkBuffer[], ChannelStateWriteRequest> requestBuilder) throws Exception {
        ChannelStateWriteRequestDispatcher dispatcher =
                new ChannelStateWriteRequestDispatcherImpl(
                        new JobManagerCheckpointStorage(),
                        JOB_ID,
                        new ChannelStateSerializerImpl());
        ChannelStateWriteResult result = new ChannelStateWriteResult();
        dispatcher.dispatch(ChannelStateWriteRequest.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX));
        dispatcher.dispatch(
                ChannelStateWriteRequest.start(
                        JOB_VERTEX_ID,
                        SUBTASK_INDEX,
                        1L,
                        result,
                        CheckpointStorageLocationReference.getDefault()));

        result.getResultSubpartitionStateHandles().completeExceptionally(new TestException());
        result.getInputChannelStateHandles().completeExceptionally(new TestException());

        NetworkBuffer[] buffers = new NetworkBuffer[] {buffer(), buffer()};
        dispatcher.dispatch(requestBuilder.apply(buffers));
        for (NetworkBuffer buffer : buffers) {
            assertThat(buffer.isRecycled()).isTrue();
        }
    }

    @Test
    void testStartNewCheckpointForSameSubtask() throws Exception {
        testStartNewCheckpointAndCheckOldCheckpointResult(false);
    }

    @Test
    void testStartNewCheckpointForDifferentSubtask() throws Exception {
        testStartNewCheckpointAndCheckOldCheckpointResult(true);
    }

    private void testStartNewCheckpointAndCheckOldCheckpointResult(boolean isDifferentSubtask)
            throws Exception {
        ChannelStateWriteRequestDispatcher processor =
                new ChannelStateWriteRequestDispatcherImpl(
                        new JobManagerCheckpointStorage(),
                        JOB_ID,
                        new ChannelStateSerializerImpl());
        ChannelStateWriteResult result = new ChannelStateWriteResult();
        processor.dispatch(ChannelStateWriteRequest.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX));
        JobVertexID newJobVertex = JOB_VERTEX_ID;
        if (isDifferentSubtask) {
            newJobVertex = new JobVertexID();
            processor.dispatch(
                    ChannelStateWriteRequest.registerSubtask(newJobVertex, SUBTASK_INDEX));
        }
        processor.dispatch(
                ChannelStateWriteRequest.start(
                        JOB_VERTEX_ID,
                        SUBTASK_INDEX,
                        1L,
                        result,
                        CheckpointStorageLocationReference.getDefault()));
        assertThat(result.isDone()).isFalse();

        processor.dispatch(
                ChannelStateWriteRequest.start(
                        newJobVertex,
                        SUBTASK_INDEX,
                        2L,
                        new ChannelStateWriteResult(),
                        CheckpointStorageLocationReference.getDefault()));
        assertCheckpointFailureReason(result, CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED);
    }

    @Test
    void testStartOldCheckpointForSameSubtask() throws Exception {
        testStartOldCheckpointAfterNewCheckpointAborted(false);
    }

    @Test
    void testStartOldCheckpointForDifferentSubtask() throws Exception {
        testStartOldCheckpointAfterNewCheckpointAborted(true);
    }

    private void testStartOldCheckpointAfterNewCheckpointAborted(boolean isDifferentSubtask)
            throws Exception {
        ChannelStateWriteRequestDispatcher processor =
                new ChannelStateWriteRequestDispatcherImpl(
                        new JobManagerCheckpointStorage(),
                        JOB_ID,
                        new ChannelStateSerializerImpl());
        processor.dispatch(ChannelStateWriteRequest.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX));
        JobVertexID newJobVertex = JOB_VERTEX_ID;
        if (isDifferentSubtask) {
            newJobVertex = new JobVertexID();
            processor.dispatch(
                    ChannelStateWriteRequest.registerSubtask(newJobVertex, SUBTASK_INDEX));
        }
        processor.dispatch(
                ChannelStateWriteRequest.abort(
                        JOB_VERTEX_ID, SUBTASK_INDEX, 2L, new TestException()));

        ChannelStateWriteResult result = new ChannelStateWriteResult();
        processor.dispatch(
                ChannelStateWriteRequest.start(
                        newJobVertex,
                        SUBTASK_INDEX,
                        1L,
                        result,
                        CheckpointStorageLocationReference.getDefault()));
        assertCheckpointFailureReason(result, CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED);
    }

    @Test
    void testAbortCheckpointAndCheckAllException() throws Exception {
        testAbortCheckpointAndCheckAllException(1);
        testAbortCheckpointAndCheckAllException(2);
        testAbortCheckpointAndCheckAllException(3);
        testAbortCheckpointAndCheckAllException(5);
        testAbortCheckpointAndCheckAllException(10);
    }

    private void testAbortCheckpointAndCheckAllException(int numberOfSubtask) throws Exception {
        ChannelStateWriteRequestDispatcher processor =
                new ChannelStateWriteRequestDispatcherImpl(
                        new JobManagerCheckpointStorage(),
                        JOB_ID,
                        new ChannelStateSerializerImpl());
        List<ChannelStateWriteResult> results = new ArrayList<>(numberOfSubtask);
        for (int i = 0; i < numberOfSubtask; i++) {
            processor.dispatch(ChannelStateWriteRequest.registerSubtask(JOB_VERTEX_ID, i));
        }
        long checkpointId = 1L;
        int abortedSubtaskIndex = new Random().nextInt(numberOfSubtask);
        processor.dispatch(
                ChannelStateWriteRequest.abort(
                        JOB_VERTEX_ID, abortedSubtaskIndex, checkpointId, new TestException()));
        for (int i = 0; i < numberOfSubtask; i++) {
            ChannelStateWriteResult result = new ChannelStateWriteResult();
            results.add(result);
            processor.dispatch(
                    ChannelStateWriteRequest.start(
                            JOB_VERTEX_ID,
                            i,
                            checkpointId,
                            result,
                            CheckpointStorageLocationReference.getDefault()));
        }
        assertThat(results).allMatch(ChannelStateWriteResult::isDone);
        for (int i = 0; i < numberOfSubtask; i++) {
            ChannelStateWriteResult result = results.get(i);
            if (i == abortedSubtaskIndex) {
                assertHasSpecialCause(result, TestException.class);
            } else {
                assertCheckpointFailureReason(result, CHANNEL_STATE_SHARED_STREAM_EXCEPTION);
            }
        }
    }

    private NetworkBuffer buffer() {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(10), FreeingBufferRecycler.INSTANCE);
    }
}
