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
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.completeInput;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.completeOutput;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.write;
import static org.assertj.core.api.Assertions.fail;

/** {@link ChannelStateWriteRequestDispatcherImpl} tests. */
@ExtendWith(ParameterizedTestExtension.class)
public class ChannelStateWriteRequestDispatcherTest {

    @Parameters(name = "expectedException={0} requests={1}")
    public static List<Object[]> data() {
        return Arrays.asList(
                // valid calls
                new Object[] {empty(), asList(start(), completeIn(), completeOut())},
                new Object[] {empty(), asList(start(), writeIn(), completeIn())},
                new Object[] {empty(), asList(start(), writeOut(), completeOut())},
                new Object[] {empty(), asList(start(), writeOutFuture(), completeOut())},
                new Object[] {empty(), asList(start(), completeIn(), writeOut())},
                new Object[] {empty(), asList(start(), completeIn(), writeOutFuture())},
                new Object[] {empty(), asList(start(), completeOut(), writeIn())},
                // invalid without start
                new Object[] {of(IllegalArgumentException.class), singletonList(writeIn())},
                new Object[] {of(IllegalArgumentException.class), singletonList(writeOut())},
                new Object[] {of(IllegalArgumentException.class), singletonList(writeOutFuture())},
                new Object[] {of(IllegalArgumentException.class), singletonList(completeIn())},
                new Object[] {of(IllegalArgumentException.class), singletonList(completeOut())},
                // invalid double complete
                new Object[] {
                    of(IllegalArgumentException.class), asList(start(), completeIn(), completeIn())
                },
                new Object[] {
                    of(IllegalArgumentException.class),
                    asList(start(), completeOut(), completeOut())
                },
                // invalid write after complete
                new Object[] {
                    of(IllegalStateException.class), asList(start(), completeIn(), writeIn())
                },
                new Object[] {
                    of(IllegalStateException.class), asList(start(), completeOut(), writeOut())
                },
                new Object[] {
                    of(IllegalStateException.class),
                    asList(start(), completeOut(), writeOutFuture())
                },
                // invalid double start
                new Object[] {of(IllegalStateException.class), asList(start(), start())});
    }

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;

    @Parameter public Optional<Class<Exception>> expectedException;

    @Parameter(value = 1)
    public List<ChannelStateWriteRequest> requests;

    private static final long CHECKPOINT_ID = 42L;

    private static CheckpointInProgressRequest completeOut() {
        return completeOutput(JOB_VERTEX_ID, SUBTASK_INDEX, CHECKPOINT_ID);
    }

    private static CheckpointInProgressRequest completeIn() {
        return completeInput(JOB_VERTEX_ID, SUBTASK_INDEX, CHECKPOINT_ID);
    }

    private static ChannelStateWriteRequest writeIn() {
        return write(
                JOB_VERTEX_ID,
                SUBTASK_INDEX,
                CHECKPOINT_ID,
                new InputChannelInfo(1, 1),
                CloseableIterator.ofElement(
                        new NetworkBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(1),
                                FreeingBufferRecycler.INSTANCE),
                        Buffer::recycleBuffer));
    }

    private static ChannelStateWriteRequest writeOut() {
        return write(
                JOB_VERTEX_ID,
                SUBTASK_INDEX,
                CHECKPOINT_ID,
                new ResultSubpartitionInfo(1, 1),
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(1),
                        FreeingBufferRecycler.INSTANCE));
    }

    private static ChannelStateWriteRequest writeOutFuture() {
        CompletableFuture<List<Buffer>> outFuture = new CompletableFuture<>();
        ChannelStateWriteRequest writeRequest =
                write(
                        JOB_VERTEX_ID,
                        SUBTASK_INDEX,
                        CHECKPOINT_ID,
                        new ResultSubpartitionInfo(1, 1),
                        outFuture);
        outFuture.complete(
                singletonList(
                        new NetworkBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(1),
                                FreeingBufferRecycler.INSTANCE)));
        return writeRequest;
    }

    private static SubtaskRegisterRequest register() {
        return new SubtaskRegisterRequest(JOB_VERTEX_ID, SUBTASK_INDEX);
    }

    private static CheckpointStartRequest start() {
        return new CheckpointStartRequest(
                JOB_VERTEX_ID,
                SUBTASK_INDEX,
                CHECKPOINT_ID,
                new ChannelStateWriteResult(),
                new CheckpointStorageLocationReference(new byte[] {1}));
    }

    @TestTemplate
    void doRun() {
        ChannelStateWriteRequestDispatcher processor =
                new ChannelStateWriteRequestDispatcherImpl(
                        new JobManagerCheckpointStorage(),
                        JOB_ID,
                        new ChannelStateSerializerImpl());
        try {
            processor.dispatch(register());
            for (ChannelStateWriteRequest request : requests) {
                processor.dispatch(request);
            }
        } catch (Throwable t) {
            if (expectedException.filter(e -> e.isInstance(t)).isPresent()) {
                return;
            }
            throw new RuntimeException("unexpected exception", t);
        }
        expectedException.ifPresent(e -> fail("expected exception " + e));
    }
}
