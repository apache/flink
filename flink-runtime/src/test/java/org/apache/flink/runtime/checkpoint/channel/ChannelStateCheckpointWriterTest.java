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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory.MemoryCheckpointOutputStream;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;
import static org.apache.flink.core.memory.MemorySegmentFactory.wrap;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHANNEL_STATE_SHARED_STREAM_EXCEPTION;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteResultUtil.assertAllSubtaskDoneNormally;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteResultUtil.assertAllSubtaskNotDone;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteResultUtil.assertCheckpointFailureReason;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteResultUtil.assertHasSpecialCause;
import static org.apache.flink.runtime.state.CheckpointedStateScope.EXCLUSIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** {@link ChannelStateCheckpointWriter} test. */
class ChannelStateCheckpointWriterTest {
    private static final RunnableWithException NO_OP_RUNNABLE = () -> {};
    private final Random random = new Random();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;
    private static final SubtaskID SUBTASK_ID = SubtaskID.of(JOB_VERTEX_ID, SUBTASK_INDEX);

    @TempDir private Path temporaryFolder;

    @Test
    void testFileHandleSize() throws Exception {
        int numChannels = 3;
        int numWritesPerChannel = 4;
        int numBytesPerWrite = 5;
        ChannelStateWriteResult result = new ChannelStateWriteResult();
        ChannelStateCheckpointWriter writer =
                createWriter(
                        result,
                        new FsCheckpointStreamFactory(
                                        getSharedInstance(),
                                        fromLocalFile(
                                                TempDirUtils.newFolder(
                                                        temporaryFolder, "checkpointsDir")),
                                        fromLocalFile(
                                                TempDirUtils.newFolder(
                                                        temporaryFolder, "sharedStateDir")),
                                        numBytesPerWrite - 1,
                                        numBytesPerWrite - 1)
                                .createCheckpointStateOutputStream(EXCLUSIVE));

        InputChannelInfo[] channels =
                IntStream.range(0, numChannels)
                        .mapToObj(i -> new InputChannelInfo(0, i))
                        .toArray(InputChannelInfo[]::new);
        for (int call = 0; call < numWritesPerChannel; call++) {
            for (int channel = 0; channel < numChannels; channel++) {
                write(writer, channels[channel], getData(numBytesPerWrite));
            }
        }
        writer.completeInput(JOB_VERTEX_ID, SUBTASK_INDEX);
        writer.completeOutput(JOB_VERTEX_ID, SUBTASK_INDEX);

        for (InputChannelStateHandle handle : result.inputChannelStateHandles.get()) {
            assertThat(handle.getStateSize())
                    .isEqualTo((Integer.BYTES + numBytesPerWrite) * numWritesPerChannel);
        }
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void testSmallFilesNotWritten() throws Exception {
        int threshold = 100;
        File checkpointsDir = TempDirUtils.newFolder(temporaryFolder, "checkpointsDir");
        File sharedStateDir = TempDirUtils.newFolder(temporaryFolder, "sharedStateDir");
        FsCheckpointStreamFactory checkpointStreamFactory =
                new FsCheckpointStreamFactory(
                        getSharedInstance(),
                        fromLocalFile(checkpointsDir),
                        fromLocalFile(sharedStateDir),
                        threshold,
                        threshold);
        ChannelStateWriteResult result = new ChannelStateWriteResult();
        ChannelStateCheckpointWriter writer =
                createWriter(
                        result,
                        checkpointStreamFactory.createCheckpointStateOutputStream(EXCLUSIVE));
        NetworkBuffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(threshold / 2),
                        FreeingBufferRecycler.INSTANCE);
        writer.writeInput(JOB_VERTEX_ID, SUBTASK_INDEX, new InputChannelInfo(1, 2), buffer);
        writer.completeOutput(JOB_VERTEX_ID, SUBTASK_INDEX);
        writer.completeInput(JOB_VERTEX_ID, SUBTASK_INDEX);
        assertThat(result.isDone()).isTrue();
        assertThat(checkpointsDir).isEmptyDirectory();
        assertThat(sharedStateDir).isEmptyDirectory();
    }

    @Test
    void testEmptyState() throws Exception {
        MemoryCheckpointOutputStream stream =
                new MemoryCheckpointOutputStream(1000) {
                    @Override
                    public StreamStateHandle closeAndGetHandle() {
                        fail("closeAndGetHandle shouldn't be called for empty channel state");
                        return null;
                    }
                };
        ChannelStateCheckpointWriter writer = createWriter(new ChannelStateWriteResult(), stream);
        writer.completeOutput(JOB_VERTEX_ID, SUBTASK_INDEX);
        writer.completeInput(JOB_VERTEX_ID, SUBTASK_INDEX);
        assertThat(stream.isClosed()).isTrue();
    }

    @Test
    void testRecyclingBuffers() {
        ChannelStateCheckpointWriter writer = createWriter(new ChannelStateWriteResult());
        NetworkBuffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(10),
                        FreeingBufferRecycler.INSTANCE);
        writer.writeInput(JOB_VERTEX_ID, SUBTASK_INDEX, new InputChannelInfo(1, 2), buffer);
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testFlush() throws Exception {
        class FlushRecorder extends DataOutputStream {
            private boolean flushed = false;

            private FlushRecorder() {
                super(new ByteArrayOutputStream());
            }

            @Override
            public void flush() throws IOException {
                flushed = true;
                super.flush();
            }
        }

        FlushRecorder dataStream = new FlushRecorder();
        final ChannelStateCheckpointWriter writer =
                new ChannelStateCheckpointWriter(
                        Collections.singleton(SUBTASK_ID),
                        1L,
                        new ChannelStateSerializerImpl(),
                        NO_OP_RUNNABLE,
                        new MemoryCheckpointOutputStream(42),
                        dataStream);
        writer.registerSubtaskResult(SUBTASK_ID, new ChannelStateWriteResult());
        writer.completeInput(JOB_VERTEX_ID, SUBTASK_INDEX);
        writer.completeOutput(JOB_VERTEX_ID, SUBTASK_INDEX);

        assertThat(dataStream.flushed).isTrue();
    }

    @Test
    void testResultCompletion() throws Exception {
        for (int maxSubtasksPerChannelStateFile = 1;
                maxSubtasksPerChannelStateFile < 10;
                maxSubtasksPerChannelStateFile++) {
            testMultiTaskCompletionAndAssertResult(maxSubtasksPerChannelStateFile);
        }
    }

    private void testMultiTaskCompletionAndAssertResult(int maxSubtasksPerChannelStateFile)
            throws Exception {
        Map<SubtaskID, ChannelStateWriteResult> subtasks = new HashMap<>();
        for (int i = 0; i < maxSubtasksPerChannelStateFile; i++) {
            subtasks.put(SubtaskID.of(new JobVertexID(), i), new ChannelStateWriteResult());
        }
        MemoryCheckpointOutputStream stream = new MemoryCheckpointOutputStream(1000);
        ChannelStateCheckpointWriter writer = createWriter(stream, subtasks.keySet());
        for (Map.Entry<SubtaskID, ChannelStateWriteResult> entry : subtasks.entrySet()) {
            writer.registerSubtaskResult(entry.getKey(), entry.getValue());
        }

        for (SubtaskID subtaskID : subtasks.keySet()) {
            assertAllSubtaskNotDone(subtasks.values());
            assertThat(stream.isClosed()).isFalse();
            writer.completeInput(subtaskID.getJobVertexID(), subtaskID.getSubtaskIndex());
            assertAllSubtaskNotDone(subtasks.values());
            assertThat(stream.isClosed()).isFalse();
            writer.completeOutput(subtaskID.getJobVertexID(), subtaskID.getSubtaskIndex());
        }
        assertThat(stream.isClosed()).isTrue();
        assertAllSubtaskDoneNormally(subtasks.values());
    }

    @Test
    void testTaskUnregister() throws Exception {
        testTaskUnregisterAndAssertResult(2);
        testTaskUnregisterAndAssertResult(3);
        testTaskUnregisterAndAssertResult(5);
        testTaskUnregisterAndAssertResult(10);
    }

    private void testTaskUnregisterAndAssertResult(int maxSubtasksPerChannelStateFile)
            throws Exception {
        Map<SubtaskID, ChannelStateWriteResult> subtasks = new HashMap<>();
        for (int i = 0; i < maxSubtasksPerChannelStateFile; i++) {
            subtasks.put(SubtaskID.of(new JobVertexID(), i), new ChannelStateWriteResult());
        }
        MemoryCheckpointOutputStream stream = new MemoryCheckpointOutputStream(1000);
        ChannelStateCheckpointWriter writer = createWriter(stream, subtasks.keySet());
        SubtaskID unregisterSubtask = null;
        Iterator<Map.Entry<SubtaskID, ChannelStateWriteResult>> iterator =
                subtasks.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<SubtaskID, ChannelStateWriteResult> entry = iterator.next();
            if (unregisterSubtask == null) {
                unregisterSubtask = entry.getKey();
                iterator.remove();
                continue;
            }
            writer.registerSubtaskResult(entry.getKey(), entry.getValue());
        }

        for (SubtaskID subtaskID : subtasks.keySet()) {
            writer.completeInput(subtaskID.getJobVertexID(), subtaskID.getSubtaskIndex());
            writer.completeOutput(subtaskID.getJobVertexID(), subtaskID.getSubtaskIndex());
        }
        assertAllSubtaskNotDone(subtasks.values());
        assertThat(stream.isClosed()).isFalse();

        assert unregisterSubtask != null;
        writer.releaseSubtask(unregisterSubtask);
        assertThat(stream.isClosed()).isTrue();
        assertAllSubtaskDoneNormally(subtasks.values());
    }

    @Test
    void testTaskFailThenCompleteOtherTask() {
        testTaskFailAfterAllTaskRegisteredAndAssertResult(2);
        testTaskFailAfterAllTaskRegisteredAndAssertResult(3);
        testTaskFailAfterAllTaskRegisteredAndAssertResult(5);
        testTaskFailAfterAllTaskRegisteredAndAssertResult(10);
    }

    private void testTaskFailAfterAllTaskRegisteredAndAssertResult(
            int maxSubtasksPerChannelStateFile) {
        Map<SubtaskID, ChannelStateWriteResult> subtasks = new HashMap<>();
        for (int i = 0; i < maxSubtasksPerChannelStateFile; i++) {
            subtasks.put(SubtaskID.of(new JobVertexID(), i), new ChannelStateWriteResult());
        }
        MemoryCheckpointOutputStream stream = new MemoryCheckpointOutputStream(1000);
        ChannelStateCheckpointWriter writer = createWriter(stream, subtasks.keySet());
        SubtaskID firstSubtask = null;
        for (Map.Entry<SubtaskID, ChannelStateWriteResult> entry : subtasks.entrySet()) {
            if (firstSubtask == null) {
                firstSubtask = entry.getKey();
            }
            writer.registerSubtaskResult(entry.getKey(), entry.getValue());
        }
        assertThat(stream.isClosed()).isFalse();

        assert firstSubtask != null;
        writer.fail(
                firstSubtask.getJobVertexID(), firstSubtask.getSubtaskIndex(), new TestException());
        assertThat(stream.isClosed()).isTrue();

        for (Map.Entry<SubtaskID, ChannelStateWriteResult> entry : subtasks.entrySet()) {
            if (firstSubtask.equals(entry.getKey())) {
                assertHasSpecialCause(entry.getValue(), TestException.class);
                continue;
            }
            assertCheckpointFailureReason(entry.getValue(), CHANNEL_STATE_SHARED_STREAM_EXCEPTION);
        }
    }

    @Test
    void testCloseGetHandleThrowException() throws Exception {
        Map<SubtaskID, ChannelStateWriteResult> subtasks = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            subtasks.put(SubtaskID.of(new JobVertexID(), i), new ChannelStateWriteResult());
        }
        CloseExceptionOutputStream stream = new CloseExceptionOutputStream();
        ChannelStateCheckpointWriter writer = createWriter(stream, subtasks.keySet());
        for (Map.Entry<SubtaskID, ChannelStateWriteResult> entry : subtasks.entrySet()) {
            SubtaskID subtaskID = entry.getKey();
            writer.registerSubtaskResult(subtaskID, entry.getValue());
            NetworkBuffer buffer =
                    new NetworkBuffer(
                            MemorySegmentFactory.allocateUnpooledSegment(10),
                            FreeingBufferRecycler.INSTANCE);
            writer.writeInput(
                    subtaskID.getJobVertexID(),
                    subtaskID.getSubtaskIndex(),
                    new InputChannelInfo(1, 2),
                    buffer);
        }

        for (SubtaskID subtaskID : subtasks.keySet()) {
            assertAllSubtaskNotDone(subtasks.values());
            assertThat(stream.isClosed()).isFalse();
            writer.completeInput(subtaskID.getJobVertexID(), subtaskID.getSubtaskIndex());
            assertAllSubtaskNotDone(subtasks.values());
            assertThat(stream.isClosed()).isFalse();
            writer.completeOutput(subtaskID.getJobVertexID(), subtaskID.getSubtaskIndex());
        }
        assertThat(stream.isClosed()).isTrue();
        for (Map.Entry<SubtaskID, ChannelStateWriteResult> entry : subtasks.entrySet()) {
            assertThatThrownBy(() -> entry.getValue().getInputChannelStateHandles().get())
                    .cause()
                    .isInstanceOf(IOException.class)
                    .hasMessage("Test closeAndGetHandle exception.");
            assertThatThrownBy(() -> entry.getValue().getResultSubpartitionStateHandles().get())
                    .cause()
                    .isInstanceOf(IOException.class)
                    .hasMessage("Test closeAndGetHandle exception.");
        }
    }

    @Test
    void testRegisterSubtaskAfterWriterDone() {
        Map<SubtaskID, ChannelStateWriteResult> subtasks = new HashMap<>();
        SubtaskID subtask0 = SubtaskID.of(JOB_VERTEX_ID, 0);
        SubtaskID subtask1 = SubtaskID.of(JOB_VERTEX_ID, 1);
        subtasks.put(subtask0, new ChannelStateWriteResult());
        subtasks.put(subtask1, new ChannelStateWriteResult());
        MemoryCheckpointOutputStream stream = new MemoryCheckpointOutputStream(1000);
        ChannelStateCheckpointWriter writer = createWriter(stream, subtasks.keySet());
        writer.fail(new JobVertexID(), 0, new TestException());
        assertThatThrownBy(
                        () -> writer.registerSubtaskResult(subtask0, new ChannelStateWriteResult()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The write is done.");
        assertThatThrownBy(
                        () -> writer.registerSubtaskResult(subtask1, new ChannelStateWriteResult()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The write is done.");
    }

    @Test
    void testRecordingOffsets() throws Exception {
        Map<InputChannelInfo, Integer> offsetCounts = new HashMap<>();
        offsetCounts.put(new InputChannelInfo(1, 1), 1);
        offsetCounts.put(new InputChannelInfo(1, 2), 2);
        offsetCounts.put(new InputChannelInfo(1, 3), 5);
        int numBytes = 100;

        ChannelStateWriteResult result = new ChannelStateWriteResult();
        ChannelStateCheckpointWriter writer = createWriter(result);
        for (Map.Entry<InputChannelInfo, Integer> e : offsetCounts.entrySet()) {
            for (int i = 0; i < e.getValue(); i++) {
                write(writer, e.getKey(), getData(numBytes));
            }
        }
        writer.completeInput(JOB_VERTEX_ID, SUBTASK_INDEX);
        writer.completeOutput(JOB_VERTEX_ID, SUBTASK_INDEX);

        for (InputChannelStateHandle handle : result.inputChannelStateHandles.get()) {
            int headerSize = Integer.BYTES;
            int lengthSize = Integer.BYTES;
            assertThat(handle.getOffsets()).isEqualTo(singletonList((long) headerSize));
            assertThat(handle.getDelegate().getStateSize())
                    .isEqualTo(
                            headerSize
                                    + lengthSize
                                    + numBytes * offsetCounts.remove(handle.getInfo()));
        }
        assertThat(offsetCounts).isEmpty();
    }

    private byte[] getData(int len) {
        byte[] bytes = new byte[len];
        random.nextBytes(bytes);
        return bytes;
    }

    private void write(
            ChannelStateCheckpointWriter writer, InputChannelInfo channelInfo, byte[] data) {
        MemorySegment segment = wrap(data);
        NetworkBuffer buffer =
                new NetworkBuffer(
                        segment,
                        FreeingBufferRecycler.INSTANCE,
                        Buffer.DataType.DATA_BUFFER,
                        segment.size());
        writer.writeInput(JOB_VERTEX_ID, SUBTASK_INDEX, channelInfo, buffer);
    }

    private ChannelStateCheckpointWriter createWriter(ChannelStateWriteResult result) {
        return createWriter(result, new MemoryCheckpointOutputStream(1000));
    }

    private ChannelStateCheckpointWriter createWriter(
            ChannelStateWriteResult result, CheckpointStateOutputStream stream) {
        ChannelStateCheckpointWriter writer =
                createWriter(stream, Collections.singleton(SUBTASK_ID));
        writer.registerSubtaskResult(SUBTASK_ID, result);
        return writer;
    }

    private ChannelStateCheckpointWriter createWriter(
            CheckpointStateOutputStream stream, Set<SubtaskID> subtasks) {
        return new ChannelStateCheckpointWriter(
                subtasks, 1L, stream, new ChannelStateSerializerImpl(), NO_OP_RUNNABLE);
    }
}

/** The output stream that throws an exception when close or closeAndGetHandle. */
class CloseExceptionOutputStream extends MemoryCheckpointOutputStream {
    public CloseExceptionOutputStream() {
        super(1000);
    }

    @Nullable
    @Override
    public StreamStateHandle closeAndGetHandle() throws IOException {
        throw new IOException("Test closeAndGetHandle exception.");
    }
}
