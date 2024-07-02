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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.FullyFinishedOperatorState;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.filemerging.LogicalFile;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DiscardRecordedStateObject;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestingRelativeFileStateHandle;
import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.runtime.state.filemerging.DirectoryStreamStateHandle;
import org.apache.flink.runtime.state.filemerging.EmptyFileMergingOperatorStreamStateHandle;
import org.apache.flink.runtime.state.filemerging.FileMergingOperatorStreamStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.empty;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A collection of utility methods for testing the (de)serialization of checkpoint metadata for
 * persistence.
 */
public class CheckpointTestUtils {

    /**
     * Creates a random collection of OperatorState objects containing various types of state
     * handles.
     *
     * @param basePath The basePath for savepoint, will be null for checkpoint.
     * @param numAllRunningTaskStates Number of tasks belong to all running vertex.
     * @param numPartlyFinishedTaskStates Number of tasks belong to partly finished vertex.
     * @param numFullyFinishedTaskStates Number of tasks belong to fully finished vertex.
     * @param numSubtasksPerTask Number of subtasks for each task.
     */
    public static Collection<OperatorState> createOperatorStates(
            Random random,
            @Nullable String basePath,
            int numAllRunningTaskStates,
            int numPartlyFinishedTaskStates,
            int numFullyFinishedTaskStates,
            int numSubtasksPerTask) {

        List<OperatorState> taskStates =
                new ArrayList<>(
                        numAllRunningTaskStates
                                + numPartlyFinishedTaskStates
                                + numFullyFinishedTaskStates);

        for (int stateIdx = 0; stateIdx < numAllRunningTaskStates; ++stateIdx) {
            OperatorState taskState = new OperatorState(new OperatorID(), numSubtasksPerTask, 128);
            randomlySetCoordinatorState(taskState, random);
            randomlySetSubtaskState(
                    taskState, IntStream.range(0, numSubtasksPerTask).toArray(), random, basePath);
            taskStates.add(taskState);
        }

        for (int stateIdx = 0; stateIdx < numPartlyFinishedTaskStates; ++stateIdx) {
            OperatorState taskState = new OperatorState(new OperatorID(), numSubtasksPerTask, 128);
            randomlySetCoordinatorState(taskState, random);
            randomlySetSubtaskState(
                    taskState,
                    IntStream.range(0, numSubtasksPerTask / 2).toArray(),
                    random,
                    basePath);
            IntStream.range(numSubtasksPerTask / 2, numSubtasksPerTask)
                    .forEach(
                            index ->
                                    taskState.putState(
                                            index, FinishedOperatorSubtaskState.INSTANCE));
            taskStates.add(taskState);
        }

        for (int stateIdx = 0; stateIdx < numFullyFinishedTaskStates; ++stateIdx) {
            taskStates.add(
                    new FullyFinishedOperatorState(new OperatorID(), numSubtasksPerTask, 128));
        }

        return taskStates;
    }

    private static void randomlySetCoordinatorState(OperatorState taskState, Random random) {
        final boolean hasCoordinatorState = random.nextBoolean();
        if (hasCoordinatorState) {
            final ByteStreamStateHandle stateHandle =
                    createDummyByteStreamStreamStateHandle(random);
            taskState.setCoordinatorState(stateHandle);
        }
    }

    private static void randomlySetSubtaskState(
            OperatorState taskState, int[] subtasksToSet, Random random, String basePath) {
        boolean hasOperatorStateBackend = random.nextBoolean();
        boolean hasOperatorStateStream = random.nextBoolean();

        boolean hasKeyedBackend = random.nextInt(4) != 0;
        boolean hasKeyedStream = random.nextInt(4) != 0;
        boolean isIncremental = random.nextInt(3) == 0;

        for (int subtaskIdx : subtasksToSet) {
            final OperatorSubtaskState.Builder state = OperatorSubtaskState.builder();
            if (hasOperatorStateBackend) {
                state.setManagedOperatorState(createDummyOperatorStreamStateHandle(random));
            }

            if (hasOperatorStateStream) {
                state.setRawOperatorState(createDummyOperatorStreamStateHandle(random));
            }

            if (hasKeyedBackend) {
                final KeyedStateHandle stateHandle;
                if (isSavepoint(basePath)) {
                    stateHandle = createDummyKeyGroupSavepointStateHandle(random, basePath);
                } else if (isIncremental) {
                    stateHandle = createDummyIncrementalKeyedStateHandle(random);
                } else {
                    stateHandle = createDummyKeyGroupStateHandle(random, null);
                }
                state.setRawKeyedState(stateHandle);
            }

            if (hasKeyedStream) {
                final KeyedStateHandle stateHandle;
                if (isSavepoint(basePath)) {
                    stateHandle = createDummyKeyGroupSavepointStateHandle(random, basePath);
                } else {
                    stateHandle = createDummyKeyGroupStateHandle(random, null);
                }
                state.setManagedKeyedState(stateHandle);
            }

            state.setInputChannelState(
                    (random.nextBoolean() && !isSavepoint(basePath))
                            ? singleton(createNewInputChannelStateHandle(random.nextInt(5), random))
                            : empty());
            state.setResultSubpartitionState(
                    (random.nextBoolean() && !isSavepoint(basePath))
                            ? singleton(
                                    createNewResultSubpartitionStateHandle(
                                            random.nextInt(5), random))
                            : empty());

            taskState.putState(subtaskIdx, state.build());
        }
    }

    private static OperatorStreamStateHandle createDummyOperatorStreamStateHandle(Random rnd) {
        Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>();
        offsetsMap.put(
                "A",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0, 10, 20}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
        offsetsMap.put(
                "B",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {30, 40, 50}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
        offsetsMap.put(
                "C",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {60, 70, 80}, OperatorStateHandle.Mode.UNION));

        boolean enableFileMerging = rnd.nextBoolean();
        if (enableFileMerging) {
            DirectoryStreamStateHandle taskOwnedDirHandle =
                    DirectoryStreamStateHandle.of(new Path(createRandomUUID(rnd).toString()));
            DirectoryStreamStateHandle sharedDirHandle =
                    DirectoryStreamStateHandle.of(new Path(createRandomUUID(rnd).toString()));
            return rnd.nextBoolean()
                    ? new FileMergingOperatorStreamStateHandle(
                            taskOwnedDirHandle,
                            sharedDirHandle,
                            offsetsMap,
                            createDummySegmentFileStateHandle(rnd, false))
                    : EmptyFileMergingOperatorStreamStateHandle.create(
                            taskOwnedDirHandle, sharedDirHandle);
        } else {
            StreamStateHandle operatorStateStream =
                    new ByteStreamStateHandle(
                            "b", ("Beautiful").getBytes(ConfigConstants.DEFAULT_CHARSET));
            return new OperatorStreamStateHandle(offsetsMap, operatorStateStream);
        }
    }

    private static boolean isSavepoint(String basePath) {
        return basePath != null;
    }

    /** Creates a bunch of random master states. */
    public static Collection<MasterState> createRandomMasterStates(Random random, int num) {
        final ArrayList<MasterState> states = new ArrayList<>(num);

        for (int i = 0; i < num; i++) {
            int version = random.nextInt(10);
            String name = StringUtils.getRandomString(random, 5, 500);
            byte[] bytes = new byte[random.nextInt(5000) + 1];
            random.nextBytes(bytes);

            states.add(new MasterState(name, bytes, version));
        }

        return states;
    }

    /**
     * Asserts that two MasterStates are equal.
     *
     * <p>The MasterState avoids overriding {@code equals()} on purpose, because equality is not
     * well defined in the raw contents.
     */
    public static void assertMasterStateEquality(MasterState a, MasterState b) {
        assertThat(b.version()).isEqualTo(a.version());
        assertThat(b.name()).isEqualTo(a.name());
        assertThat(b.bytes()).isEqualTo(a.bytes());
    }

    // ------------------------------------------------------------------------

    /** utility class, not meant to be instantiated. */
    private CheckpointTestUtils() {}

    public static IncrementalRemoteKeyedStateHandle createDummyIncrementalKeyedStateHandle(
            Random rnd) {
        return createDummyIncrementalKeyedStateHandle(42L, rnd);
    }

    public static IncrementalRemoteKeyedStateHandle createDummyIncrementalKeyedStateHandle(
            long checkpointId, Random rnd) {
        return new IncrementalRemoteKeyedStateHandle(
                createRandomUUID(rnd),
                new KeyGroupRange(1, 1),
                checkpointId,
                createRandomHandleAndLocalPathList(rnd),
                createRandomHandleAndLocalPathList(rnd),
                createDummyStreamStateHandle(rnd, null));
    }

    public static List<HandleAndLocalPath> createRandomHandleAndLocalPathList(Random rnd) {
        boolean enableFileMerging = rnd.nextBoolean();
        final int size = rnd.nextInt(4);
        List<HandleAndLocalPath> result = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            String localPath = createRandomUUID(rnd).toString();
            StreamStateHandle stateHandle =
                    enableFileMerging
                            ? createDummySegmentFileStateHandle(rnd)
                            : createDummyStreamStateHandle(rnd, null);
            result.add(HandleAndLocalPath.of(stateHandle, localPath));
        }

        return result;
    }

    public static KeyGroupsStateHandle createDummyKeyGroupSavepointStateHandle(
            Random rnd, String basePath) {
        return new KeyGroupsSavepointStateHandle(
                new KeyGroupRangeOffsets(1, 1, new long[] {rnd.nextInt(1024)}),
                createDummyStreamStateHandle(rnd, basePath));
    }

    public static KeyGroupsStateHandle createDummyKeyGroupStateHandle(Random rnd, String basePath) {
        return new KeyGroupsStateHandle(
                new KeyGroupRangeOffsets(1, 1, new long[] {rnd.nextInt(1024)}),
                createDummyStreamStateHandle(rnd, basePath));
    }

    public static ByteStreamStateHandle createDummyByteStreamStreamStateHandle(Random rnd) {
        return (ByteStreamStateHandle) createDummyStreamStateHandle(rnd, null);
    }

    public static StreamStateHandle createDummyStreamStateHandle(
            Random rnd, @Nullable String basePath) {
        if (!isSavepoint(basePath)) {
            String stateId = String.valueOf(createRandomUUID(rnd));
            byte[] stateContent = stateId.getBytes(StandardCharsets.UTF_8);
            return new TestingStreamStateHandle(stateId, stateContent);
        } else {
            long stateSize = rnd.nextLong();
            if (stateSize <= 0) {
                stateSize = -stateSize;
            }
            String relativePath = String.valueOf(createRandomUUID(rnd));
            Path statePath = new Path(basePath, relativePath);
            return new TestingRelativeFileStateHandle(statePath, relativePath, stateSize);
        }
    }

    private static StreamStateHandle createDummySegmentFileStateHandle(Random rnd) {
        return createDummySegmentFileStateHandle(rnd, rnd.nextBoolean());
    }

    private static StreamStateHandle createDummySegmentFileStateHandle(
            Random rnd, boolean isEmpty) {
        return isEmpty
                ? new TestingSegmentFileStateHandle(
                        new Path(UUID.randomUUID().toString()),
                        0,
                        0,
                        CheckpointedStateScope.EXCLUSIVE)
                : new TestingSegmentFileStateHandle(
                        new Path(String.valueOf(createRandomUUID(rnd))),
                        0,
                        1,
                        CheckpointedStateScope.SHARED);
    }

    private static class TestingSegmentFileStateHandle extends SegmentFileStateHandle
            implements DiscardRecordedStateObject {

        private static final long serialVersionUID = 1L;

        private boolean disposed;

        public TestingSegmentFileStateHandle(
                Path filePath, long startPos, long stateSize, CheckpointedStateScope scope) {
            super(
                    filePath,
                    startPos,
                    stateSize,
                    scope,
                    LogicalFile.LogicalFileId.generateRandomId());
        }

        @Override
        public void collectSizeStats(StateObjectSizeStatsCollector collector) {
            // Collect to LOCAL_MEMORY for test
            collector.add(StateObjectLocation.LOCAL_MEMORY, getStateSize());
        }

        @Override
        public void discardState() {
            super.discardState();
            disposed = true;
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }
    }

    private static UUID createRandomUUID(Random rnd) {
        return new UUID(rnd.nextLong(), rnd.nextLong());
    }
}
