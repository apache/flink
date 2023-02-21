/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;

/** Class for common test utilities. */
public class TestUtils {

    public static File createSavepointWithOperatorState(
            File savepointFile, long savepointId, OperatorID... operatorIds) throws IOException {
        final Collection<OperatorState> operatorStates = createOperatorState(operatorIds);
        final CheckpointMetadata savepoint =
                new CheckpointMetadata(savepointId, operatorStates, Collections.emptyList());

        try (FileOutputStream fileOutputStream = new FileOutputStream(savepointFile)) {
            Checkpoints.storeCheckpointMetadata(savepoint, fileOutputStream);
        }

        return savepointFile;
    }

    private static Collection<OperatorState> createOperatorState(OperatorID... operatorIds) {
        Random random = new Random();
        Collection<OperatorState> operatorStates = new ArrayList<>(operatorIds.length);

        for (OperatorID operatorId : operatorIds) {
            final OperatorState operatorState = new OperatorState(operatorId, 1, 42);
            final OperatorSubtaskState subtaskState =
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(
                                    new OperatorStreamStateHandle(
                                            Collections.emptyMap(),
                                            new ByteStreamStateHandle("foobar", new byte[0])))
                            .setInputChannelState(
                                    singleton(createNewInputChannelStateHandle(10, random)))
                            .setResultSubpartitionState(
                                    singleton(createNewResultSubpartitionStateHandle(10, random)))
                            .build();
            operatorState.putState(0, subtaskState);
            operatorStates.add(operatorState);
        }

        return operatorStates;
    }

    @Nonnull
    public static JobGraph createJobGraphFromJobVerticesWithCheckpointing(
            SavepointRestoreSettings savepointRestoreSettings, JobVertex... jobVertices) {

        // enable checkpointing which is required to resume from a savepoint
        final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                CheckpointCoordinatorConfiguration.builder()
                        .setCheckpointInterval(1000L)
                        .setCheckpointTimeout(1000L)
                        .setMinPauseBetweenCheckpoints(1000L)
                        .setMaxConcurrentCheckpoints(1)
                        .setCheckpointRetentionPolicy(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION)
                        .setExactlyOnce(true)
                        .setUnalignedCheckpointsEnabled(false)
                        .setTolerableCheckpointFailureNumber(0)
                        .build();
        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(checkpointCoordinatorConfiguration, null);

        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .addJobVertices(Arrays.asList(jobVertices))
                .setJobCheckpointingSettings(checkpointingSettings)
                .setSavepointRestoreSettings(savepointRestoreSettings)
                .build();
    }

    private TestUtils() {
        throw new UnsupportedOperationException("This class should not be instantiated.");
    }
}
