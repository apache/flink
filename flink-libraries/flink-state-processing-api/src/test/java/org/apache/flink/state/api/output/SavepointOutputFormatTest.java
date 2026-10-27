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

package org.apache.flink.state.api.output;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FirstAttemptInitializationContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for writing output savepoint metadata. */
class SavepointOutputFormatTest {

    @TempDir private File temporaryFolder;

    @Test
    void testSavepointOutputFormatOnlyWorksWithParallelismOne() throws Exception {
        Path path = new Path(temporaryFolder.getAbsolutePath());
        SavepointOutputFormat format = createSavepointOutputFormat(path);

        assertThatThrownBy(() -> format.open(FirstAttemptInitializationContext.of(0, 2)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testSavepointOutputFormat() throws Exception {
        Path path = new Path(temporaryFolder.getAbsolutePath());
        SavepointOutputFormat format = createSavepointOutputFormat(path);

        CheckpointMetadata metadata = createSavepoint();

        format.open(FirstAttemptInitializationContext.of(0, 1));
        format.writeRecord(metadata);
        format.close();

        CheckpointMetadata metadataOnDisk = SavepointLoader.loadSavepointMetadata(path.getPath());

        assertThat(metadataOnDisk.getCheckpointId())
                .as("Incorrect checkpoint id")
                .isEqualTo(metadata.getCheckpointId());

        assertThat(metadataOnDisk.getOperatorStates())
                .as("Incorrect number of operator states in savepoint")
                .hasSameSizeAs(metadata.getOperatorStates());

        assertThat(metadataOnDisk.getOperatorStates().iterator().next())
                .as("Incorrect operator state in savepoint")
                .isEqualTo(metadata.getOperatorStates().iterator().next());
    }

    private CheckpointMetadata createSavepoint() {
        OperatorState operatorState =
                new OperatorState(null, null, OperatorIDGenerator.fromUid("uid"), 1, 128);

        operatorState.putState(0, OperatorSubtaskState.builder().build());
        return new CheckpointMetadata(
                0, Collections.singleton(operatorState), Collections.emptyList());
    }

    private SavepointOutputFormat createSavepointOutputFormat(Path path) throws Exception {
        RuntimeContext ctx = new MockStreamingRuntimeContext(1, 0);

        SavepointOutputFormat format = new SavepointOutputFormat(path);
        format.setRuntimeContext(ctx);

        return format;
    }
}
