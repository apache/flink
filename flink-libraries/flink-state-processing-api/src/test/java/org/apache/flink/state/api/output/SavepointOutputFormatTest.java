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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;

/** Test for writing output savepoint metadata. */
public class SavepointOutputFormatTest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test(expected = IllegalStateException.class)
    public void testSavepointOutputFormatOnlyWorksWithParallelismOne() throws Exception {
        Path path = new Path(temporaryFolder.newFolder().getAbsolutePath());
        SavepointOutputFormat format = createSavepointOutputFormat(path);

        format.open(0, 2);
    }

    @Test
    public void testSavepointOutputFormat() throws Exception {
        Path path = new Path(temporaryFolder.newFolder().getAbsolutePath());
        SavepointOutputFormat format = createSavepointOutputFormat(path);

        CheckpointMetadata metadata = createSavepoint();

        format.open(0, 1);
        format.writeRecord(metadata);
        format.close();

        CheckpointMetadata metadataOnDisk = SavepointLoader.loadSavepointMetadata(path.getPath());

        Assert.assertEquals(
                "Incorrect checkpoint id",
                metadata.getCheckpointId(),
                metadataOnDisk.getCheckpointId());

        Assert.assertEquals(
                "Incorrect number of operator states in savepoint",
                metadata.getOperatorStates().size(),
                metadataOnDisk.getOperatorStates().size());

        Assert.assertEquals(
                "Incorrect operator state in savepoint",
                metadata.getOperatorStates().iterator().next(),
                metadataOnDisk.getOperatorStates().iterator().next());
    }

    private CheckpointMetadata createSavepoint() {
        OperatorState operatorState = new OperatorState(OperatorIDGenerator.fromUid("uid"), 1, 128);

        operatorState.putState(0, OperatorSubtaskState.builder().build());
        return new CheckpointMetadata(
                0, Collections.singleton(operatorState), Collections.emptyList());
    }

    private SavepointOutputFormat createSavepointOutputFormat(Path path) throws Exception {
        RuntimeContext ctx = new MockStreamingRuntimeContext(false, 1, 0);

        SavepointOutputFormat format = new SavepointOutputFormat(path);
        format.setRuntimeContext(ctx);

        return format;
    }
}
