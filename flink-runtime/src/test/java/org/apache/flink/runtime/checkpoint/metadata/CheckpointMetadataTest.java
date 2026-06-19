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

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Simple tests for the {@link CheckpointMetadata} data holder class. */
class CheckpointMetadataTest {

    @Test
    void testConstructAndDispose() throws Exception {
        final Random rnd = new Random();

        final long checkpointId = rnd.nextInt(Integer.MAX_VALUE) + 1;
        final int numTaskStates = 4;
        final int numSubtaskStates = 16;
        final int numMasterStates = 7;

        Collection<OperatorState> taskStates =
                CheckpointTestUtils.createOperatorStates(
                        rnd, null, numTaskStates, 0, 0, numSubtaskStates);

        Collection<MasterState> masterStates =
                CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

        CheckpointMetadata checkpoint =
                new CheckpointMetadata(checkpointId, taskStates, masterStates);

        assertThat(checkpoint.getCheckpointId()).isEqualTo(checkpointId);
        assertThat(checkpoint.getOperatorStates()).isEqualTo(taskStates);
        assertThat(checkpoint.getMasterStates()).isEqualTo(masterStates);

        assertThat(checkpoint.getOperatorStates()).isNotEmpty();
        assertThat(checkpoint.getMasterStates()).isNotEmpty();

        checkpoint.dispose();

        assertThat(checkpoint.getOperatorStates()).isEmpty();
        assertThat(checkpoint.getMasterStates()).isEmpty();
    }
}
