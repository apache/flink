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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateSampleOperatorSubtaskState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

/** Tests the properties of {@link FullyFinishedOperatorState}. */
public class FullyFinishedOperatorStateTest {

    @Test
    public void testFullyFinishedOperatorState() {
        OperatorState operatorState = new FullyFinishedOperatorState(new OperatorID(), 5, 256);
        assertTrue(operatorState.isFullyFinished());

        assertEquals(0, operatorState.getSubtaskStates().size());
        assertEquals(0, operatorState.getStates().size());
        assertEquals(0, operatorState.getNumberCollectedStates());

        try {
            operatorState.putState(0, OperatorSubtaskState.builder().build());
            fail("Should not be able to put new subtask states for a fully finished state");
        } catch (UnsupportedOperationException e) {
            // Expected
        }

        try {
            operatorState.setCoordinatorState(
                    new ByteStreamStateHandle("test", new byte[] {1, 2, 3, 4}));
            fail("Should not be able to set coordinator states for a fully finished state");
        } catch (UnsupportedOperationException e) {
            // Excepted
        }
    }

    @Test
    void testGetDiscardables() throws IOException {
        Tuple2<List<StateObject>, OperatorSubtaskState> opSubtaskStates1 =
                generateSampleOperatorSubtaskState();
        Tuple2<List<StateObject>, OperatorSubtaskState> opSubtaskStates2 =
                generateSampleOperatorSubtaskState();

        OperatorState operatorState = new OperatorState(new OperatorID(), 2, 256);
        operatorState.putState(0, opSubtaskStates1.f1);
        operatorState.putState(1, opSubtaskStates2.f1);
        ByteStreamStateHandle coordinatorState =
                new ByteStreamStateHandle("test", new byte[] {1, 2, 3, 4});
        operatorState.setCoordinatorState(coordinatorState);
        HashSet<StateObject> discardables = new HashSet<>();
        discardables.addAll(opSubtaskStates1.f0.subList(0, 4));
        discardables.add(((InputChannelStateHandle) opSubtaskStates1.f0.get(4)).getDelegate());
        discardables.add(
                ((ResultSubpartitionStateHandle) opSubtaskStates1.f0.get(5)).getDelegate());
        discardables.addAll(opSubtaskStates2.f0.subList(0, 4));
        discardables.add(((InputChannelStateHandle) opSubtaskStates2.f0.get(4)).getDelegate());
        discardables.add(
                ((ResultSubpartitionStateHandle) opSubtaskStates2.f0.get(5)).getDelegate());
        discardables.add(coordinatorState);
        assertThat(new HashSet<>(operatorState.getDiscardables())).isEqualTo(discardables);
    }
}
