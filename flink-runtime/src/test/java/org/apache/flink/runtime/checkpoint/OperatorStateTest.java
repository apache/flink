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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OperatorState}. */
class OperatorStateTest {

    /**
     * The coordinator state field changed its type from {@link ByteStreamStateHandle} to the {@code
     * StreamStateHandle} interface it implements, so that it can hold the handle of a file. This
     * tests that an {@link OperatorState} Java-serialized by an earlier version, such as one in a
     * high availability entry, can still be deserialized. The file
     * operator-state-with-coordinator-state-flink2.3 holds such a state, written with Flink 2.3.0
     * on the classpath by:
     *
     * <pre>{@code
     * OperatorState state =
     *         new OperatorState("source", "source-uid", new OperatorID(1L, 2L), 1, 128);
     * state.setCoordinatorState(
     *         new ByteStreamStateHandle("coordinator-state", new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
     * Files.write(file, InstantiationUtil.serializeObject(state));
     * }</pre>
     */
    @Test
    void testDeserializesOperatorStateSerializedByAnEarlierVersion() throws Exception {
        final OperatorState state;
        try (InputStream in =
                getClass()
                        .getClassLoader()
                        .getResourceAsStream("operator-state-with-coordinator-state-flink2.3")) {
            state = InstantiationUtil.deserializeObject(in, getClass().getClassLoader());
        }

        assertThat(state.getOperatorID()).isEqualTo(new OperatorID(1L, 2L));
        assertThat(state.getOperatorUid()).contains("source-uid");
        assertThat(state.getCoordinatorState()).isInstanceOf(ByteStreamStateHandle.class);
        assertThat(((ByteStreamStateHandle) state.getCoordinatorState()).getData())
                .containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
    }
}
