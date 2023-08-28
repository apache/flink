/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A test for {@link KeyGroupsStateHandle} */
class KeyGroupsStateHandleTest {

    @Test
    void testNonEmptyIntersection() {
        KeyGroupRangeOffsets offsets = new KeyGroupRangeOffsets(0, 7);
        byte[] dummy = new byte[10];
        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", dummy);
        KeyGroupsStateHandle handle = new KeyGroupsStateHandle(offsets, streamHandle);

        KeyGroupRange expectedRange = new KeyGroupRange(0, 3);
        KeyGroupsStateHandle newHandle = handle.getIntersection(expectedRange);
        assertThat(newHandle).isNotNull();
        assertThat(newHandle.getDelegateStateHandle()).isEqualTo(streamHandle);
        assertThat(newHandle.getKeyGroupRange()).isEqualTo(expectedRange);
        assertThat(newHandle.getStateHandleId()).isEqualTo(handle.getStateHandleId());
    }

    @Test
    void testEmptyIntersection() {
        KeyGroupRangeOffsets offsets = new KeyGroupRangeOffsets(0, 7);
        byte[] dummy = new byte[10];
        StreamStateHandle streamHandle = new ByteStreamStateHandle("test", dummy);
        KeyGroupsStateHandle handle = new KeyGroupsStateHandle(offsets, streamHandle);
        // return null if the keygroup intersection is empty.
        KeyGroupRange newRange = new KeyGroupRange(8, 11);
        assertThat(handle.getIntersection(newRange)).isNull();
    }
}
