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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;

import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.state.StateHandleID.randomStateHandleId;
import static org.assertj.core.api.Assertions.assertThat;

class ChangelogStateBackendHandleTest {

    @Test
    void testPublicConstructor() {
        long checkpointId = 2L;
        long materializationID = 1L;
        long size = 2L;
        validateHandle(
                checkpointId,
                materializationID,
                size,
                new ChangelogStateBackendHandleImpl(
                        emptyList(),
                        emptyList(),
                        KeyGroupRange.of(1, 2),
                        checkpointId,
                        materializationID,
                        size));
    }

    @Test
    void testRestore() {
        long checkpointId = 2L;
        long materializationID = 1L;
        long size = 2L;
        validateHandle(
                checkpointId,
                materializationID,
                size,
                ChangelogStateBackendHandleImpl.restore(
                        emptyList(),
                        emptyList(),
                        KeyGroupRange.of(1, 2),
                        checkpointId,
                        materializationID,
                        size,
                        randomStateHandleId()));
    }

    private void validateHandle(
            long checkpointId,
            long materializationID,
            long size,
            ChangelogStateBackendHandleImpl handle) {
        assertThat(handle.getCheckpointId()).isEqualTo(checkpointId);
        assertThat(handle.getMaterializationID()).isEqualTo(materializationID);
        assertThat(handle.getCheckpointedSize()).isEqualTo(size);
    }
}
