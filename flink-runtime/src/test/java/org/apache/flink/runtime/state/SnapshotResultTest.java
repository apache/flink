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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SnapshotResultTest {

    @Test
    void discardState() throws Exception {
        SnapshotResult<StateObject> result =
                SnapshotResult.withLocalState(mock(StateObject.class), mock(StateObject.class));
        result.discardState();
        verify(result.getJobManagerOwnedSnapshot()).discardState();
        verify(result.getTaskLocalSnapshot()).discardState();
    }

    @Test
    void getStateSize() {
        long size = 42L;

        SnapshotResult<StateObject> result =
                SnapshotResult.withLocalState(
                        new DummyStateObject(size), new DummyStateObject(size));
        assertThat(result.getStateSize()).isEqualTo(size);
    }

    static class DummyStateObject implements StateObject {

        private static final long serialVersionUID = 1L;

        private final long size;

        DummyStateObject(long size) {
            this.size = size;
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return size;
        }
    }
}
