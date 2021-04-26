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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

/** Tests for the {@link CheckpointConfigInfo}. */
public class CheckpointConfigInfoTest
        extends RestResponseMarshallingTestBase<CheckpointConfigInfo> {
    @Override
    protected Class<CheckpointConfigInfo> getTestResponseClass() {
        return CheckpointConfigInfo.class;
    }

    @Override
    protected CheckpointConfigInfo getTestResponseInstance() {
        final CheckpointConfigInfo.ExternalizedCheckpointInfo externalizedCheckpointInfo =
                new CheckpointConfigInfo.ExternalizedCheckpointInfo(true, false);

        return new CheckpointConfigInfo(
                CheckpointConfigInfo.ProcessingMode.AT_LEAST_ONCE,
                1L,
                2L,
                3L,
                4,
                externalizedCheckpointInfo,
                "stateBackendName",
                "checkpointStorageName",
                true,
                3,
                4);
    }
}
