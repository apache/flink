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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;

import java.io.IOException;

/**
 * Non-persistent {@link CheckpointStorageWorkerView} for tests. Uses {@link
 * MemCheckpointStreamFactory}.
 */
public class TestCheckpointStorageWorkerView implements CheckpointStorageWorkerView {

    private final int maxStateSize;
    private final MemCheckpointStreamFactory taskOwnedCheckpointStreamFactory;
    private final CheckpointedStateScope taskOwnedStateScope;

    public TestCheckpointStorageWorkerView(int maxStateSize) {
        this(maxStateSize, CheckpointedStateScope.EXCLUSIVE);
    }

    private TestCheckpointStorageWorkerView(
            int maxStateSize, CheckpointedStateScope taskOwnedStateScope) {
        this.maxStateSize = maxStateSize;
        this.taskOwnedCheckpointStreamFactory = new MemCheckpointStreamFactory(maxStateSize);
        this.taskOwnedStateScope = taskOwnedStateScope;
    }

    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) {
        return new MemCheckpointStreamFactory(maxStateSize);
    }

    @Override
    public CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream()
            throws IOException {
        return taskOwnedCheckpointStreamFactory.createCheckpointStateOutputStream(
                taskOwnedStateScope);
    }
}
