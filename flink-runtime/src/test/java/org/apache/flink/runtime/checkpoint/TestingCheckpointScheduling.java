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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code TestingCheckpointScheduling} is a basic testing implementation of {@link
 * CheckpointScheduling} that provides a flag indicating whether checkpoint scheduling is enabled.
 */
public class TestingCheckpointScheduling implements CheckpointScheduling {

    private final AtomicBoolean checkpointSchedulingEnabled;

    public TestingCheckpointScheduling(boolean initialState) {
        checkpointSchedulingEnabled = new AtomicBoolean(initialState);
    }

    @Override
    public void startCheckpointScheduler() {
        checkpointSchedulingEnabled.set(true);
    }

    @Override
    public void stopCheckpointScheduler() {
        checkpointSchedulingEnabled.set(false);
    }

    public boolean isEnabled() {
        return checkpointSchedulingEnabled.get();
    }
}
